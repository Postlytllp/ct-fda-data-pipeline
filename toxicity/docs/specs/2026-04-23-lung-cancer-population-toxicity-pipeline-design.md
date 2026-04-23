# Lung Cancer Population-Level Toxicity Pipeline — Design

**Date:** 2026-04-23
**Notebook:** `data_pipeline/toxicity/lung_cancer_ppl_toxicity.ipynb`
**Scope:** One-time batch pipeline (re-runnable) extracting per-arm adverse event data from ClinicalTrials.gov for FDA-approved lung cancer drugs, tagged with demographic inferences at configurable confidence levels.

---

## 1. Goal

Produce a denormalized dataset linking, for each arm of each completed-with-results lung-cancer trial: the drug regimen, the arm's demographic profile (race/ethnicity/country — inferred via a confidence cascade), and its adverse-event counts (serious + other). The dataset supports downstream analyses of toxicity in specific populations.

---

## 2. Non-goals

- Age and sex demographic analysis (race, ethnicity, country only).
- Statistical meta-analysis, hazard ratios, or cross-trial comparisons.
- EU Clinical Trials Registry integration (ClinicalTrials.gov only).
- FDA FAERS integration.
- Real-time or scheduled re-runs.
- Dose/schedule-level analysis of combo regimens.

---

## 3. Pipeline overview

Six phases implemented as cells in `lung_cancer_ppl_toxicity.ipynb`, notebook-first per the decision to extract to modules only after the logic stabilizes. Raw responses are cached; outputs are written as CSVs plus a SQLite mirror.

| Phase | Responsibility | Produces |
|---|---|---|
| 1 | Drug list: scrape NCI NSCLC + SCLC pages, harmonize via existing RxNorm tooling | `lung_cancer_drugs.csv` |
| 2 | Bulk fetch: build Essie-OR query across all drug aliases, paginate CT.gov v2, cache raw pages | `data/raw/page_*.json` |
| 3 | Parse cached pages into base dataframes | `trials`, `arms`, `arm_interventions`, `baseline_raw`, `ae_raw` |
| 4 | Demographic cascade (Tier A→D); Tier B uses regex + Gemma 4 26B MoE for ambiguous cases | `demographics` |
| 5 | Filters (has-results, AE>0 per arm, diversity pass-through by tier, drug-match) | Flag columns on base tables |
| 6 | Join + write final cohort view | `lung_cancer_toxicity_cohort.csv` + SQLite view |

---

## 4. Data sources

- **NCI lung cancer drug lists** — NSCLC and SCLC pages, human-curated. Scrape once per run; keep timestamped snapshot in `data/reference/nci_drug_list_YYYYMMDD.json`.
- **RxNorm / OpenFDA** — for brand/generic harmonization. Reuses existing `data_pipeline/harmonization/drug_harmonizer_with_purplebook.py` and `harmonize_drugs_from_file.py`. No changes to those modules.
- **ClinicalTrials.gov API v2** — `https://clinicaltrials.gov/api/v2/studies`. Single bulk query with Essie boolean OR across all drug aliases.
- **CIA World Factbook / World Bank ethnicity indicators** — seed for `data/reference/monoethnic_countries.csv` (country → dominant ancestry + homogeneity score).

---

## 5. Directory layout

```
data_pipeline/toxicity/
├── lung_cancer_ppl_toxicity.ipynb
├── docs/
│   └── specs/
│       └── 2026-04-23-lung-cancer-population-toxicity-pipeline-design.md
├── data/                          # .gitignore'd
│   ├── raw/                       # cached paginated CT.gov responses
│   │   └── page_001.json, page_002.json, ...
│   ├── llm_cache/
│   │   └── tier_b_results.jsonl   # keyed by nct_id
│   ├── reference/                 # committed
│   │   ├── nci_drug_list_YYYYMMDD.json
│   │   ├── monoethnic_countries.csv
│   │   └── drug_class_lookup.csv
│   └── outputs/                   # .gitignore'd
│       ├── lung_cancer_drugs.csv
│       ├── trials.csv
│       ├── arms.csv
│       ├── arm_interventions.csv
│       ├── demographics.csv
│       ├── ae_long.csv
│       ├── ae_arm_summary.csv
│       ├── lung_cancer_toxicity_cohort.csv
│       ├── run_summary_YYYYMMDD.json
│       └── lung_cancer_toxicity.db
```

Reference files (`monoethnic_countries.csv`, `drug_class_lookup.csv`) are committed. Raw API caches, LLM caches, and outputs are regenerable and excluded from version control.

---

## 6. Phase 1 — Drug list

1. Fetch NCI's "Drugs Approved for Non-Small Cell Lung Cancer" and "Drugs Approved for Small Cell Lung Cancer" pages. Parse drug generic and brand names.
2. Merge into one list with a `subtypes` column (`["NSCLC"]`, `["SCLC"]`, or `["NSCLC", "SCLC"]`).
3. Feed generic + brand names through existing `harmonize_drugs_from_file.py` → RxNorm RxCUIs + alias lists.
4. Output `lung_cancer_drugs.csv` with columns: `canonical_name`, `rxcui`, `aliases` (JSON list), `subtypes` (JSON list), `source` (always `nci` for this run), `snapshot_date`.

---

## 7. Phase 2 — CT.gov bulk fetch

### Query construction

Essie boolean OR over all aliases from Phase 1:

```
query.intr = "(pembrolizumab OR nivolumab OR osimertinib OR ... OR Keytruda OR Opdivo OR ...)"
aggFilters = "results:with"
filter.overallStatus = "COMPLETED,TERMINATED"
pageSize = 200
countTotal = true
format = "json"
```

`aggFilters=results:with` enforces the "has results" filter at the query level per confirmed live-probe behavior of the v2 API. `countTotal=true` returns `totalCount` on the first page to enable a progress bar. `TERMINATED` is included alongside `COMPLETED` because terminated-with-results trials are especially relevant for toxicity analysis (they are often terminated due to safety signals).

### Pagination and caching

- Iterate using `nextPageToken` → `pageToken`. Cache each page's JSON verbatim at `data/raw/page_001.json`, `page_002.json`, ...
- Cache is authoritative on re-runs: if `page_001.json` exists, don't re-fetch.
- Write after every successful page so the fetch is resumable if interrupted.
- `totalCount` is persisted in `page_001.json`; downstream cells verify expected page count matches.

### Essie OR length limits

URLs can get long with many aliases. If the OR expression exceeds safe URL length (~8 KB), split into batches of aliases and concatenate results before dedup on `nctId`.

---

## 8. Phase 3 — Parse to base tables

Iterate cached pages, emit the following dataframes. Every JSON→row mapping keeps a `raw_*` column for audit.

### `trials` (one row per NCT)
`nct_id`, `brief_title`, `official_title`, `detailed_description`, `phase`, `overall_status`, `study_type`, `start_date`, `completion_date`, `enrollment_actual`, `sponsor_name`, `sponsor_class`, `lead_sponsor_country`, `conditions` (JSON), `keywords` (JSON), `countries` (JSON), `secondary_ids` (JSON: `[{type, domain, id}]`), `lung_cancer_subtypes` (JSON: intersection with Phase 1 subtypes based on drug match), `eligibility_criteria_text`, `gender`, `minimum_age`, `maximum_age`, `healthy_volunteers`.

### `arms` (one row per arm)
`nct_id`, `arm_label` (normalized), `raw_arm_label` (verbatim from `armsInterventionsModule`), `arm_type`, `arm_description`, `n_at_risk` (from AE module), `regimen_key` (alphabetically sorted `|`-joined RxCUIs; unharmonized interventions use `unknown:<raw_name>`), `regimen_display` (alphabetically sorted `|`-joined generic names; unharmonized interventions use the raw intervention name), `match_method` (`exact_normalized`/`fuzzy`/`positional`/`unmatched`), `fuzzy_score` (0-100 or null), `arm_match_status` (`ok`/`count_mismatch`/`ambiguous`).

### `arm_interventions` (M:N)
`nct_id`, `arm_label`, `intervention_name`, `intervention_type`, `rxnorm_rxcui`, `matched_lung_cancer_drug` (bool), `is_primary_oncology` (bool — true when intervention's RxCUI is in Phase 1 drug list), `drug_class` (`targeted_oncology` / `immunotherapy` / `chemo_backbone` / `antiangiogenic` / `supportive_care` / `placebo` / `other`).

### `baseline_raw`
One row per `(nct_id, group_id, measure_title, category)` pulled verbatim from `baselineCharacteristicsModule`. Consumed by Phase 4.

### `ae_raw` (long-format AE source rows)
`nct_id`, `raw_group_id` (`EG000`…), `arm_label` (resolved), `ae_category` (`SERIOUS` / `OTHER`), `meddra_term`, `organ_system`, `affected_count`, `at_risk_count`, `source_vocab` (e.g., `MedDRA 26.1`).

### Arm-label resolution (across modules)

Titles in `armsInterventionsModule.armGroups[].label`, `baselineCharacteristicsModule.groups[].title`, `adverseEventsModule.eventGroups[].title`, and `participantFlowModule.groups[].title` are not guaranteed identical. Resolution order:

1. **Normalize**: casefold; strip prefixes (`^(experimental|active comparator|placebo|arm [a-z]|group \d+|cohort [a-z\d]+)\s*[:\-–]?\s*`); strip dose tokens (`\b\d+\s*(mg|mcg|g|mg/m2|mg/kg|auc\d+)\b`) and frequency tokens (`\bq[dwm]\d*|qod|bid|tid|daily|weekly\b`); collapse whitespace; unify dashes.
2. **Exact match** on normalized strings → `match_method=exact_normalized`.
3. **Fuzzy match** via `rapidfuzz.token_set_ratio` ≥ 90 for unresolved → `match_method=fuzzy`, `fuzzy_score=<score>`.
4. **Positional fallback** when group counts match across modules and no unique resolution → `match_method=positional` with warning log entry.
5. **Count mismatch** (baseline has N groups, AE has M, N ≠ M) → flag `arm_match_status=count_mismatch`; arm stays in `arms.csv` but is excluded from the filtered cohort view.

No LLM is used for arm resolution — the vocabulary is bounded and deterministic normalization + fuzzy matching is faster and reproducible.

---

## 9. Phase 4 — Demographic cascade

For each trial (or arm, when per-arm evidence exists), apply tiers in order; first tier with sufficient evidence wins. Lower tiers do not run once a higher tier has produced a result.

### Tier A — Reported data

- **A1** (per arm): Race / Race (NIH-OMB) baseline measure. If one category ≥ 95% of arm's total → `inferred_population=<that race>`, `inferred_diversity_pct=<pct>`, `confidence=high`.
- **A1-trial** (trial-level fallback): if arm-level fails 95%, pool all arms of the trial; if pooled single category ≥ 95% → same result with `fallback_trial_level=True`.
- **A2**: Region / Country of Enrollment measure. If ≥ 95% from one country → look up country in `monoethnic_countries.csv`. If country is in the diverse-country exclusion list (US, UK, Canada, etc.), A2 does not yield a population inference and the cascade falls through to Tier B. Otherwise map to dominant ancestry.

### Tier B — Explicit text

- **B1**: Regex over `briefTitle`, `officialTitle`, `detailedDescription` for `<race/nationality> + <patient|subject|population|participant|adult|volunteer|cohort>`, negation-aware (skip text preceded by "excluding", "without", "non-").
- **B2**: Regex + LLM over `eligibilityModule.eligibilityCriteria` inclusion block. Regex prescreen for common patterns. LLM triggers when: (a) regex yields no hit but the inclusion text length > 200 chars (worth a semantic pass), or (b) regex hits but the match falls inside a negated/stratification context (e.g., inside a bullet beginning with "excluded", "stratification", "sub-analysis"). Only applies when the criterion is a hard inclusion filter.

### Tier C — Location inference

- **C1**: Single non-US site in monoethnic whitelist → map to dominant ancestry.
- **C2**: Multi-country but all within one continent/region (Europe, East Asia, Sub-Saharan Africa, etc.) → continental population label.
- **C3**: Sponsor / lead-collaborator country in monoethnic whitelist when site list absent → `confidence=low`.

### Tier D — Registry signals

- **D1**: Secondary IDs matching regional registries: CTRI (India), JRCT (Japan), CFDA/NMPA (China), EudraCT (EU), ANZCTR (Australia/New Zealand).
- **D2**: `oversightModule` IRB/ethics committee country.

### `demographics` output columns

`nct_id`, `arm_label` (null if trial-level), `demog_tier` (`A1`/`A1-trial`/`A2`/`B1`/`B2`/`C1`/`C2`/`C3`/`D1`/`D2`/`NONE`), `demog_confidence` (`high`/`medium`/`low`), `inferred_population`, `inferred_diversity_pct` (numeric for A1 / A1-trial / A2 only), `demog_source_evidence` (raw text span or measure title), `fallback_trial_level` (bool), `biomarker_indirect_signal` (e.g., `"EGFR-selective → Asian-skewed"` or null).

### Biomarker indirect-signal flag

A supplementary column only — never used as filter criterion. Regex over eligibility criteria: `EGFR.*(mutation|positive)`, `ALK.*positive|rearrange`, `KRAS.*G12C` → sets `biomarker_indirect_signal`. Interpretation guide lives in `data/reference/drug_class_lookup.csv` notes.

### Monoethnic country reference table

`data/reference/monoethnic_countries.csv` seeded from CIA World Factbook ethnic-group percentages and World Bank indicators.

Columns: `country_iso3`, `country_name`, `dominant_ancestry`, `homogeneity_score` (0-1, computed as max single ethnic group share), `continent`, `region`, `in_diverse_exclusion_list` (true for countries with max share < 0.70: US, UK, Canada, Australia, Brazil, Singapore, South Africa, etc.).

Countries in the diverse-exclusion list are never used as Tier C1 / A2 / D evidence.

### Conflict resolution

Higher tier always wins. If Tier A1 produces 80% Japanese but the title (Tier B1) says "Japanese population", the Tier A1 result (80%) is used and the trial fails the ≥95% threshold. We do not fall back to B/C/D when Tier A has data — reported data is authoritative.

### Gemma 4 26B MoE integration

- Triggered only for B2 ambiguous cases after regex prescreen (estimated 10-30% of trials).
- Hardware: Google Colab Pro with L4 GPU runtime (24 GB VRAM; Q5_K_M GGUF via `llama.cpp` or 4-bit via `bitsandbytes`).
- Structured-output prompt with schema: `{has_population_restriction: bool, population: str|null, is_inclusion_criterion: bool, evidence_span: str, reasoning: str}`.
- Batch concurrency tuned for L4 (16-32 concurrent calls).
- Per-call results persisted to `data/llm_cache/tier_b_results.jsonl`, keyed by `nct_id`, so re-runs skip already-classified trials.
- LLM backend isolated behind a single `extract_population_from_eligibility(text) -> PopulationHit` function so it can be swapped for a hosted API later without touching pipeline code.

---

## 10. Phase 5 — Filters

Filters set boolean flag columns; the *filtered cohort view* is produced by AND-ing those flags in Phase 6. Nothing is dropped from the base tables silently.

1. **Has-results** — enforced at query time via `aggFilters=results:with`. No runtime filter.
2. **AE count > 0 per arm** → `has_any_ae` bool on `arms` (true when `total_serious_affected + total_other_affected > 0` from `ae_arm_summary`).
3. **Diversity pass-through by tier** → `passes_diversity` bool on `demographics`:
   - Tier A1 / A1-trial / A2: pass if `inferred_diversity_pct ≥ 0.95`.
   - Tier B1 / B2 / C1 / C2 / C3 / D1 / D2: pass when `inferred_population` is non-null (categorical classification).
   - Tier NONE: fail.
4. **Drug-match filter** → `has_lung_cancer_drug_match` bool on `arms` (true when at least one intervention has `is_primary_oncology=True`). Drops placebo-only / non-oncology-control arms from the cohort view.

---

## 11. Phase 6 — Join + outputs

### `ae_arm_summary` (computed from `ae_raw`)

`nct_id`, `arm_label`, `total_serious_affected`, `total_other_affected`, `total_at_risk`, `distinct_serious_terms`, `distinct_other_terms`, `serious_ae_rate` (= `total_serious_affected / total_at_risk`), `any_ae_rate`.

### `ae_long` (computed from `ae_raw`)

Same as `ae_raw` but with arm labels resolved and filtered to rows linked to a known arm.

### `lung_cancer_toxicity_cohort.csv` (denormalized, filtered)

One row per `(arm × AE term × AE category)` where `has_any_ae AND passes_diversity AND has_lung_cancer_drug_match`. Columns join: `trials` + `arms` + `demographics` + `arm_interventions` (aggregated per arm) + `ae_long`.

Key columns for analysis: `nct_id`, `phase`, `arm_label`, `regimen_display`, `primary_oncology_drugs` (pipe-joined subset in NCI list), `backbone_drugs` (pipe-joined subset classified as chemo backbone), `inferred_population`, `demog_tier`, `demog_confidence`, `ae_category`, `meddra_term`, `organ_system`, `affected_count`, `at_risk_count`, `sponsor_name`, `lead_sponsor_country`.

### SQLite mirror

`lung_cancer_toxicity.db` contains one table per CSV plus a `lung_cancer_toxicity_cohort` view defined as the filtered join. Enables SQL exploration across trial × arm × AE × demographics without re-reading CSVs.

### Run summary

`data/outputs/run_summary_YYYYMMDD.json` with: `drugs_queried`, `trials_returned`, `trials_with_baseline`, `trials_with_ae`, `arms_total`, `arms_passing_each_filter`, `demog_tier_distribution`, `llm_calls_made`, `llm_cache_hits`, `arm_match_status_distribution`, `total_runtime_seconds`.

---

## 12. Edge cases

- **Arm label mismatch between modules** — resolved per Section 8 (normalize → fuzzy → positional → flag).
- **Multiple drugs per arm** — M:N table; AEs attributed to the regimen, not individual drugs (CT.gov reports at arm level). `regimen_key` + `drug_class` enable combo-level grouping per Section 8.
- **Missing baseline module** — Tier A skipped; cascade falls through to B/C/D.
- **Arm with no interventions listed** — kept in `arms.csv`, `arm_interventions` has no rows, `has_lung_cancer_drug_match=False`, excluded from cohort view.
- **Pediatric / healthy-volunteer trials** — flagged via `gender`, `minimum_age`, `healthy_volunteers` columns; not filtered automatically.
- **Non-English characters in site names** — UTF-8 throughout; no transliteration.
- **MedDRA vocabulary version drift across trials** — captured verbatim in `source_vocab` column; no re-coding.
- **URL too long for Essie OR** — alias list split into batches with result dedup on `nctId`.
- **Pagination interruption** — each page written to cache before fetching next; resumable.
- **Tier A data exists but fails 95%** — A1 result recorded with actual pct; does not fall back to B/C/D.

---

## 13. Dependencies

Runtime:
- Python 3.10+
- `requests`, `pandas`, `tqdm`, `sqlalchemy`, `beautifulsoup4`, `pyyaml`, `rapidfuzz`
- `transformers`, `accelerate`, `bitsandbytes` OR `llama-cpp-python` for Gemma 4 26B MoE (Phase 4 B2 only)
- Colab Pro with L4 GPU runtime for Phase 4 LLM step; all other phases are CPU-fine

Reused project modules (no changes):
- `data_pipeline/harmonization/drug_harmonizer_with_purplebook.py`
- `data_pipeline/harmonization/harmonize_drugs_from_file.py`

Existing module possibly extended (Essie OR helper):
- `data_pipeline/data_sources/clinical_trials_gov.py` — may add a `search_by_intervention_or()` method; alternative is a thin toxicity-local fetcher.

---

## 14. Execution model

Cells execute top-to-bottom with each phase idempotent against its cache:

- Re-running Phase 1 refreshes the NCI list only if the prior snapshot file is missing or older than the configured TTL (default: always regenerate on manual re-run, automatic skip only on full pipeline restart).
- Re-running Phase 2 is a no-op if `data/raw/` has a complete page set matching `totalCount`.
- Re-running Phases 3-6 is fast (< 1 min) because parsing runs off cached JSON.
- Re-running Phase 4's LLM step skips trials already in `tier_b_results.jsonl`.

First full run (cold caches): estimated 10 minutes fetch + 30-60 minutes Tier B LLM pass on L4 + a few minutes parsing/joining. Subsequent runs: < 2 minutes.

---

## 15. Validation plan

- **Manual spot-check**: pick 10 random trials across different demographic tiers; verify extracted fields against the CT.gov website.
- **Sanity checks in `run_summary`**: expected trials_returned matches `totalCount`; `arm_match_status_distribution` has `count_mismatch` as a small minority; `demog_tier_distribution` has reasonable spread.
- **Cohort sanity**: `lung_cancer_toxicity_cohort.csv` row count is non-zero; at least one trial lands in each of A1, B, C tiers; no row has all three filter flags false.

---

## 16. Future work (out of scope for this spec)

- Module extraction: promote stabilized notebook logic to `data_pipeline/toxicity/*.py` modules (`drug_list.py`, `ct_fetcher.py`, `parsers.py`, `demog_cascade.py`, `filters.py`, `storage.py`).
- Same pipeline applied to other cancers / indications by parameterizing the NCI seed.
- Incorporate EU Clinical Trials Register to expand geographic coverage.
- Age/sex demographic layer on the same architecture.
- FAERS post-marketing AE cross-reference.
