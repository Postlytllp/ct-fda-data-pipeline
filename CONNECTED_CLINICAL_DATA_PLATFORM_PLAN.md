# Connected Clinical Data Platform - Comprehensive Implementation Plan

## Executive Summary

This document outlines the architecture, data sources, and implementation strategy for building a **Connected Clinical Data Platform** that integrates clinical trials, FDA drug approvals, NCBI databases, and **PubMed publications** into a single harmonized database.

---

## Table of Contents

1. [Data Sources Overview](#1-data-sources-overview)
2. [Detailed API Specifications](#2-detailed-api-specifications)
3. [Harmonized Schema Design](#3-harmonized-schema-design)
4. [Entity Resolution Strategy](#4-entity-resolution-strategy)
5. [Pipeline Architecture](#5-pipeline-architecture)
6. [Update Automation Strategy](#6-update-automation-strategy)
7. [Tech Stack Recommendations](#7-tech-stack-recommendations)
8. [Implementation Roadmap](#8-implementation-roadmap)
9. [Challenges and Best Practices](#9-challenges-and-best-practices)

---

## 1. Data Sources Overview

### 1.1 Clinical Trial Registries

| Source | Description | Update Frequency | License |
|--------|-------------|------------------|---------|
| **ClinicalTrials.gov** | US clinical trials (~500K studies) | Daily | Public Domain |
| **EU Clinical Trials Register** | European trials | Weekly | Public Access |
| **WHO ICTRP** | International registry platform | Weekly | Public Access |

### 1.2 FDA Data Sources

| Source | Description | Update Frequency | License |
|--------|-------------|------------------|---------|
| **openFDA APIs** | Drug labels, adverse events, NDC | Daily/Quarterly | Public Domain |
| **Orange Book** | Approved drugs with therapeutic equivalence | Monthly | Public Domain |
| **Drugs@FDA** | Drug approval packages | Weekly | Public Domain |
| **NDC Directory** | Drug product listings | Daily | Public Domain |
| **FDA SPL** | Structured Product Labeling | Daily | Public Domain |

### 1.3 NCBI/NLM Data Sources

| Source | Description | Update Frequency | License |
|--------|-------------|------------------|---------|
| **RxNorm** | Normalized drug naming | Weekly | UMLS License (free) |
| **MeSH** | Medical Subject Headings | Annual + Weekly | Public Domain |
| **PubChem** | Chemical compound database | Daily | Public Domain |
| **UMLS** | Unified medical vocabulary | Quarterly | UMLS License (free) |
| **UNII** | Unique Ingredient Identifiers | Weekly | Public Domain |
| **PubMed** | Biomedical literature (35M+ articles) | Daily | Public Domain |
| **SNOMED CT** | Clinical terminology (350K+ concepts) | Bi-annual + Monthly | SNOMED License (free for members) |

---

## 2. Detailed API Specifications

### 2.1 ClinicalTrials.gov API v2

**Base URL:** `https://clinicaltrials.gov/api/v2/studies`

```http
GET /studies?query.cond=diabetes&pageSize=100&format=json
GET /studies/{nctId}
```

**Key Fields:**
- `protocolSection.identificationModule.nctId` → Trial ID
- `protocolSection.sponsorCollaboratorsModule.leadSponsor.name` → Sponsor
- `protocolSection.armsInterventionsModule.interventions[].name` → Drug names
- `protocolSection.conditionsModule.conditions[]` → Conditions

**Rate Limit:** ~3 req/sec | **Bulk:** https://clinicaltrials.gov/AllPublicXML.zip

### 2.2 openFDA APIs

**Base URL:** `https://api.fda.gov`

| Endpoint | Description |
|----------|-------------|
| `/drug/label.json` | Drug labels/SPL |
| `/drug/ndc.json` | NDC directory |
| `/drug/event.json` | Adverse events (FAERS) |

**Key Fields (Label):**
- `openfda.brand_name`, `openfda.generic_name`
- `openfda.rxcui`, `openfda.unii`
- `openfda.application_number`

**Rate Limit:** 40 req/min (no key), 240 req/min (with key)

### 2.3 Orange Book Data

**URL:** https://www.fda.gov/drugs/drug-approvals-and-databases/orange-book-data-files

| File | Description |
|------|-------------|
| `products.txt` | Approved drug products |
| `patent.txt` | Patent information |
| `exclusivity.txt` | Marketing exclusivity |

### 2.4 RxNorm API

**Base URL:** `https://rxnav.nlm.nih.gov/REST`

```http
GET /drugs.json?name={drugName}
GET /rxcui/{rxcui}/allrelated.json
GET /approximateTerm.json?term={term}
```

**Concept Types:** IN (Ingredient), BN (Brand Name), SCD (Clinical Drug), SBD (Branded Drug)

### 2.5 PubChem PUG REST

**Base URL:** `https://pubchem.ncbi.nlm.nih.gov/rest/pug`

```http
GET /compound/name/{name}/cids/JSON
GET /compound/cid/{cid}/property/MolecularFormula,MolecularWeight/JSON
GET /compound/cid/{cid}/synonyms/JSON
```

### 2.6 MeSH API

**Base URL:** `https://id.nlm.nih.gov/mesh`

```http
GET /lookup/descriptor?label={term}&match=contains
```

### 2.7 SNOMED CT Snowstorm API

**Base URL:** `https://snowstorm.ihtsdotools.org/snowstorm/snomed-ct`

| Endpoint | Description |
|----------|-------------|
| `/MAIN/concepts` | Search concepts by term |
| `/MAIN/concepts/{conceptId}` | Get concept details |
| `/MAIN/concepts/{conceptId}/descendants` | Get child concepts |
| `/MAIN/concepts/{conceptId}/parents` | Get parent concepts |
| `/browser/MAIN/descriptions` | Search descriptions |

**Key Search Queries:**

```http
# Search for clinical concepts
GET /MAIN/concepts?term=diabetes&activeFilter=true&limit=50

# Get concept by SCTID
GET /MAIN/concepts/73211009  # Diabetes mellitus

# Get concept hierarchy (descendants)
GET /MAIN/concepts/73211009/descendants?stated=false&limit=100

# Search with semantic tag filter (disorders only)
GET /MAIN/concepts?term=pneumonia&semanticTag=disorder&activeFilter=true

# ECL (Expression Constraint Language) queries
GET /MAIN/concepts?ecl=<64572001  # All descendants of "Disease"
GET /MAIN/concepts?ecl=<<73211009  # Diabetes and all subtypes
```

**Key Concept Fields:**
- `conceptId` → SNOMED CT Identifier (SCTID)
- `fsn.term` → Fully Specified Name
- `pt.term` → Preferred Term
- `definitionStatus` → PRIMITIVE or FULLY_DEFINED
- `moduleId` → Source module (International, US, UK, etc.)

**Semantic Tags (Hierarchies):**
- `disorder` - Clinical conditions/diseases
- `finding` - Clinical findings
- `procedure` - Medical procedures
- `substance` - Drugs and chemicals
- `body structure` - Anatomical structures
- `organism` - Pathogens, organisms

**SNOMED CT Cross-Mappings Available:**
- SNOMED CT ↔ ICD-10 (official WHO map)
- SNOMED CT ↔ ICD-O-3 (oncology)
- SNOMED CT ↔ OPCS-4 (procedures)

**Rate Limit:** Public browser API - reasonable use | **Local:** Deploy Snowstorm server

**Bulk Access:** RF2 release files from SNOMED International or NLM (UMLS)

### 2.8 PubMed E-utilities API

**Base URL:** `https://eutils.ncbi.nlm.nih.gov/entrez/eutils`

| Endpoint | Description |
|----------|-------------|
| `/esearch.fcgi` | Search PubMed, returns PMIDs |
| `/efetch.fcgi` | Fetch full article records |
| `/elink.fcgi` | Find linked records (trial↔article) |
| `/esummary.fcgi` | Get article summaries |

**Key Search Strategies for Clinical Trial Links:**

```http
# Search by NCT ID in Secondary Source ID field
GET /esearch.fcgi?db=pubmed&term=NCT01234567[SI]&retmode=json

# Search by NCT ID in all fields
GET /esearch.fcgi?db=pubmed&term=NCT01234567&retmode=json

# Fetch article details
GET /efetch.fcgi?db=pubmed&id=12345678&rettype=xml&retmode=xml

# Find ClinicalTrials.gov links for a PMID
GET /elink.fcgi?dbfrom=pubmed&cmd=llinkslib&id=16210666&holding=CTgov

# Find related articles
GET /elink.fcgi?dbfrom=pubmed&db=pubmed&id=12345678&cmd=neighbor
```

**Key Article Fields:**
- `PMID` → PubMed identifier
- `ArticleTitle` → Publication title
- `Abstract/AbstractText` → Article abstract
- `AuthorList/Author` → Authors with affiliations
- `Journal/Title`, `ISOAbbreviation` → Journal info
- `PubDate` → Publication date
- `MeshHeadingList` → MeSH terms
- `DataBankList/AccessionNumberList` → NCT IDs (Secondary Source)
- `PublicationTypeList` → Article type (Clinical Trial, RCT, etc.)

**Rate Limit:** 3 req/sec (no key), 10 req/sec (with API key)

**Bulk Access:** PubMed Baseline files via FTP: `ftp.ncbi.nlm.nih.gov/pubmed/baseline/`

---

## 3. Harmonized Schema Design

### 3.1 Core Entity Model

```
CLINICAL_TRIAL ─── DRUG (Harmonized) ─── COMPANY (Normalized)
      │                   │                      │
      ├───────────────────┼──────────────────────┤
      ▼                   ▼                      ▼
DISEASE/CONDITION   FDA_APPROVAL         REGULATORY_STATUS
      │
      ▼
PUBMED_ARTICLES ←── Trial Results Publications
```

### 3.2 Harmonized Drug Table

```sql
CREATE TABLE harmonized_drugs (
    drug_id UUID PRIMARY KEY,
    harmonized_name VARCHAR(500) NOT NULL,
    harmonized_generic_name VARCHAR(500),
    harmonized_brand_name VARCHAR(500),
    
    -- Cross-Reference IDs
    rxcui VARCHAR(20) UNIQUE,
    unii VARCHAR(20) UNIQUE,
    pubchem_cid VARCHAR(20),
    cas_number VARCHAR(50),
    
    -- Collections
    brand_names JSONB,
    generic_names JSONB,
    synonyms JSONB,
    atc_codes JSONB,
    
    -- Properties
    molecular_formula VARCHAR(200),
    routes JSONB,
    dosage_forms JSONB,
    
    -- Metadata
    data_sources JSONB,
    confidence_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### 3.3 Harmonized Condition Table

```sql
CREATE TABLE harmonized_conditions (
    condition_id UUID PRIMARY KEY,
    harmonized_name VARCHAR(500) NOT NULL,
    preferred_term VARCHAR(500),
    
    -- Cross-Reference IDs
    mesh_id VARCHAR(20),
    mesh_tree_numbers JSONB,
    meddra_pt_code VARCHAR(20),
    icd10_codes JSONB,
    
    -- SNOMED CT Integration
    snomed_ct_id VARCHAR(20),           -- Primary SCTID
    snomed_fsn VARCHAR(500),             -- Fully Specified Name
    snomed_semantic_tag VARCHAR(50),     -- disorder, finding, etc.
    snomed_hierarchy_path JSONB,         -- Parent concept chain
    snomed_related_concepts JSONB,       -- Related SCTIDs
    
    synonyms JSONB,
    disease_category VARCHAR(200),
    parent_condition_id UUID REFERENCES harmonized_conditions(condition_id),
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_condition_snomed ON harmonized_conditions(snomed_ct_id);
```

### 3.4 Harmonized Company Table

```sql
CREATE TABLE harmonized_companies (
    company_id UUID PRIMARY KEY,
    harmonized_name VARCHAR(500) NOT NULL,
    legal_name VARCHAR(500),
    
    duns_number VARCHAR(20),
    fda_applicant_id VARCHAR(50),
    
    name_variations JSONB,
    company_type VARCHAR(50),
    headquarters_country VARCHAR(100),
    parent_company_id UUID REFERENCES harmonized_companies(company_id),
    
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 3.5 Clinical Trial Table (SCD Type 2)

```sql
CREATE TABLE clinical_trials (
    trial_id VARCHAR(50) PRIMARY KEY,
    registry_source VARCHAR(50) NOT NULL,
    title TEXT,
    overall_status VARCHAR(50),
    phase VARCHAR(50),
    study_type VARCHAR(50),
    
    start_date DATE,
    completion_date DATE,
    enrollment_count INTEGER,
    
    sponsor_id UUID REFERENCES harmonized_companies(company_id),
    countries JSONB,
    
    -- SCD Type 2
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    version INTEGER DEFAULT 1,
    record_hash VARCHAR(64),
    
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 3.6 Junction Tables

```sql
-- Trial ↔ Drug
CREATE TABLE trial_drugs (
    trial_id VARCHAR(50) REFERENCES clinical_trials(trial_id),
    drug_id UUID REFERENCES harmonized_drugs(drug_id),
    intervention_type VARCHAR(50),
    is_primary_intervention BOOLEAN,
    PRIMARY KEY (trial_id, drug_id)
);

-- Trial ↔ Condition
CREATE TABLE trial_conditions (
    trial_id VARCHAR(50) REFERENCES clinical_trials(trial_id),
    condition_id UUID REFERENCES harmonized_conditions(condition_id),
    is_primary_condition BOOLEAN,
    PRIMARY KEY (trial_id, condition_id)
);

-- Drug ↔ Condition (Indications)
CREATE TABLE drug_indications (
    drug_id UUID REFERENCES harmonized_drugs(drug_id),
    condition_id UUID REFERENCES harmonized_conditions(condition_id),
    indication_type VARCHAR(50),
    approval_date DATE,
    PRIMARY KEY (drug_id, condition_id, indication_type)
);
```

### 3.7 FDA Approvals Table

```sql
CREATE TABLE fda_approvals (
    approval_id UUID PRIMARY KEY,
    application_number VARCHAR(20) NOT NULL,
    application_type VARCHAR(10),
    drug_id UUID REFERENCES harmonized_drugs(drug_id),
    applicant_id UUID REFERENCES harmonized_companies(company_id),
    
    approval_date DATE,
    marketing_status VARCHAR(50),
    therapeutic_equivalence_code VARCHAR(10),
    
    dosage_form VARCHAR(100),
    route VARCHAR(100),
    strength VARCHAR(200),
    
    patents JSONB,
    exclusivities JSONB,
    source VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 3.8 PubMed Articles Table

```sql
CREATE TABLE pubmed_articles (
    pmid VARCHAR(20) PRIMARY KEY,
    title TEXT NOT NULL,
    abstract TEXT,
    
    -- Journal Information
    journal_name VARCHAR(500),
    journal_abbrev VARCHAR(100),
    publication_date DATE,
    pub_year INTEGER,
    volume VARCHAR(50),
    issue VARCHAR(50),
    pages VARCHAR(100),
    
    -- Identifiers
    doi VARCHAR(100),
    pmc_id VARCHAR(20),
    
    -- Authors
    authors JSONB,  -- [{"last_name": "", "first_name": "", "affiliation": ""}]
    first_author VARCHAR(200),
    
    -- Classification
    publication_types JSONB,  -- ["Clinical Trial", "Randomized Controlled Trial"]
    mesh_terms JSONB,         -- [{"descriptor": "", "qualifier": "", "major_topic": true}]
    keywords JSONB,
    
    -- Linked Data
    referenced_nct_ids JSONB,  -- NCT IDs found in article (DataBankList)
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_pubmed_pub_date ON pubmed_articles(publication_date);
CREATE INDEX idx_pubmed_nct_ids ON pubmed_articles USING GIN(referenced_nct_ids);
CREATE INDEX idx_pubmed_mesh ON pubmed_articles USING GIN(mesh_terms);
```

### 3.9 Trial ↔ Publication Junction Table

```sql
CREATE TABLE trial_publications (
    trial_id VARCHAR(50) REFERENCES clinical_trials(trial_id),
    pmid VARCHAR(20) REFERENCES pubmed_articles(pmid),
    
    -- Link Classification
    link_type VARCHAR(50) NOT NULL,  -- 'results', 'protocol', 'secondary_analysis', 'related'
    link_source VARCHAR(50),          -- 'nct_reference', 'pubmed_si', 'text_mining', 'manual'
    
    -- Confidence & Validation
    confidence_score DECIMAL(5,2),
    is_primary_publication BOOLEAN DEFAULT FALSE,
    is_validated BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    discovered_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (trial_id, pmid)
);

CREATE INDEX idx_trial_pub_link_type ON trial_publications(link_type);
```

---

## 4. Entity Resolution Strategy

### 4.1 Drug Name Harmonization Pipeline

```
INPUT: Raw Drug Name (e.g., "ESBRIET 267MG CAPSULES")
    │
    ▼
STEP 1: Preprocessing
    - Normalize case, remove special chars
    - Extract strength/dosage form
    │
    ▼
STEP 2: RxNorm Lookup (Primary)
    - Exact match → Approximate match
    - Get RXCUI, related concepts
    │
    ▼
STEP 3: openFDA Cross-Reference
    - Get NDC, UNII, manufacturer
    - Validate RxNorm match
    │
    ▼
STEP 4: PubChem Enrichment
    - Get CID, molecular properties
    │
    ▼
STEP 5: Consolidate & Score
    - Merge sources, calculate confidence
    │
    ▼
OUTPUT: Harmonized Drug Record
    harmonized_name: "pirfenidone"
    rxcui: "745467", unii: "D7NLD2R896"
    confidence: 95.0
```

### 4.2 Confidence Scoring

| Component | Points |
|-----------|--------|
| RxNorm exact match | 40 |
| RxNorm approximate match | 25 |
| FDA match | 20 |
| PubChem match | 15 |
| Cross-validation (multiple sources agree) | 20 |

### 4.3 Condition Harmonization (with SNOMED CT)

```
INPUT: "Idiopathic Pulmonary Fibrosis", "IPF"
    │
    ▼
STEP 1: SNOMED CT Lookup (Primary - Most Comprehensive)
    - Query Snowstorm: term=idiopathic pulmonary fibrosis&semanticTag=disorder
    - Get SCTID: 28168000 (Idiopathic fibrosing alveolitis)
    - Extract hierarchy path, related concepts
    │
    ▼
STEP 2: MeSH Cross-Reference
    - Use UMLS mappings or direct lookup
    - mesh_id: D054990
    │
    ▼
STEP 3: MedDRA Mapping
    - Via UMLS or SNOMED→MedDRA maps
    - meddra_pt: 10021240
    │
    ▼
STEP 4: ICD-10 Derivation
    - Use SNOMED CT → ICD-10 official map
    - icd10: J84.112
    │
    ▼
OUTPUT: Harmonized Condition
    snomed_ct_id: "28168000"
    snomed_fsn: "Idiopathic fibrosing alveolitis (disorder)"
    mesh_id: "D054990"
    meddra_pt: "10021240"
    icd10_codes: ["J84.112"]
```

**SNOMED CT Advantages for Condition Harmonization:**

| Feature | Benefit |
|---------|---------|
| **Hierarchical structure** | Navigate from specific → general conditions |
| **350K+ concepts** | Most comprehensive clinical terminology |
| **Official ICD-10 maps** | Reliable coding derivation |
| **Semantic tags** | Filter by disorder, finding, procedure |
| **ECL queries** | Find all subtypes of a condition |

**SNOMED CT Semantic Hierarchy Example:**
```
Disease (64572001)
  └── Disorder of respiratory system (50043002)
        └── Interstitial lung disease (51615001)
              └── Idiopathic fibrosing alveolitis (28168000) ← Target
```

### 4.4 Company Normalization

- Remove common suffixes (Inc, LLC, Corp, Pharmaceuticals)
- Fuzzy match against known aliases (threshold: 0.85)
- Maintain alias mapping table
- Track parent/subsidiary relationships

### 4.5 Trial ↔ Publication Linking Strategy

```
INPUT: Clinical Trial NCT ID (e.g., "NCT01234567")
    │
    ▼
STRATEGY 1: Direct NCT Reference (High Confidence: 95+)
    - Query PubMed: NCT01234567[SI] (Secondary Source ID field)
    - Articles that explicitly cite the NCT number in DataBankList
    │
    ▼
STRATEGY 2: ClinicalTrials.gov References (High Confidence: 90+)
    - Parse trial's "references" field for PMIDs
    - These are curator-added by trial registrants
    │
    ▼
STRATEGY 3: Full-Text Search (Medium Confidence: 70-85)
    - Query PubMed: NCT01234567 (all fields)
    - Validates by checking title/abstract mention
    │
    ▼
STRATEGY 4: Author + Condition Matching (Lower Confidence: 50-70)
    - Match principal investigator name + condition + date range
    - Fuzzy matching with validation rules
    │
    ▼
OUTPUT: Linked Publications with confidence scores
```

**Link Type Classification:**

| Link Type | Description | Detection Method |
|-----------|-------------|------------------|
| `results` | Primary results publication | NCT in DataBankList + "Clinical Trial" pub type |
| `protocol` | Study protocol paper | Title contains "protocol" or "design" |
| `secondary_analysis` | Secondary/subgroup analyses | References primary pub, same NCT |
| `related` | Related research (reviews, meta-analyses) | NCT mention without being the source trial |

**PubMed Publication Types for Clinical Trials:**
- `Clinical Trial` - General clinical trial
- `Randomized Controlled Trial` - RCT
- `Clinical Trial, Phase I/II/III/IV` - Phase-specific
- `Controlled Clinical Trial` - Non-randomized controlled
- `Pragmatic Clinical Trial` - Real-world evidence

---

## 5. Pipeline Architecture

### 5.1 High-Level Flow

```
DATA SOURCES → EXTRACTION → STAGING → TRANSFORMATION → HARMONIZED → SERVING
                  │             │            │              │           │
              API/Files    Raw Tables   Cleaning     SCD Type 2    REST API
                                        + Entity      + History     Exports
                                        Resolution
```

### 5.2 Airflow DAG Structure

```python
# Daily Pipeline
extract_clinical_trials >> extract_fda_data >> update_reference_data
    >> harmonize_drugs >> harmonize_conditions >> harmonize_companies
    >> link_entities >> link_trial_publications >> load_scd2 
    >> quality_checks >> reports

# Weekly PubMed Linking DAG
get_updated_trials >> search_pubmed_by_nct >> fetch_article_details
    >> classify_link_types >> update_trial_publications >> validate_links
```

---

## 6. Update Automation Strategy

### 6.1 Update Frequency Matrix

| Source | Type | Frequency | Method |
|--------|------|-----------|--------|
| ClinicalTrials.gov | Incremental | Daily | API delta |
| ClinicalTrials.gov | Full | Monthly | Bulk XML |
| openFDA Labels | Incremental | Daily | API filter |
| Orange Book | Full | Monthly | File download |
| RxNorm | Full | Weekly | UMLS download |
| MeSH | Full | Weekly | NLM download |
| PubMed (trial links) | Incremental | Weekly | E-utilities API |
| PubMed (new articles) | Incremental | Daily | E-utilities API |
| SNOMED CT | Full | Bi-annual | RF2 release files |
| SNOMED CT (maps) | Full | Monthly | SNOMED Int. / UMLS |

### 6.2 Cron Schedule

```yaml
daily_incremental:   "0 0 * * *"    # Midnight UTC
weekly_reference:    "0 2 * * 0"    # Sunday 2 AM
weekly_pubmed_links: "0 3 * * 0"    # Sunday 3 AM (after reference data)
monthly_full:        "0 4 1 * *"    # 1st of month 4 AM
monthly_snomed_maps: "0 5 1 * *"    # 1st of month 5 AM (SNOMED→ICD maps)
biannual_snomed:     "0 6 1 1,7 *"  # Jan & Jul 1st (major releases)
```

---

## 7. Tech Stack Recommendations

### 7.1 Core Stack

| Layer | Technology |
|-------|------------|
| **Database** | PostgreSQL 15+ (JSONB, pg_trgm) |
| **Cache** | Redis |
| **Orchestration** | Apache Airflow 2.x |
| **Language** | Python 3.11+ |
| **API** | FastAPI |
| **Monitoring** | Prometheus + Grafana |

### 7.2 Key Python Libraries

```
httpx, aiohttp          # Async HTTP
pandas, pyarrow         # Data processing
pydantic                # Validation
sqlalchemy, asyncpg     # Database
rapidfuzz               # Fuzzy matching
great-expectations      # Data quality
```

---

## 8. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-3)
- PostgreSQL schema setup
- ClinicalTrials.gov & openFDA extractors
- Basic Airflow DAG

### Phase 2: Entity Resolution (Weeks 4-6)
- RxNorm client & drug harmonizer
- **SNOMED CT Snowstorm client**
- **SNOMED CT → ICD-10/MeSH mapping integration**
- MeSH/MedDRA condition resolver
- Company normalizer + caching

### Phase 3: Integration (Weeks 7-9)
- Orange Book & Drugs@FDA parsers
- Trial-entity linking
- SCD Type 2 implementation
- **PubMed E-utilities client**
- **Trial ↔ Publication linking pipeline**

### Phase 4: Automation (Weeks 10-12)
- Incremental update logic
- Data quality checks
- **PubMed article sync DAG**
- Monitoring & alerting
- API development

---

## 9. Challenges and Best Practices

### 9.1 Common Challenges

| Challenge | Mitigation |
|-----------|------------|
| Drug name variations | Multi-source resolution + fuzzy matching |
| API rate limits | Request queuing, caching, bulk downloads |
| Data quality issues | Validation rules, confidence scoring |
| Entity disambiguation | Cross-reference identifiers (RXCUI, UNII) |
| Schema evolution | SCD Type 2, versioning |

### 9.2 Best Practices

1. **Always use authoritative identifiers** (RXCUI, UNII, MeSH ID, SNOMED CT SCTID)
2. **Cache API responses** to reduce external calls
3. **Implement idempotent pipelines** for safe reruns
4. **Track data lineage** (source, timestamp, version)
5. **Monitor harmonization quality** metrics continuously
6. **Use bulk downloads** when available for reference data
7. **Implement circuit breakers** for external API failures

### 9.3 Data Licensing Notes

| Source | License | Commercial Use |
|--------|---------|----------------|
| ClinicalTrials.gov | Public Domain | ✅ Yes |
| openFDA | Public Domain | ✅ Yes |
| RxNorm | UMLS (free registration) | ✅ Yes |
| MeSH | Public Domain | ✅ Yes |
| PubChem | Public Domain | ✅ Yes |
| PubMed | Public Domain | ✓ Yes |
| SNOMED CT | Affiliate License (free for members) | ✓ Yes (member countries) |
| MedDRA | Subscription Required | License needed |
| DrugBank | CC BY-NC 4.0 | Academic only |

**SNOMED CT Licensing Note:** SNOMED CT is free for use in SNOMED International member countries (US, UK, Australia, etc.). US users can access via NLM/UMLS. Non-member countries require an affiliate license.

---

## Appendix: Example API Responses

### RxNorm Drug Search Response
```json
{
  "drugGroup": {
    "name": "pirfenidone",
    "conceptGroup": [{
      "tty": "SBD",
      "conceptProperties": [{
        "rxcui": "1486960",
        "name": "pirfenidone 267 MG Oral Capsule [Esbriet]"
      }]
    }]
  }
}
```

### openFDA NDC Response
```json
{
  "results": [{
    "product_ndc": "50242-510",
    "brand_name": "ESBRIET",
    "generic_name": "PIRFENIDONE",
    "openfda": {
      "rxcui": ["745467"],
      "unii": ["D7NLD2R896"]
    }
  }]
}
```

### SNOMED CT Snowstorm Concept Search Response
```json
{
  "items": [{
    "conceptId": "28168000",
    "active": true,
    "definitionStatus": "FULLY_DEFINED",
    "moduleId": "900000000000207008",
    "effectiveTime": "20020131",
    "fsn": {
      "term": "Idiopathic fibrosing alveolitis (disorder)",
      "lang": "en"
    },
    "pt": {
      "term": "Idiopathic pulmonary fibrosis",
      "lang": "en"
    },
    "id": "28168000"
  }],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

### SNOMED CT → ICD-10 Map Entry Example
```json
{
  "referencedComponentId": "28168000",
  "mapTarget": "J84.112",
  "mapGroup": 1,
  "mapPriority": 1,
  "mapRule": "TRUE",
  "mapAdvice": "ALWAYS J84.112",
  "correlationId": "447561005"
}
```

### PubMed ESearch Response (NCT ID Lookup)
```json
{
  "esearchresult": {
    "count": "3",
    "retmax": "3",
    "idlist": ["34567890", "34123456", "33987654"],
    "translationset": [{
      "from": "NCT02476279[SI]",
      "to": "NCT02476279[Secondary Source ID]"
    }]
  }
}
```

### PubMed EFetch Article Response (XML Summary)
```xml
<PubmedArticle>
  <MedlineCitation>
    <PMID>34567890</PMID>
    <Article>
      <ArticleTitle>Efficacy and Safety of Pirfenidone in IPF: Results from ASCEND Trial</ArticleTitle>
      <Abstract>
        <AbstractText>BACKGROUND: Pirfenidone has shown efficacy in Phase 3 trials...</AbstractText>
      </Abstract>
      <AuthorList>
        <Author><LastName>King</LastName><ForeName>Talmadge E</ForeName></Author>
      </AuthorList>
      <Journal>
        <Title>New England Journal of Medicine</Title>
        <ISOAbbreviation>N Engl J Med</ISOAbbreviation>
      </Journal>
      <PublicationTypeList>
        <PublicationType>Randomized Controlled Trial</PublicationType>
        <PublicationType>Clinical Trial, Phase III</PublicationType>
      </PublicationTypeList>
    </Article>
    <MeshHeadingList>
      <MeshHeading>
        <DescriptorName MajorTopicYN="Y">Idiopathic Pulmonary Fibrosis</DescriptorName>
      </MeshHeading>
      <MeshHeading>
        <DescriptorName MajorTopicYN="N">Pirfenidone</DescriptorName>
      </MeshHeading>
    </MeshHeadingList>
  </MedlineCitation>
  <PubmedData>
    <ArticleIdList>
      <ArticleId IdType="pubmed">34567890</ArticleId>
      <ArticleId IdType="doi">10.1056/NEJMoa1402582</ArticleId>
      <ArticleId IdType="pmc">PMC4123456</ArticleId>
    </ArticleIdList>
    <ReferenceList>
      <Reference>
        <ArticleId IdType="pubmed">23571587</ArticleId>
      </Reference>
    </ReferenceList>
  </PubmedData>
</PubmedArticle>
```

### Trial-Publication Link Example
```json
{
  "trial_id": "NCT02476279",
  "linked_publications": [
    {
      "pmid": "34567890",
      "title": "Efficacy and Safety of Pirfenidone in IPF: Results from ASCEND Trial",
      "link_type": "results",
      "link_source": "pubmed_si",
      "confidence_score": 98.5,
      "is_primary_publication": true,
      "publication_date": "2014-05-29",
      "journal": "N Engl J Med"
    },
    {
      "pmid": "34123456",
      "title": "ASCEND Trial Protocol: A Phase 3 Study of Pirfenidone",
      "link_type": "protocol",
      "link_source": "pubmed_si",
      "confidence_score": 95.0,
      "is_primary_publication": false
    }
  ]
}
```

---

*Document Version: 1.2 | Updated: January 2026 | Added: PubMed Integration, SNOMED CT Integration*
