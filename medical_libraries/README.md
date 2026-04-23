# Drug Harmonization System

A comprehensive multi-source drug name harmonization system that integrates RxNorm, openFDA, and UNII APIs to provide standardized, validated drug information.

## Overview

This system combines three authoritative drug databases:
- **RxNorm** (NLM): Normalized drug naming system with RxCUI identifiers
- **openFDA** (FDA): Drug product information, NDC codes, and adverse events
- **UNII** (FDA): Unique ingredient identifiers with chemical data

## Components

### 1. RxNorm Client (`rxnorm_client.py`)
Interfaces with the NLM RxNorm API to retrieve:
- RxCUI identifiers
- Generic and brand names
- Drug ingredients
- ATC classification codes
- NDC codes
- Drug synonyms

**API Endpoint**: `https://rxnav.nlm.nih.gov/REST`

**Usage**:
```python
from medical_libraries.rxnorm_client import RxNormClient

client = RxNormClient()
drugs = client.search_drug_by_name("pirfenidone")
drug_info = client.get_drug_info(drugs[0].rxcui)
```

### 2. OpenFDA Client (`openfda_client.py`)
Interfaces with the FDA openFDA API to retrieve:
- NDC product codes
- Brand and generic names
- Manufacturer information
- Dosage forms and routes
- Strength information
- UNII codes
- Active ingredients
- Drug labels and adverse events

**API Endpoint**: `https://api.fda.gov/drug`

**Usage**:
```python
from medical_libraries.openfda_client import OpenFDAClient

client = OpenFDAClient()
drugs = client.search_drug_by_name("nintedanib")
labels = client.search_drug_labels("nintedanib")
```

### 3. UNII Client (`unii_client.py`)
Interfaces with the FDA UNII Substance Registry to retrieve:
- UNII codes
- CAS numbers
- Molecular formulas
- Chemical structure data (InChI, SMILES)
- Substance types

**API Endpoint**: `https://fdasis.nlm.nih.gov/srs/api/v1`

**Usage**:
```python
from medical_libraries.unii_client import UNIIClient

client = UNIIClient()
substances = client.search_substance_by_name("pirfenidone")
```

### 4. Drug Harmonizer (`drug_harmonizer.py`)
Unified orchestrator that combines all three sources to provide comprehensive, harmonized drug information.

**Usage**:
```python
from medical_libraries.drug_harmonizer import DrugHarmonizer

harmonizer = DrugHarmonizer()

# Single drug
result = harmonizer.harmonize_drug("pirfenidone")

# Batch processing
drugs = ["pirfenidone", "nintedanib", "prednisone"]
results = harmonizer.harmonize_drug_list(drugs)

# Export to JSON
harmonizer.export_to_json(results, "harmonized_drugs.json")
```

## Harmonized Drug Fields

The `HarmonizedDrugInfo` dataclass provides standardized fields:

### Primary Standardized Fields
- **`harmonized_drug_name`**: Primary standardized drug name (prefers generic)
- **`harmonized_generic_name`**: Standardized generic name
- **`harmonized_brand_name`**: Primary brand name
- **`strength`**: Drug strengths (e.g., ["500 mg", "800 mg"])
- **`route`**: Routes of administration (e.g., ["ORAL", "INTRAVENOUS"])
- **`dosage_form`**: Dosage forms (e.g., ["TABLET", "CAPSULE"])
- **`dose`**: Typical dose information
- **`administration_details`**: Administration instructions

### Identifiers & Codes
- **`rxcui`**: RxNorm Concept Unique Identifier
- **`unii_codes`**: FDA Unique Ingredient Identifiers
- **`ndc_codes`**: National Drug Codes
- **`atc_codes`**: Anatomical Therapeutic Chemical codes
- **`cas_numbers`**: Chemical Abstracts Service numbers

### Names & Synonyms
- **`all_brand_names`**: All brand names from all sources
- **`all_generic_names`**: All generic names from all sources
- **`all_synonyms`**: Complete list of drug synonyms
- **`rxnorm_ingredients`**: Active pharmaceutical ingredients

### Chemical Data
- **`molecular_formulas`**: Molecular formulas
- **`fda_manufacturers`**: Manufacturer names

### Metadata
- **`sources_used`**: List of APIs queried (e.g., ["RxNorm", "openFDA"])
- **`confidence_score`**: Quality score (0-100) based on data completeness

## Testing

Run the comprehensive test suite:

```bash
python test_drug_harmonization.py
```

The test suite includes:
- Individual API client tests (RxNorm, openFDA, UNII)
- Unified harmonization tests
- Batch processing tests
- IPF-specific drug tests

### Test Output Example

```
📊 HARMONIZED DRUG INFORMATION
────────────────────────────────────────────────────────────────

🎯 STANDARDIZED FIELDS:
   Harmonized Drug Name: pirfenidone
   Harmonized Generic Name: pirfenidone
   Harmonized Brand Name: PIRFENIDONE
   Strength: 267 MG, 801 MG
   Route(s): ORAL
   Dosage Form(s): TABLET, FILM COATED

🔍 IDENTIFIERS:
   RxNorm RXCUI: 1592254
   UNII Code(s): D7NLD2JX7U
   CAS Number(s): 53179-13-8
   ATC Code(s): L04AX05
   NDC Code(s): 45 codes

📊 METADATA:
   Sources Used: RxNorm, openFDA, UNII
   Confidence Score: 90.0/100
   Data Quality: 🟢 EXCELLENT
```

## Confidence Scoring

The system calculates a confidence score (0-100) based on:

- **Source Coverage** (30 points): 10 points per data source
- **Data Completeness** (40 points):
  - RxCUI present: +10
  - Brand names found: +10
  - Generic names found: +10
  - Identifiers (UNII/ATC) found: +10
- **Cross-validation** (30 points):
  - Multiple sources agree on generic name: +15
  - NDC or UNII codes present: +15

**Quality Levels**:
- 🟢 **EXCELLENT** (80-100): High confidence, multiple sources
- 🟡 **GOOD** (60-79): Good coverage, some validation
- 🟠 **FAIR** (40-59): Limited data, single source
- 🔴 **POOR** (0-39): Minimal information

## Example: Full Integration

```python
import logging
from medical_libraries.drug_harmonizer import DrugHarmonizer

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize harmonizer
harmonizer = DrugHarmonizer(
    use_rxnorm=True,
    use_openfda=True,
    use_unii=True
)

# Harmonize IPF drugs
ipf_drugs = [
    "pirfenidone",
    "nintedanib",
    "esbriet",
    "ofev"
]

results = harmonizer.harmonize_drug_list(ipf_drugs)

# Display results
for drug_name, info in results.items():
    print(f"{drug_name}:")
    print(f"  Harmonized Name: {info.harmonized_drug_name}")
    print(f"  Generic: {info.harmonized_generic_name}")
    print(f"  Strength: {', '.join(info.strength[:3])}")
    print(f"  Route: {', '.join(info.route)}")
    print(f"  Dosage Form: {', '.join(info.dosage_form)}")
    print(f"  RXCUI: {info.rxcui}")
    print(f"  Confidence: {info.confidence_score}/100")
    print()

# Export results
harmonizer.export_to_json(results, "ipf_drugs_harmonized.json")
```

## JSON Export Format

```json
{
  "pirfenidone": {
    "query_name": "pirfenidone",
    "harmonized_drug_name": "pirfenidone",
    "harmonized_generic_name": "pirfenidone",
    "harmonized_brand_name": "PIRFENIDONE",
    "strength": ["267 MG", "801 MG"],
    "route": ["ORAL"],
    "dosage_form": ["TABLET", "CAPSULE"],
    "dose": [],
    "administration_details": [],
    "rxcui": "1592254",
    "rxnorm_name": "pirfenidone",
    "brand_names": ["Esbriet", "PIRFENIDONE"],
    "generic_names": ["pirfenidone"],
    "atc_codes": ["L04AX05"],
    "ndc_codes": ["67857-520-03", "67857-521-30", ...],
    "unii_codes": ["D7NLD2JX7U"],
    "cas_numbers": ["53179-13-8"],
    "molecular_formulas": ["C12H11NO"],
    "sources_used": ["RxNorm", "openFDA", "UNII"],
    "confidence_score": 90.0
  }
}
```

## API Rate Limits

- **RxNorm**: No authentication required, rate-limited per IP
- **openFDA**: 240 requests per minute (1000/day without API key)
- **UNII**: No explicit rate limits documented

The clients implement exponential backoff and retry logic to handle rate limits gracefully.

## Error Handling

All clients include:
- Request retry logic (3 attempts by default)
- Exponential backoff on failures
- Comprehensive logging
- Graceful degradation (continues with partial data)

## Dependencies

Add to `requirements.txt`:
```
requests>=2.28.0
```

## Future Enhancements

- [ ] Add WHO ATC classification browser integration
- [ ] Implement DrugBank API integration
- [ ] Add PubChem cross-references
- [ ] Cache frequently accessed drugs
- [ ] Add drug interaction checking
- [ ] Implement fuzzy name matching for better search
- [ ] Add support for combination drugs

## License

This code integrates with public FDA and NLM APIs. Usage is subject to their respective Terms of Service.

## Support

For issues or questions, refer to the official API documentation:
- [RxNorm API](https://lhncbc.nlm.nih.gov/RxNav/APIs/RxNormAPIs.html)
- [openFDA API](https://open.fda.gov/apis/)
- [UNII Substance Registry](https://www.fda.gov/industry/structured-product-labeling-resources/unique-ingredient-identifier-unii)
