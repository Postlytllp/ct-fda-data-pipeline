
import os
import sys
import json
from drug_harmonizer_with_purplebook import DrugHarmonizer

def test_rxnorm_integration():
    # Paths
    base_dir = "d:/CT_FDA/data_pipeline/harmonization"
    drugsfda_path = os.path.join(base_dir, "drug-drugsfda-0001-of-0001.json")
    ndc_path = os.path.join(base_dir, "drug-ndc-0001-of-0001.json")
    products_path = os.path.join(base_dir, "products.txt")
    patent_path = os.path.join(base_dir, "patent.txt")
    exclusivity_path = os.path.join(base_dir, "exclusivity.txt")
    purplebook_path = os.path.join(base_dir, "purplebook-search-january-data-download.csv")
    rxnconso_path = os.path.join(base_dir, "RXNCONSO.RRF")

    print(f"Initializing DrugHarmonizer with RxNorm path: {rxnconso_path}")
    harmonizer = DrugHarmonizer(
        drugsfda_path=drugsfda_path,
        ndc_path=ndc_path,
        products_path=products_path,
        patent_path=patent_path,
        exclusivity_path=exclusivity_path,
        purplebook_path=purplebook_path,
        rxnconso_path=rxnconso_path
    )

    # Test Case 1: Known RxNorm concept
    # "17-hydrocorticosteroid" is a synonym for "17-hydroxycorticosteroid" (RXCUI 19)
    query = "17-hydrocorticosteroid"
    print(f"\n--- Testing Query: '{query}' ---")
    result = harmonizer.harmonize_drug(query)
    
    print(f"Sources Used: {result['sources_used']}")
    print(f"Synonyms Found: {len(result['synonyms'])}")
    
    expected_synonym = "17-hydroxycorticosteroid"
    if expected_synonym in result['synonyms']:
        print(f"PASS: Found expected synonym '{expected_synonym}'")
    else:
        print(f"FAIL: Did not find expected synonym '{expected_synonym}'")
        print("First 10 synonyms:", result['synonyms'][:10])
        
    if "RxNorm" in result['sources_used']:
        print("PASS: RxNorm listed in sources")
    else:
        print("FAIL: RxNorm NOT listed in sources")

    # Test Case 2: Non-existent term (sanity check)
    query_dummy = "NonExistentDrugXYZ123"
    print(f"\n--- Testing Query: '{query_dummy}' ---")
    result_dummy = harmonizer.harmonize_drug(query_dummy)
    if "RxNorm" not in result_dummy['sources_used']:
        print("PASS: RxNorm correctly not used for dummy query")
    else:
        print("FAIL: RxNorm used for dummy query?")

if __name__ == "__main__":
    test_rxnorm_integration()
