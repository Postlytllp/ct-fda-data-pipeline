from drug_harmonizer_with_purplebook import DrugHarmonizer
import json
import os

harmonizer = DrugHarmonizer(
    drugsfda_path='drug-drugsfda-0001-of-0001.json',
    ndc_path='package.txt', # dummy
    products_path='products.txt', # dummy
    patent_path='patent.txt', # dummy
    exclusivity_path='exclusivity.txt', # dummy
    purplebook_path='purplebook-search-january-data-download.csv', # dummy
    rxnconso_path='RXNCONSO.RRF', # dummy
    drug_label_path='drug-label_mini.json'
)

# Test case
drug_name = "TREPROSTINIL SODIUM"
print(f"Testing harmonization for: {drug_name}")

# Access internal search directly
matches = harmonizer._search_drug_labels(drug_name)
print(f"Found {len(matches)} matches.")

for i, m in enumerate(matches):
    print(f"\nMatch {i+1}:")
    print(f"  Brand: {m.get('openfda', {}).get('brand_name')}")
    print(f"  Generic: {m.get('openfda', {}).get('generic_name')}")
    # Truncate SPL to avoid massive output, but show enough
    spl = m.get('spl_product_data_elements', [])
    print(f"  SPL Elements: {spl}")
