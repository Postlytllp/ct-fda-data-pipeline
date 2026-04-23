
import json
import os
import sys
from drug_harmonizer_with_purplebook import DrugHarmonizer
from datetime import datetime

def run_test_harmonization():
    print("=" * 70)
    print("TESTING DRUG HARMONIZER WITH IMPROVED PARSER")
    print("=" * 70)
    
    # Initialize harmonizer with same paths as main script
    try:
        harmonizer = DrugHarmonizer(
            drugsfda_path='drug-drugsfda-0001-of-0001.json',
            ndc_path='drug-ndc-0001-of-0001.json',
            products_path='products.txt',
            patent_path='patent.txt',
            exclusivity_path='exclusivity.txt',
            purplebook_path='purplebook-search-january-data-download.csv',
            rxnconso_path='RXNCONSO.RRF',
            drug_label_path='drug-label_mini.json'
        )
    except Exception as e:
        print(f"Error checking files or initializing: {e}")
        # If files are missing (e.g. in a different dir), this might fail.
        # Assuming run from valid CWD.
        return

    drugs_to_test = ["Humira", "Treprostinil"]
    results = {}
    
    for drug in drugs_to_test:
        print(f"\nHarmonizing: {drug}...")
        try:
            result = harmonizer.harmonize_drug(drug)
            results[drug] = result
            
            # Print brief summary of extracted indications
            history = result.get('indication_approval_history', [])
            print(f"  Found {len(history)} indication history entries")
            if history:
                print(f"  Latest indication length: {len(history[-1].get('indication', ''))} chars")
                
        except Exception as e:
            print(f"  Error harmonizing {drug}: {e}")
            results[drug] = {"error": str(e)}
            
    # Save output with timestamp
    output_file = "test_harmonization_output_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to: {output_file}")
        
    except Exception as e:
        print(f"Error saving output: {e}")

if __name__ == "__main__":
    run_test_harmonization()
