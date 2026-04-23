"""
Test script to inspect EU CTIS API response structure
"""
import json
import logging
import sys
from data_sources.eu_clinical_trials import EUClinicalTrialsClient

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Test EU API for specific trial"""
    client = EUClinicalTrialsClient()
    
    # Test with the specific trial the user mentioned
    trial_id = "2025-521278-32-00"
    
    print(f"\n{'='*80}")
    print(f"Fetching trial: {trial_id}")
    print(f"{'='*80}\n")
    
    # Fetch raw trial data
    trial = client._fetch_full_trial(trial_id)
    
    if trial:
        print(f"\n{'='*80}")
        print("PARSED TRIAL DATA:")
        print(f"{'='*80}")
        print(f"Trial ID: {trial.eudract_number}")
        print(f"Title: {trial.title}")
        print(f"Condition: {trial.medical_condition}")
        print(f"Status: {trial.trial_status}")
        print(f"Phase: {trial.trial_phase}")
        print(f"Sponsor: {trial.sponsor}")
        print(f"Description: {trial.description[:200] if trial.description else 'EMPTY'}")
        print(f"Inclusion Criteria: {trial.inclusion_criteria[:200] if trial.inclusion_criteria else 'EMPTY'}")
        print(f"Exclusion Criteria: {trial.exclusion_criteria[:200] if trial.exclusion_criteria else 'EMPTY'}")
        print(f"Interventions: {trial.intervention_names}")
        
        # Save raw trial data for inspection
        if trial.raw_trial:
            output_file = f"d:\\CT_FDA\\data_pipeline\\data\\debug_eu_trial_{trial_id.replace('-', '_')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(trial.raw_trial, f, indent=2, ensure_ascii=False)
            print(f"\n{'='*80}")
            print(f"Raw trial data saved to: {output_file}")
            print(f"{'='*80}\n")
            
            # Print available keys in the raw response
            print(f"\n{'='*80}")
            print("RAW API RESPONSE STRUCTURE:")
            print(f"{'='*80}")
            print(f"Top-level keys: {list(trial.raw_trial.keys())}")
            
            auth_app = trial.raw_trial.get('authorizedApplication', {})
            if auth_app:
                print(f"authorizedApplication keys: {list(auth_app.keys())}")
                
                auth_part1 = auth_app.get('authorizedPartI', {})
                if auth_part1:
                    print(f"authorizedPartI keys: {list(auth_part1.keys())}")
                    
                    # Check for intervention-related fields
                    for key in auth_part1.keys():
                        if 'interv' in key.lower() or 'product' in key.lower() or 'imp' in key.lower():
                            print(f"  -> Found potential intervention field: {key}")
                            value = auth_part1[key]
                            if isinstance(value, list) and value:
                                print(f"     Type: list with {len(value)} items")
                                if value[0]:
                                    print(f"     First item type: {type(value[0])}")
                                    if isinstance(value[0], dict):
                                        print(f"     First item keys: {list(value[0].keys())}")
                            elif isinstance(value, dict):
                                print(f"     Type: dict with keys: {list(value.keys())}")
                    
                    # Check for eligibility-related fields
                    for key in auth_part1.keys():
                        if 'eligib' in key.lower() or 'criteri' in key.lower() or 'inclusion' in key.lower() or 'exclusion' in key.lower():
                            print(f"  -> Found potential eligibility field: {key}")
                            value = auth_part1[key]
                            if isinstance(value, str):
                                print(f"     Value preview: {value[:100]}...")
                            elif isinstance(value, dict):
                                print(f"     Type: dict with keys: {list(value.keys())}")
    else:
        print(f"ERROR: Failed to fetch trial {trial_id}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
