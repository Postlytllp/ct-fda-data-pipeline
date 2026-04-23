"""
Test script to verify interventional trial filtering
"""
import logging
from data_sources.clinical_trials_gov import ClinicalTrialsGovClient
from data_sources.eu_clinical_trials import EUClinicalTrialsClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_us_interventional_filter():
    """Test ClinicalTrials.gov interventional filter"""
    print("\n" + "="*80)
    print("Testing ClinicalTrials.gov Interventional Filter")
    print("="*80)
    
    client = ClinicalTrialsGovClient()
    trials = client.search_ipf_trials(limit=10)
    
    print(f"\nFound {len(trials)} IPF trials")
    print("\nVerifying all trials are interventional:")
    print("-" * 80)
    
    interventional_count = 0
    observational_count = 0
    
    for i, trial in enumerate(trials, 1):
        study_type = trial.study_type
        if 'interventional' in study_type.lower():
            interventional_count += 1
            status = "✓ Interventional"
        else:
            observational_count += 1
            status = "✗ NOT Interventional"
        
        print(f"{i}. {trial.nct_id}: {trial.title[:60]}...")
        print(f"   Study Type: {study_type} {status}")
        print(f"   Phase: {trial.phase}")
    
    print("\n" + "="*80)
    print(f"Summary:")
    print(f"  Interventional: {interventional_count}")
    print(f"  Observational: {observational_count}")
    print("="*80)
    
    if observational_count > 0:
        print("⚠️ WARNING: Found observational trials in results!")
    else:
        print("✓ SUCCESS: All trials are interventional!")

def test_eu_interventional_filter():
    """Test EU CTIS interventional filter"""
    print("\n" + "="*80)
    print("Testing EU CTIS Interventional Filter")
    print("="*80)
    
    client = EUClinicalTrialsClient()
    trials = client.search_ipf_trials(max_pages=1)
    
    print(f"\nFound {len(trials)} IPF trials")
    print("\nVerifying all trials are interventional:")
    print("-" * 80)
    
    interventional_count = 0
    observational_count = 0
    
    for i, trial in enumerate(trials[:10], 1):
        trial_type = trial.trial_type
        if 'interventional' in trial_type.lower():
            interventional_count += 1
            status = "✓ Interventional"
        else:
            observational_count += 1
            status = "✗ NOT Interventional"
        
        print(f"{i}. {trial.eudract_number}: {trial.title[:60]}...")
        print(f"   Trial Type: {trial_type} {status}")
        print(f"   Phase: {trial.trial_phase}")
    
    print("\n" + "="*80)
    print(f"Summary:")
    print(f"  Interventional: {interventional_count}")
    print(f"  Observational: {observational_count}")
    print("="*80)
    
    if observational_count > 0:
        print("⚠️ WARNING: Found observational trials in results!")
    else:
        print("✓ SUCCESS: All trials are interventional!")

if __name__ == "__main__":
    test_us_interventional_filter()
    test_eu_interventional_filter()
    
    print("\n" + "="*80)
    print("Interventional filter tests completed!")
    print("="*80)
