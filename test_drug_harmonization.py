"""
Comprehensive test script for drug harmonization system
Tests RxNorm, openFDA, UNII integration and harmonized fields
"""

import logging
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from medical_libraries.drug_harmonizer import DrugHarmonizer, HarmonizedDrugInfo
from medical_libraries.rxnorm_client import RxNormClient
from medical_libraries.openfda_client import OpenFDAClient
from medical_libraries.unii_client import UNIIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def print_separator(title=""):
    """Print a formatted separator"""
    if title:
        print(f"\n{'='*80}")
        print(f"  {title}")
        print(f"{'='*80}")
    else:
        print(f"{'='*80}")

def test_rxnorm_client():
    """Test RxNorm client with corrected API"""
    print_separator("Testing RxNorm Client")
    
    client = RxNormClient()
    
    test_drugs = ["pirfenidone", "nintedanib", "aspirin"]
    
    for drug_name in test_drugs:
        print(f"\n🔍 Searching RxNorm for: {drug_name}")
        
        # Search for drug
        drugs = client.search_drug_by_name(drug_name)
        
        if drugs:
            print(f"   ✓ Found {len(drugs)} result(s)")
            for i, drug in enumerate(drugs[:2], 1):
                print(f"     {i}. {drug.name} (RXCUI: {drug.rxcui}, TTY: {drug.tty})")
            
            # Get detailed info for first result
            if drugs[0].rxcui:
                print(f"\n📋 Detailed info for RXCUI {drugs[0].rxcui}:")
                drug_info = client.get_drug_info(drugs[0].rxcui)
                
                if drug_info:
                    print(f"     Name: {drug_info.name}")
                    print(f"     Generic: {drug_info.generic_name}")
                    if drug_info.brand_names:
                        print(f"     Brand Names: {', '.join(drug_info.brand_names[:3])}")
                    if drug_info.ingredients:
                        print(f"     Ingredients: {', '.join(drug_info.ingredients)}")
                    if drug_info.atc_codes:
                        print(f"     ATC Codes: {', '.join(drug_info.atc_codes)}")
                    if drug_info.ndc_codes:
                        print(f"     NDC Codes: {len(drug_info.ndc_codes)} codes")
        else:
            print(f"   ✗ No results found")
    
    print_separator()

def test_openfda_client():
    """Test openFDA client"""
    print_separator("Testing openFDA Client")
    
    client = OpenFDAClient()
    
    test_drugs = ["pirfenidone", "ibuprofen"]
    
    for drug_name in test_drugs:
        print(f"\n🔍 Searching openFDA for: {drug_name}")
        
        drugs = client.search_drug_by_name(drug_name, limit=3)
        
        if drugs:
            print(f"   ✓ Found {len(drugs)} result(s)")
            for i, drug in enumerate(drugs[:2], 1):
                print(f"\n     {i}. {drug.brand_name or drug.generic_name}")
                print(f"        Brand: {drug.brand_name}")
                print(f"        Generic: {drug.generic_name}")
                print(f"        NDC: {', '.join(drug.product_ndc[:2])}")
                if drug.dosage_form:
                    print(f"        Dosage Form: {drug.dosage_form}")
                if drug.route:
                    print(f"        Route: {', '.join(drug.route[:3])}")
                if drug.active_ingredients:
                    print(f"        Active Ingredients:")
                    for ing in drug.active_ingredients[:2]:
                        print(f"          - {ing.get('name', '')} ({ing.get('strength', '')})")
                if drug.unii:
                    print(f"        UNII: {', '.join(drug.unii[:2])}")
        else:
            print(f"   ✗ No results found")
    
    print_separator()

def test_unii_client():
    """Test UNII client"""
    print_separator("Testing UNII Client")
    
    client = UNIIClient()
    
    test_substances = ["pirfenidone", "caffeine"]
    
    for substance_name in test_substances:
        print(f"\n🔍 Searching UNII for: {substance_name}")
        
        substances = client.search_substance_by_name(substance_name)
        
        if substances:
            print(f"   ✓ Found {len(substances)} result(s)")
            for i, substance in enumerate(substances[:2], 1):
                print(f"\n     {i}. {substance.display_name}")
                print(f"        UNII: {substance.unii}")
                print(f"        Preferred Term: {substance.preferred_term}")
                if substance.cas_number:
                    print(f"        CAS Number: {substance.cas_number}")
                if substance.molecular_formula:
                    print(f"        Molecular Formula: {substance.molecular_formula}")
                if substance.substance_type:
                    print(f"        Type: {substance.substance_type}")
        else:
            print(f"   ✗ No results found")
    
    print_separator()

def test_drug_harmonizer():
    """Test unified drug harmonizer with all new fields"""
    print_separator("Testing Unified Drug Harmonizer")
    
    harmonizer = DrugHarmonizer(use_rxnorm=True, use_openfda=True, use_unii=True)
    
    # Test single drug harmonization
    test_drug = "pirfenidone"
    print(f"\n🔬 Harmonizing drug: {test_drug}\n")
    
    result = harmonizer.harmonize_drug(test_drug)
    
    if result:
        print(f"{'─'*80}")
        print(f"📊 HARMONIZED DRUG INFORMATION")
        print(f"{'─'*80}\n")
        
        # Display harmonized standardized fields
        print("🎯 STANDARDIZED FIELDS:")
        print(f"   Harmonized Drug Name: {result.harmonized_drug_name or 'N/A'}")
        print(f"   Harmonized Generic Name: {result.harmonized_generic_name or 'N/A'}")
        print(f"   Harmonized Brand Name: {result.harmonized_brand_name or 'N/A'}")
        
        if result.strength:
            print(f"   Strength: {', '.join(result.strength[:5])}")
        if result.route:
            print(f"   Route(s): {', '.join(result.route[:5])}")
        if result.dosage_form:
            print(f"   Dosage Form(s): {', '.join(result.dosage_form[:5])}")
        
        print(f"\n🔍 IDENTIFIERS:")
        if result.rxcui:
            print(f"   RxNorm RXCUI: {result.rxcui}")
        if result.unii_codes:
            print(f"   UNII Code(s): {', '.join(result.unii_codes[:3])}")
        if result.cas_numbers:
            print(f"   CAS Number(s): {', '.join(result.cas_numbers[:3])}")
        if result.atc_codes:
            print(f"   ATC Code(s): {', '.join(result.atc_codes[:3])}")
        if result.ndc_codes:
            print(f"   NDC Code(s): {len(result.ndc_codes)} codes")
        
        print(f"\n📝 NAMES & SYNONYMS:")
        if result.all_brand_names:
            brand_list = ', '.join(sorted(result.all_brand_names)[:5])
            print(f"   Brand Names ({len(result.all_brand_names)}): {brand_list}")
        if result.all_generic_names:
            generic_list = ', '.join(sorted(result.all_generic_names)[:5])
            print(f"   Generic Names ({len(result.all_generic_names)}): {generic_list}")
        if result.rxnorm_ingredients:
            print(f"   Ingredients: {', '.join(result.rxnorm_ingredients[:3])}")
        
        print(f"\n🧪 CHEMICAL DATA:")
        if result.molecular_formulas:
            print(f"   Molecular Formula(s): {', '.join(result.molecular_formulas[:3])}")
        
        print(f"\n📊 METADATA:")
        print(f"   Sources Used: {', '.join(result.sources_used)}")
        print(f"   Confidence Score: {result.confidence_score:.1f}/100")
        
        # Quality indicator
        if result.confidence_score >= 80:
            quality = "🟢 EXCELLENT"
        elif result.confidence_score >= 60:
            quality = "🟡 GOOD"
        elif result.confidence_score >= 40:
            quality = "🟠 FAIR"
        else:
            quality = "🔴 POOR"
        print(f"   Data Quality: {quality}")
        
        print(f"\n{'─'*80}")
    else:
        print(f"   ✗ Failed to harmonize {test_drug}")
    
    print_separator()

def test_batch_harmonization():
    """Test batch drug harmonization"""
    print_separator("Testing Batch Harmonization")
    
    harmonizer = DrugHarmonizer(use_rxnorm=True, use_openfda=True, use_unii=True)
    
    # Common drugs for testing
    test_drugs = [
        "pirfenidone",
        "nintedanib", 
        "prednisone",
        "acetaminophen",
        "ibuprofen"
    ]
    
    print(f"\n🔬 Harmonizing {len(test_drugs)} drugs...\n")
    
    results = harmonizer.harmonize_drug_list(test_drugs)
    
    # Display summary table
    print(f"\n{'─'*120}")
    print(f"{'Drug Name':<20} {'Harmonized Name':<25} {'Sources':<15} {'Confidence':<12} {'Strength':<25}")
    print(f"{'─'*120}")
    
    for drug_name, info in results.items():
        harmonized = info.harmonized_drug_name or 'N/A'
        sources = '+'.join(info.sources_used) if info.sources_used else 'None'
        confidence = f"{info.confidence_score:.1f}/100"
        strength = ', '.join(info.strength[:2]) if info.strength else 'N/A'
        
        print(f"{drug_name:<20} {harmonized[:24]:<25} {sources:<15} {confidence:<12} {strength[:24]:<25}")
    
    print(f"{'─'*120}\n")
    
    # Export to JSON
    output_file = "harmonized_drugs_test.json"
    harmonizer.export_to_json(results, output_file)
    print(f"✅ Results exported to: {output_file}\n")
    
    print_separator()

def test_ipf_drugs():
    """Test with IPF-specific drugs"""
    print_separator("Testing IPF-Related Drugs")
    
    harmonizer = DrugHarmonizer(use_rxnorm=True, use_openfda=True, use_unii=True)
    
    ipf_drugs = [
        "pirfenidone",
        "nintedanib",
        "esbriet",  # Brand name for pirfenidone
        "ofev",     # Brand name for nintedanib
        "n-acetylcysteine"
    ]
    
    print(f"\n🔬 Harmonizing IPF drugs...\n")
    
    results = harmonizer.harmonize_drug_list(ipf_drugs)
    
    for drug_name, info in results.items():
        print(f"\n📋 {drug_name}:")
        print(f"   Harmonized: {info.harmonized_drug_name}")
        print(f"   Generic: {info.harmonized_generic_name}")
        print(f"   Brand: {info.harmonized_brand_name}")
        if info.route:
            print(f"   Route: {', '.join(info.route[:3])}")
        if info.dosage_form:
            print(f"   Dosage: {', '.join(info.dosage_form[:3])}")
        print(f"   Confidence: {info.confidence_score:.1f}/100")
    
    print_separator()

def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("  DRUG HARMONIZATION SYSTEM - COMPREHENSIVE TEST SUITE")
    print("="*80 + "\n")
    
    try:
        # Test individual clients
        print("PHASE 1: Testing Individual API Clients")
        print("─" * 80)
        test_rxnorm_client()
        test_openfda_client()
        test_unii_client()
        
        # Test harmonizer
        print("\nPHASE 2: Testing Unified Harmonization")
        print("─" * 80)
        test_drug_harmonizer()
        test_batch_harmonization()
        test_ipf_drugs()
        
        print("\n" + "="*80)
        print("  ✅ ALL TESTS COMPLETED SUCCESSFULLY")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        print(f"\n❌ Test suite failed: {e}\n")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
