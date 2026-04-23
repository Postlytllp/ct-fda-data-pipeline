"""
Test script for updated FDA API clients
"""
import logging
from medical_libraries.openfda_client import OpenFDAClient
from medical_libraries.unii_client import UNIIClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_openfda_ndc():
    """Test openFDA NDC API with new search pattern"""
    print("\n" + "="*80)
    print("Testing openFDA NDC API")
    print("="*80)
    
    client = OpenFDAClient()
    
    # Test with secukinumab (as suggested by user)
    test_drugs = ['secukinumab', 'pirfenidone', 'nintedanib']
    
    for drug_name in test_drugs:
        print(f"\nSearching for: {drug_name}")
        print("-" * 40)
        
        results = client.search_drug_by_name(drug_name, limit=5)
        
        if results:
            print(f"Found {len(results)} results:")
            for i, drug in enumerate(results[:3], 1):
                print(f"\n{i}. Brand: {drug.brand_name}")
                print(f"   Generic: {drug.generic_name}")
                print(f"   NDC: {drug.product_ndc[0] if drug.product_ndc else 'N/A'}")
                print(f"   Dosage Form: {drug.dosage_form}")
                print(f"   Route: {', '.join(drug.route) if drug.route else 'N/A'}")
                print(f"   UNII: {', '.join(drug.unii[:2]) if drug.unii else 'N/A'}")
        else:
            print(f"No results found for {drug_name}")

def test_unii_api():
    """Test openFDA UNII API with new search pattern"""
    print("\n" + "="*80)
    print("Testing openFDA UNII API")
    print("="*80)
    
    client = UNIIClient()
    
    # Test with secukinumab (as suggested by user)
    test_substances = ['secukinumab', 'pirfenidone', 'nintedanib']
    
    for substance_name in test_substances:
        print(f"\nSearching for: {substance_name}")
        print("-" * 40)
        
        results = client.search_substance_by_name(substance_name)
        
        if results:
            print(f"Found {len(results)} results:")
            for i, substance in enumerate(results[:3], 1):
                print(f"\n{i}. Substance Name: {substance.preferred_term}")
                print(f"   UNII: {substance.unii}")
                print(f"   Display Name: {substance.display_name}")
                print(f"   Type: {substance.substance_type or 'N/A'}")
                print(f"   CAS Number: {substance.cas_number or 'N/A'}")
                print(f"   Molecular Formula: {substance.molecular_formula or 'N/A'}")
        else:
            print(f"No results found for {substance_name}")
    
    # Test get by UNII code
    print("\n" + "="*80)
    print("Testing get substance by UNII code")
    print("="*80)
    
    # Example UNII for pirfenidone: 12239GYV3Q
    test_unii = "12239GYV3Q"
    print(f"\nFetching UNII: {test_unii}")
    print("-" * 40)
    
    result = client.get_substance_by_unii(test_unii)
    
    if result:
        print(f"Found: {result.preferred_term}")
        print(f"UNII: {result.unii}")
        print(f"Display Name: {result.display_name}")
        print(f"Type: {result.substance_type or 'N/A'}")
        print(f"CAS Number: {result.cas_number or 'N/A'}")
        print(f"Molecular Formula: {result.molecular_formula or 'N/A'}")
    else:
        print(f"No result found for UNII {test_unii}")

if __name__ == "__main__":
    test_openfda_ndc()
    test_unii_api()
    
    print("\n" + "="*80)
    print("API tests completed!")
    print("="*80)
