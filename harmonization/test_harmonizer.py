"""
Test script to verify the DrugHarmonizer is working correctly with exact lowercase matching.
"""
import json
from drug_harmonizer_with_purplebook import DrugHarmonizer

# Initialize harmonizer
print("Initializing DrugHarmonizer...")
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

# Test with TREPROSTINIL SODIUM (uppercase - should match lowercase in data)
print("\n" + "="*80)
print("Testing: TREPROSTINIL SODIUM (uppercase)")
print("="*80)
result1 = harmonizer.harmonize_drug("TREPROSTINIL SODIUM")
print(f"\nSources found: {result1['sources_used']}")
print(f"Confidence score: {result1['confidence_score']}")
print(f"Harmonized generic name: {result1['harmonized_generic_name']}")
print(f"Harmonized brand name: {result1['harmonized_brand_name']}")
print(f"Generic names found: {result1['generic_names']}")
print(f"Brand names found: {result1['brand_names']}")
print(f"First approval date: {result1.get('first_approval_date')}")
print(f"Recent approval dates (first 5): {result1.get('recent_approval_dates', [])[:5]}")

print("\nApplication Timeline:")
for app in result1.get('applications', []):
    print(f"  App: {app['application_number']} ({app['application_type']})")
    for event in app['events']: # Print all events to see patent submissions
        print(f"    - {event['date']} [{event['source']}] {event['submission_type']}: {event['status']} ({event['description']})")

# Test with treprostinil sodium (lowercase)
print("\n" + "="*80)
print("Testing: treprostinil sodium (lowercase)")
print("="*80)
result2 = harmonizer.harmonize_drug("treprostinil sodium")
print(f"\nSources found: {result2['sources_used']}")
print(f"Confidence score: {result2['confidence_score']}")
print(f"Harmonized generic name: {result2['harmonized_generic_name']}")
print(f"Harmonized brand name: {result2['harmonized_brand_name']}")

# Test with Treprostinil Sodium (mixed case)
print("\n" + "="*80)
print("Testing: Treprostinil Sodium (mixed case)")
print("="*80)
result3 = harmonizer.harmonize_drug("Treprostinil Sodium")
print(f"\nSources found: {result3['sources_used']}")
print(f"Confidence score: {result3['confidence_score']}")
print(f"Harmonized generic name: {result3['harmonized_generic_name']}")
print(f"Harmonized brand name: {result3['harmonized_brand_name']}")

# Check if all three queries return the same results
print("\n" + "="*80)
print("Verification: All queries should return identical results")
print("="*80)
print(f"UPPERCASE sources: {result1['sources_used']}")
print(f"lowercase sources: {result2['sources_used']}")
print(f"MixedCase sources: {result3['sources_used']}")
print(f"\nAll match: {result1['sources_used'] == result2['sources_used'] == result3['sources_used']}")

# Show the normalized name that's being used for lookup
print("\n" + "="*80)
print("Debug: Normalized names")
print("="*80)
print(f"'TREPROSTINIL SODIUM' normalized to: '{harmonizer._normalize_name('TREPROSTINIL SODIUM')}'")
print(f"'treprostinil sodium' normalized to: '{harmonizer._normalize_name('treprostinil sodium')}'")
print(f"'Treprostinil Sodium' normalized to: '{harmonizer._normalize_name('Treprostinil Sodium')}'")

# Check what's actually in the Orange Book index
print("\n" + "="*80)
print("Debug: Checking Orange Book index")
print("="*80)
search_key = harmonizer._normalize_name("TREPROSTINIL SODIUM")
print(f"Searching Orange Book ingredient index with key: '{search_key}'")
if search_key in harmonizer.orangebook_by_ingredient:
    print(f"✓ Found in orangebook_by_ingredient index!")
    print(f"  Number of matches: {len(harmonizer.orangebook_by_ingredient[search_key])}")
else:
    print(f"✗ NOT found in orangebook_by_ingredient index")
    # Show similar keys
    similar_keys = [k for k in harmonizer.orangebook_by_ingredient.keys() if 'treprostinil' in k]
    if similar_keys:
        print(f"  Similar keys found: {similar_keys[:10]}")
    else:
        print("  No similar keys found containing 'treprostinil'")

print("\n" + "="*80)
print("Test complete!")
print("="*80)
