"""Simple diagnostic to check Orange Book data for TREPROSTINIL"""
import sys

# Load Orange Book products
print("Loading Orange Book products.txt...")
products = []
try:
    with open('products.txt', 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        headers = [h.strip() for h in lines[0].split('~')]
        
        for line in lines[1:]:
            if line.strip():
                values = [v.strip() for v in line.split('~')]
                while len(values) < len(headers):
                    values.append('')
                products.append(dict(zip(headers, values)))
    print(f"Loaded {len(products)} products")
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

# Search for TREPROSTINIL
print("\nSearching for products containing 'TREPROSTINIL'...")
found = []
for p in products:
    ing = p.get('Ingredient', '').lower()
    trade = p.get('Trade_Name', '').lower()
    if 'treprostinil' in ing or 'treprostinil' in trade:
        found.append(p)

print(f"Found {len(found)} matches")

if found:
    print("\nFirst 5 matches:")
    for i, p in enumerate(found[:5], 1):
        print(f"\n{i}. Ingredient: {p.get('Ingredient', 'N/A')}")
        print(f"   Trade Name: {p.get('Trade_Name', 'N/A')}")
        print(f"   Applicant: {p.get('Applicant', 'N/A')}")
        print(f"   Appl_No: {p.get('Appl_No', 'N/A')}")
        
# Check exact matches for "treprostinil sodium"
print("\n" + "="*60)
print("Checking for EXACT match: 'treprostinil sodium'")
exact_matches = [p for p in products if p.get('Ingredient', '').lower() == 'treprostinil sodium']
print(f"Exact matches: {len(exact_matches)}")

if exact_matches:
    for p in exact_matches[:3]:
        print(f"  - {p.get('Trade_Name', 'N/A')} ({p.get('Applicant', 'N/A')})")
