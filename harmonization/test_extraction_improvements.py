
import os
import sys
from label_indication_extractor import LabelIndicationExtractor

def test_specific_drugs():
    print("=" * 60)
    print("TESTING IMPROVED EXTRACTION LOGIC")
    print("=" * 60)
    
    extractor = LabelIndicationExtractor()
    
    test_cases = [
        {
            "name": "HUMIRA (Adalimumab)",
            "url": "https://www.accessdata.fda.gov/drugsatfda_docs/label/2021/125057s417lbl.pdf", # Recent label
            "expected_pattern": "Smart Header Detection (Drug Name Exclusion)"
        },
        {
            "name": "REMODULIN (Treprostinil)",
            "url": "https://www.accessdata.fda.gov/drugsatfda_docs/label/2018/021272s026lbl.pdf",
            "expected_pattern": "Dynamic Delimiter (Dashed)"
        }
    ]
    
    for case in test_cases:
        print("\n" + "-" * 50)
        print(f"Testing: {case['name']}")
        print(f"URL: {case['url']}")
        print("-" * 50)
        
        pdf_path = extractor.download_pdf(case['url'])
        if pdf_path:
            text = extractor.extract_indication_section(pdf_path)
            if text:
                print(f"\n✓ SUCCESS: Extracted {len(text)} characters")
                print("First 500 chars:")
                print(text[:500])
                print("-" * 20)
                print("Last 200 chars:")
                print(text[-200:])
            else:
                print("\n✗ FAILED: Could not extract section")
        else:
            print("\n✗ FAILED: Could not download PDF")

if __name__ == "__main__":
    test_specific_drugs()
