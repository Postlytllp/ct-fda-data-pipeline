
import os
import sys
from label_indication_extractor import LabelIndicationExtractor

def debug_humira():
    url = "https://www.accessdata.fda.gov/drugsatfda_docs/label/2012/125057s232lbl.pdf"
    extractor = LabelIndicationExtractor()
    print(f"Downloading {url}...")
    pdf_path = extractor.download_pdf(url, "debug_humira.pdf")
    
    if not pdf_path:
        print("Failed to download")
        return

    print("Extracting text with FIXED logic...")
    indication = extractor.extract_indication_section(pdf_path)
    
    if indication:
        print(f"\n✓ SUCCESS: Extracted {len(indication)} characters")
        print("First 500 chars:")
        print(indication[:500])
        print("-" * 20)
        print("Last 500 chars:")
        print(indication[-500:])
        
        # Check if it contains the text past the previous cutoff
        check_phrase = "established in patients who have lost response"
        if check_phrase in indication:
            print("\n✓ Verification Passed: Found text past previous cutoff!")
        else:
            print("\n✗ Verification Failed: Did not find text past cutoff.")
    else:
        print("\n✗ FAILED: Could not extract section")

if __name__ == "__main__":
    debug_humira()
