import fitz  # PyMuPDF
import unicodedata
import sys
from indication_extraction import extract_dashed_section, format_extracted_text, download_pdf

def analyze_unicode_characters(text):
    """
    Analyze and display unicode characters in the text.
    """
    unicode_info = []
    
    for i, char in enumerate(text):
        if ord(char) > 127:  # Non-ASCII character
            unicode_info.append({
                'position': i,
                'character': char,
                'unicode_code': f'U+{ord(char):04X}',
                'unicode_name': unicodedata.name(char, 'UNKNOWN'),
                'category': unicodedata.category(char),
                'context': text[max(0, i-20):min(len(text), i+20)]
            })
    
    return unicode_info

def extract_and_analyze_indications(pdf_path, output_file="output.txt"):
    """
    Extract indications section and analyze unicode characters.
    Save results to output file.
    """
    print(f"\n{'='*60}")
    print(f"Processing: {pdf_path}")
    print(f"{'='*60}\n")
    
    # Extract the raw section
    raw_section = extract_dashed_section(pdf_path, "INDICATIONS AND USAGE")
    
    if not raw_section:
        print("Failed to extract section")
        return
    
    # Analyze unicode in raw section
    print("Analyzing unicode characters in RAW extracted text...")
    unicode_chars = analyze_unicode_characters(raw_section)
    
    # Format the section
    formatted_section = format_extracted_text(raw_section)
    
    # Analyze unicode in formatted section
    print("Analyzing unicode characters in FORMATTED text...")
    formatted_unicode_chars = analyze_unicode_characters(formatted_section)
    
    # Write results to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("UNICODE ANALYSIS REPORT\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"PDF File: {pdf_path}\n")
        f.write(f"Raw section length: {len(raw_section)} characters\n")
        f.write(f"Formatted section length: {len(formatted_section)} characters\n")
        f.write(f"Unicode characters found in raw: {len(unicode_chars)}\n")
        f.write(f"Unicode characters found in formatted: {len(formatted_unicode_chars)}\n\n")
        
        # Write unicode character details for RAW section
        f.write("="*80 + "\n")
        f.write("UNICODE CHARACTERS IN RAW SECTION\n")
        f.write("="*80 + "\n\n")
        
        for info in unicode_chars[:50]:  # Limit to first 50
            f.write(f"Position: {info['position']}\n")
            f.write(f"Character: '{info['character']}'\n")
            f.write(f"Unicode: {info['unicode_code']}\n")
            f.write(f"Name: {info['unicode_name']}\n")
            f.write(f"Category: {info['category']}\n")
            f.write(f"Context: ...{info['context']}...\n")
            f.write("-" * 40 + "\n")
        
        if len(unicode_chars) > 50:
            f.write(f"\n... and {len(unicode_chars) - 50} more unicode characters\n\n")
        
        # Write unicode character details for FORMATTED section
        f.write("\n" + "="*80 + "\n")
        f.write("UNICODE CHARACTERS IN FORMATTED SECTION\n")
        f.write("="*80 + "\n\n")
        
        for info in formatted_unicode_chars[:50]:  # Limit to first 50
            f.write(f"Position: {info['position']}\n")
            f.write(f"Character: '{info['character']}'\n")
            f.write(f"Unicode: {info['unicode_code']}\n")
            f.write(f"Name: {info['unicode_name']}\n")
            f.write(f"Category: {info['category']}\n")
            f.write(f"Context: ...{info['context']}...\n")
            f.write("-" * 40 + "\n")
        
        if len(formatted_unicode_chars) > 50:
            f.write(f"\n... and {len(formatted_unicode_chars) - 50} more unicode characters\n\n")
        
        # Write the full RAW section
        f.write("\n" + "="*80 + "\n")
        f.write("FULL RAW INDICATIONS AND USAGE SECTION\n")
        f.write("="*80 + "\n\n")
        f.write(raw_section)
        f.write("\n\n")
        
        # Write the full FORMATTED section
        f.write("="*80 + "\n")
        f.write("FULL FORMATTED INDICATIONS AND USAGE SECTION\n")
        f.write("="*80 + "\n\n")
        f.write(formatted_section)
        f.write("\n\n")
        
        # Write byte representation of first 500 chars
        f.write("="*80 + "\n")
        f.write("BYTE REPRESENTATION (First 500 chars of raw)\n")
        f.write("="*80 + "\n\n")
        sample = raw_section[:500]
        f.write(f"String: {repr(sample)}\n\n")
        f.write("Bytes: ")
        f.write(' '.join(f'{ord(c):02X}' for c in sample[:100]))
        f.write("\n\n")
    
    print(f"\nResults saved to: {output_file}")
    print(f"\nFirst 500 characters of RAW section:")
    print("-" * 60)
    print(raw_section[:500])
    print("-" * 60)
    
    print(f"\nFirst 500 characters of FORMATTED section:")
    print("-" * 60)
    print(formatted_section[:500])
    print("-" * 60)
    
    print(f"\nUnicode characters found in raw: {len(unicode_chars)}")
    print(f"Unicode characters found in formatted: {len(formatted_unicode_chars)}")
    
    if unicode_chars:
        print("\nSample unicode characters (first 10):")
        for info in unicode_chars[:10]:
            print(f"  {info['unicode_code']} ({info['unicode_name']}): '{info['character']}'")

if __name__ == "__main__":
    # Test with the first PDF from the example
    test_url = "https://www.accessdata.fda.gov/drugsatfda_docs/label/2021/125514s096lbl.pdf"
    
    print("="*60)
    print("UNICODE EXTRACTION TEST")
    print("="*60)
    
    # Download PDF
    pdf_path = download_pdf(test_url)
    
    if pdf_path:
        # Extract and analyze
        extract_and_analyze_indications(pdf_path, "output.txt")
        print("\n" + "="*60)
        print("Test completed! Check output.txt for full results.")
        print("="*60)
    else:
        print("Failed to download PDF")
