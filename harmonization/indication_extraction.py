import fitz  # PyMuPDF
import re
import unicodedata
import requests
import os
import sys
from pathlib import Path

def download_pdf(pdf_url, output_folder="label_pdf"):
    """
    Download PDF from URL and save to the specified folder.
    
    Args:
        pdf_url: URL of the PDF to download
        output_folder: Folder to save the PDF (default: "label_pdf")
    
    Returns:
        Path to the downloaded PDF file, or None if download failed
    """
    try:
        # Create output folder if it doesn't exist
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        
        # Extract filename from URL
        filename = os.path.basename(pdf_url)
        if not filename.endswith('.pdf'):
            filename = "downloaded.pdf"
        
        pdf_path = os.path.join(output_folder, filename)
        
        # Download with headers
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        
        print(f"Downloading PDF from: {pdf_url}")
        response = requests.get(pdf_url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        # Save to file
        with open(pdf_path, "wb") as f:
            f.write(response.content)
        
        print(f"PDF downloaded to: {pdf_path}")
        return pdf_path
        
    except Exception as e:
        print(f"Error downloading PDF from {pdf_url}: {e}")
        return None


def format_extracted_text(text):
    """
    Format extracted text - replace bullet unicode and remove unnecessary line breaks.
    
    Args:
        text: Raw extracted text
    
    Returns:
        Text with \uf0b7 replaced by - and unnecessary line breaks removed
    """
    if not text:
        return text
    
    # Step 1: Replace \uf0b7 with -
    text = text.replace('\uf0b7', '-')
    
    # Remove line breaks after standalone - characters
    text = text.replace('-\n', '- ')
    
    # Step 2: Remove unnecessary line breaks
    lines = text.split('\n')
    cleaned_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i].rstrip()
        
        # Skip empty lines - but collapse consecutive empty lines
        if not line:
            # Only add if the last line was not already empty
            if not cleaned_lines or cleaned_lines[-1] != '':
                cleaned_lines.append('')
            i += 1
            continue
        
        # Check if this is a standalone bullet/marker line (just '-', 'o', etc.)
        # For '-' and 'o', we want to join them with the next line
        if line.strip() in ['-']:
            # Join with next line if it exists
            if i + 1 < len(lines) and lines[i + 1].strip():
                current_line = line + ' ' + lines[i + 1].strip()
                i += 2
                # Continue processing this combined line
            else:
                cleaned_lines.append(line)
                i += 1
                continue
        elif line.strip() in ['*', '•']:
            cleaned_lines.append(line)
            i += 1
            continue
        
        # Start building a complete line
        current_line = line
        
        # Keep joining with next lines if current line doesn't end with:
        # - Sentence ending punctuation (. ! ? : ))
        # - And next line exists and doesn't look like a new section
        while i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            
            # Stop if next line is empty (paragraph break)
            if not next_line:
                break
            
            # Stop if next line is a bullet marker
            if next_line in ['-', 'o', '*', '•']:
                break
            
            # Stop if current line ends with sentence-ending punctuation
            if current_line.rstrip().endswith(('.', '!', '?', ':', ')')):
                break
            
            # Stop if next line starts with uppercase and looks like a header
            # (short line, mostly uppercase, or starts new section)
            if next_line and next_line[0].isupper():
                # Check if it's a header (short, mostly caps, or has section reference)
                if len(next_line) < 60 and (next_line.isupper() or '(' in next_line[:20]):
                    # Could be a header, but check if it's just a continuation
                    # If current line ends mid-sentence, likely continuation
                    if not current_line.rstrip().endswith((',', 'and', 'or', 'with', 'of', 'to', 'in', 'for')):
                        break
            
            # Join the lines with a space
            current_line = current_line.rstrip() + ' ' + next_line
            i += 1
        
        cleaned_lines.append(current_line)
        i += 1
    
    return '\n'.join(cleaned_lines)


def extract_dashed_section(pdf_path, start_marker="INDICATIONS AND USAGE"):
    """
    Extract text between first encounter of a dashed section marker 
    and the next dashed line (---------------------------).
    
    Args:
        pdf_path: Path to the PDF file
        start_marker: The section name to find (default: "INDICATIONS AND USAGE")
    
    Returns:
        Extracted section text, or None if not found
    """
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Error opening PDF: {e}")
        return None
    
    # 1. Extract ALL text from PDF
    raw_text = "\n".join(page.get_text("text") for page in doc)

    # # save the raw text to a file with the PDF name
    # import os
    # pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
    # raw_text_filename = f"{pdf_basename}_raw_text.txt"
    # # Write with unicode escape sequences visible
    # with open(raw_text_filename, "w", encoding="ascii", errors="backslashreplace") as f:
    #     f.write(raw_text)
    
    doc.close()
    
    # 2. Normalize Unicode and cleanup
    text = unicodedata.normalize("NFKD", raw_text)
    text = text.replace("\r", "\n")
    text = text.replace("\u00ad", "")   # soft hyphen
    text = text.replace("•", "")        # bullets
    
    # Fix broken line hyphenation
    text = re.sub(r"-\n", "", text)
    
    # Collapse multiple newlines
    text = re.sub(r"\n{2,}", "\n", text)
    
    # 3. Find the first occurrence of the dashed section marker
    # Pattern: ---------------------------INDICATIONS AND USAGE----------------------------
    pattern = re.compile(
        rf"-{{3,}}\s*{re.escape(start_marker)}\s*-{{3,}}",
        flags=re.IGNORECASE
    )
    
    match = pattern.search(text)
    if not match:
        print(f"Section marker '{start_marker}' not found")
        return None
    
    # Start extracting from the end of the matched pattern
    start_pos = match.end()
    
    # 4. Find the next occurrence of dashed line (at least 3 dashes)
    # Look for lines that start with at least 3 dashes
    next_dash_pattern = re.compile(r"\n-{3,}")
    
    next_dash = next_dash_pattern.search(text, start_pos)
    if not next_dash:
        print("Next dashed line not found, extracting to end of document")
        section = text[start_pos:]
    else:
        end_pos = next_dash.start()
        section = text[start_pos:end_pos]
    
    # 5. Final cleanup
    section = section.strip(" \n\t:-–—")
    section = re.sub(r"\n{2,}", "\n", section)
    
    # Remove section number references like (1), (2.1), etc.
    section = re.sub(r"\(\d+(?:\.\d+)?\)", "", section)
    
    if not section:
        print("Extracted section is empty")
        return None
    
    return section


# ===== USAGE EXAMPLE =====
if __name__ == "__main__":
    # Test URLs for PDF downloads
    test_urls = [
        "https://www.accessdata.fda.gov/drugsatfda_docs/label/2021/125514s096lbl.pdf",
        "https://www.accessdata.fda.gov/drugsatfda_docs/label/2017/084427s039s041lbl.pdf"
    ]
    
    print("="*60)
    print("DOWNLOADING AND EXTRACTING INDICATIONS FROM PDFs")
    print("="*60 + "\n")
    
    for url in test_urls:
        # Download the PDF
        pdf_path = download_pdf(url)
        sys.stdout.flush()
        
        if pdf_path:
            # Extract INDICATIONS AND USAGE section
            indications = extract_dashed_section(pdf_path, "INDICATIONS AND USAGE")
            
            if indications:
                # Format the text for better readability
                formatted_indications = format_extracted_text(indications)
                
                print("\n" + "="*60)
                print(f"INDICATIONS AND USAGE - {os.path.basename(pdf_path)}")
                print("="*60 + "\n")
                print(formatted_indications)
                print("\n" + "="*60)
                print(f"Extracted {len(indications)} characters")
                print("="*60 + "\n")
                sys.stdout.flush()
            else:
                print(f"Failed to extract section from {os.path.basename(pdf_path)}\n")
                sys.stdout.flush()
        else:
            print(f"Failed to download PDF from {url}\n")
            sys.stdout.flush()
    
    # You can also extract other dashed sections:
    # dosage = extract_dashed_section(pdf_path, "DOSAGE AND ADMINISTRATION")
    # warnings = extract_dashed_section(pdf_path, "WARNINGS AND PRECAUTIONS")