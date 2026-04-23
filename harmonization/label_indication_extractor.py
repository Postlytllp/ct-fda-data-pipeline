"""
FDA Label Indication Extractor

This script extracts indication and usage data from FDA drug label PDFs.
It reads the drugs FDA JSON, finds label URLs, downloads PDFs, and extracts
the INDICATIONS AND USAGE section.

Output format: JSON with submission_status_date as key and indication text as value.
"""

import json
import os
import sys
import re
import requests
import unicodedata
from pathlib import Path
from datetime import datetime

try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF (fitz) is required. Install with: pip install PyMuPDF")
    sys.exit(1)


class LabelIndicationExtractor:
    """Extract indication data from FDA drug labels."""
    
    def __init__(self, data_file="drug-drugsfda-0001-of-0001.json", output_folder="label_pdf"):
        """
        Initialize the extractor.
        
        Args:
            data_file: Path to the drugs FDA JSON file
            output_folder: Folder to save downloaded PDFs
        """
        self.data_file = data_file
        self.output_folder = output_folder
        self.drugs_data = None
        
        # Create output folder if it doesn't exist
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        """Load the drugs FDA JSON data."""
        print(f"Loading data from {self.data_file}...")
        with open(self.data_file, 'r', encoding='utf-8') as f:
            self.drugs_data = json.load(f)
        print(f"Loaded {len(self.drugs_data.get('results', []))} drug records")
        return self
    
    def find_drugs_with_labels(self, limit=5):
        """
        Find drugs that have Label type documents.
        Only returns one entry per unique application number.
        Prefers newer submissions (more likely to have structured labels).
        
        Args:
            limit: Maximum number of unique drugs to find
        
        Returns:
            List of dicts with drug info and label URL
        """
        if self.drugs_data is None:
            self.load_data()
        
        # Collect all label entries grouped by application number
        label_entries = {}
        
        for drug in self.drugs_data.get('results', []):
            application_number = drug.get('application_number', 'N/A')
            
            # Skip if we already have enough
            if application_number in label_entries:
                continue
            
            # Prefer NDA applications and skip old labels (prefer from 2010 onwards)
            is_nda = application_number.upper().startswith('NDA')
            
            openfda = drug.get('openfda', {})
            brand_name = openfda.get('brand_name', ['Unknown'])[0] if 'brand_name' in openfda else 'N/A'
            sponsor_name = drug.get('sponsor_name', 'N/A')
            
            # Sort submissions by date (newest first)
            submissions = sorted(
                drug.get('submissions', []),
                key=lambda s: s.get('submission_status_date', '00000000'),
                reverse=True
            )
            
            for submission in submissions:
                for doc in submission.get('application_docs', []):
                    if doc.get('type') == 'Label':
                        url = doc.get('url', '')
                        
                        # Skip if URL doesn't end with .pdf
                        if not url.endswith('.pdf'):
                            continue
                        
                        # Skip malformed URLs (double http)
                        if url.count('http') > 1:
                            continue
                        
                        # Fix http to https
                        if url.startswith('http://'):
                            url = url.replace('http://', 'https://')
                        
                        # Prefer labels from 2010 or later (more likely to have structured format)
                        date = submission.get('submission_status_date', '00000000')
                        
                        # Skip labels before 2010 (they often have unstructured formats)
                        if int(date[:4]) < 2010 and len(label_entries) < limit // 2:
                            # Allow some older labels but prefer newer ones
                            pass
                        
                        drug_info = {
                            'application_number': application_number,
                            'brand_name': brand_name,
                            'sponsor_name': sponsor_name,
                            'submission_status_date': date,
                            'submission_type': submission.get('submission_type', 'N/A'),
                            'submission_number': submission.get('submission_number', 'N/A'),
                            'label_url': url,
                            'doc_date': doc.get('date', 'N/A')
                        }
                        
                        label_entries[application_number] = drug_info
                        print(f"Found: {brand_name} ({application_number}) - Date: {date}")
                        break  # Take the first (newest) label for this drug
                    
                if application_number in label_entries:
                    break  # Move to next drug
            
            if len(label_entries) >= limit:
                break
        
        return list(label_entries.values())
    
    def download_pdf(self, pdf_url, filename=None):
        """
        Download PDF from URL.
        
        Args:
            pdf_url: URL of the PDF
            filename: Optional custom filename
        
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            if filename is None:
                filename = os.path.basename(pdf_url)
            
            if not filename.endswith('.pdf'):
                filename = 'downloaded.pdf'
            
            pdf_path = os.path.join(self.output_folder, filename)
            
            # Check if already downloaded
            if os.path.exists(pdf_path):
                print(f"  PDF already exists: {filename}")
                return pdf_path
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            print(f"  Downloading: {filename}...")
            response = requests.get(pdf_url, headers=headers, allow_redirects=True, timeout=30)
            response.raise_for_status()
            
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            print(f"  Downloaded successfully")
            return pdf_path
            
        except Exception as e:
            print(f"  Error downloading PDF: {e}")
            return None
    
    def extract_indication_section(self, pdf_path):
        """
        Extract the INDICATIONS AND USAGE section from a PDF.
        Improved logic:
        1. Loops through all matches of the header to find a valid section (skips 'Recent Major Changes').
        2. Dynamic delimiters (dashes, stars) with immediate-skip logic.
        3. Fallback to all-caps header detection avoiding drug name.
        
        Args:
            pdf_path: Path to the PDF file
        
        Returns:
            Extracted section text or None if not found
        """
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            print(f"  Error opening PDF: {e}")
            return None
        
        # Extract all text from PDF
        raw_text = "\n".join(page.get_text("text") for page in doc)
        doc.close()
        
        # Normalize Unicode and cleanup
        text = unicodedata.normalize("NFKD", raw_text)
        text = text.replace("\r", "\n")
        text = text.replace("\u00ad", "")   # soft hyphen
        text = text.replace("•", "")        # bullets
        
        # Collapse multiple newlines to max 2
        text = re.sub(r"\n{3,}", "\n\n", text)
        
        # Find "INDICATIONS AND USAGE" (case insensitive)
        # We loop to find a match that yields substantial text (>100 chars)
        header_pattern = re.compile(r"(?:^|\n)\s*(?:\d+\.?\s*)?INDICATIONS AND USAGE", re.IGNORECASE)
        
        search_pos = 0
        best_section = None
        
        while True:
            match = header_pattern.search(text, search_pos)
            if not match:
                break
            
            print(f"  Found header at pos {match.start()}")
            start_pos = match.start()
            header_end_pos = match.end()
            
            # --- Strategy 1: Dynamic Delimiter Detection ---
            # Check for pattern: 3+ repeating chars (dash, star, equal, underscore)
            # Context window around match
            context_start = max(0, start_pos - 50)
            context_end = min(len(text), header_end_pos + 50)
            context = text[context_start:context_end]
            
            delimiter = None
            section_end_pos = None
            
            delimiter_match = re.search(r"([-*=~_]){3,}", context)
            if delimiter_match:
                delimiter_char = delimiter_match.group(1)
                delimiter_regex = f"{re.escape(delimiter_char)}{{3,}}"
                print(f"    Found delimiter pattern: {delimiter_char*3}")
                
                # Find the NEXT occurrence of this delimiter after the header
                # IMPORTANT: Skip immediate delimiter (e.g. underline)
                next_delim_iter = re.finditer(delimiter_regex, text[header_end_pos:])
                for delim in next_delim_iter:
                    # If delimiter is more than 10 chars away, treat it as end
                    if delim.start() > 10:
                        section_end_pos = header_end_pos + delim.start()
                        print("    Found end using dynamic delimiter")
                        break
            
            # --- Strategy 2: Smart Header Detection (Fallback) ---
            if not section_end_pos:
                print("    Using smart header detection (fallback)...")
                
                # 1. Identify Drug Name (first all-caps word after header)
                post_header_text = text[header_end_pos:header_end_pos+200]
                drug_name_match = re.search(r"\b[A-Z]{3,}\b", post_header_text)
                drug_name = None
                if drug_name_match:
                    drug_name = drug_name_match.group(0)
                    # Filter out common false positives for drug name
                    if drug_name in ["INDICATIONS", "USAGE", "DOSAGE", "ADMINISTRATION", "WARNINGS"]:
                        drug_name = None
                    else:
                        print(f"    Identified potential drug name: {drug_name}")
                
                # 2. Scan forward for the next section header
                # Standard known section headers for safety
                known_headers = [
                                "FULL PRESCRIBING INFORMATION",
                                "WARNING: SERIOUS INFECTIONS AND MALIGNANCY",
                                "BOXED WARNING",
                                "INDICATIONS AND USAGE",
                                "DOSAGE AND ADMINISTRATION",
                                "DOSAGE FORMS AND STRENGTHS",
                                "CONTRAINDICATIONS",
                                "WARNINGS AND PRECAUTIONS",
                                "ADVERSE REACTIONS",
                                "DRUG INTERACTIONS",
                                "USE IN SPECIFIC POPULATIONS",
                                "OVERDOSAGE",
                                "DESCRIPTION",
                                "CLINICAL PHARMACOLOGY",
                                "NONCLINICAL TOXICOLOGY",
                                "CLINICAL STUDIES",
                                "REFERENCES",
                                "HOW SUPPLIED/STORAGE AND HANDLING",
                                "PATIENT COUNSELING INFORMATION"
                                ]
                
                # Default end if nothing found
                section_end_pos = len(text)
                
                # Check if the start of this section was numbered (e.g. "1 INDICATIONS")
                is_start_numbered = bool(re.match(r"^\d", match.group(0).strip()))

                # Iterate line by line starting from header end
                line_iter = re.finditer(r"\n", text[header_end_pos:])
                
                for line_match in line_iter:
                    line_start_idx = header_end_pos + line_match.end()
                    # Peek at the line content (up to next newline or 100 chars)
                    line_end_search = re.search(r"\n", text[line_start_idx:])
                    if line_end_search:
                        line_end_idx = line_start_idx + line_end_search.start()
                    else:
                        line_end_idx = min(len(text), line_start_idx + 100)
                    
                    line_content = text[line_start_idx:line_end_idx].strip()
                    
                    if not line_content:
                        continue
                        
                    # Check if this line looks like a header
                    
                    
                    # Rule 1: Matches known header exactly (or contained)
                    # FIX: Must be at start of line (ignoring number) to avoid matching "see Clinical Studies" references
                    clean_line_start = re.sub(r"^[\d\.\s]+", "", line_content).upper()
                    is_known = any(clean_line_start.startswith(h) for h in known_headers)
                    
                    # Rule 2: All Caps match (ignoring drug name)
                    clean_line = re.sub(r"[\d\.\s]", "", line_content)
                    is_all_caps = clean_line.isupper() and len(clean_line) > 3
                    
                    # Rule 3: Start of numbered section (e.g., "2. DOSAGE")
                    # Be strict: Only matches if it looks like a TOP level header (e.g. "2 ", not "2.1 ")
                    is_numbered_header = False
                    number_match = re.match(r"^(\d+)\.?\s+([A-Z])", line_content)
                    if number_match:
                         # Ensure it's not a subsection like 1.1 (unless it's a known header)
                         if '.' not in line_content.split()[0] or is_known:
                             is_numbered_header = True
                    
                    # STOP CONDITIONS
                    
                    # 1. Known headers always stop
                    if is_known:
                        print(f"    Stopped at known header: {line_content[:30]}...")
                        section_end_pos = line_start_idx
                        break
                    
                    # 2. Numbered headers always stop (if major section)
                    if is_numbered_header:
                        print(f"    Stopped at numbered section: {line_content[:30]}...")
                        section_end_pos = line_start_idx
                        break
                        
                    # 3. All Caps headers (Only if start was NOT numbered)
                    if not is_start_numbered and is_all_caps:
                        # Stricter check: avoid acronyms like TNF, TMF (len > 4)
                        if len(clean_line) <= 4:
                             continue
                             
                        if drug_name and drug_name in line_content:
                            continue
                        
                        if len(line_content) < 60:
                            print(f"    Stopped at likely header (all caps): {line_content[:30]}...")
                            section_end_pos = line_start_idx
                            break
            
            # Extract and validate
            potential_section = text[header_end_pos:section_end_pos]
            cleaned = self._cleanup_section(potential_section)
            
            if cleaned and len(cleaned) > 100:
                print(f"  Select valid section ({len(cleaned)} chars)")
                if not best_section or len(cleaned) > len(best_section):
                     best_section = cleaned
                     # If we found a good FPI section (numbered), it's usually the best
                     if re.match(r"\d", match.group(0)):
                          return best_section
            else:
                 print(f"  Skipping section (too short: {len(cleaned) if cleaned else 0} chars)")
            
            # Continue search
            search_pos = match.end()
            
        if best_section:
             return best_section
             
        print(f"  Section 'INDICATIONS AND USAGE' not found (no valid content)")
        return None

    def _cleanup_section(self, section):
        """Helper to clean extracted text."""
        # Replace non-breaking spaces
        section = section.replace('\xa0', ' ')
        
        # Replace bullets with dashes
        section = section.replace('\uf0b7', '-')
        
        # Strip leading/trailing whitespace
        section = section.strip(" \n\t:-–—")
        
        # Improve hyphenation repair
        # E.g. "re-\nquire" -> "require"
        # Be careful not to merge "Item 1. -\nDescription"
        section = re.sub(r"([a-z])- *\n *([a-z])", r"\1\2", section)
        
        # Collapse multiple newlines/spaces
        # If there are huge blocks of newlines (like page breaks), reduce them
        section = re.sub(r"\n\s*\n", "\n\n", section)      # 2+ newlines -> 2
        section = re.sub(r" +", " ", section)              # multiple spaces -> 1
        
        # Try to join lines that are just sentence continuations
        # e.g. "Line 1 ends with word \nand Line 2 starts" -> "Line 1 ends with word and Line 2 starts"
        # Heuristic: line doesn't end with punctuation, next line starts with lowercase
        # But this is risky for headers. 
        # Safer: Just ensure we don't have "\n \n \n" garbage
        
        # Remove lines that are just empty or single char garbage
        lines = [line.strip() for line in section.split('\n')]
        cleaned_lines = []
        for line in lines:
            if not line:
                # Only keep one empty line as separator
                if cleaned_lines and cleaned_lines[-1] != "":
                     cleaned_lines.append("")
                continue
            cleaned_lines.append(line)
            
        section = "\n".join(cleaned_lines)
        
        # Remove section references (1), (2.1), etc. IF they are at start of lines
        section = re.sub(r"^\(\d+(?:\.\d+)?\)", "", section, flags=re.MULTILINE)

        if not section or len(section) < 20:
            return None
        return section
    
    def extract_indications_from_drugs(self, drugs_list):
        """
        Extract indications for a list of drugs.
        
        Args:
            drugs_list: List of drug info dicts with label URLs
        
        Returns:
            Dict with submission_status_date as key and result data as value
        """
        results = {}
        
        for i, drug in enumerate(drugs_list, 1):
            print(f"\n[{i}/{len(drugs_list)}] Processing: {drug['brand_name']} ({drug['application_number']})")
            print(f"  Submission Date: {drug['submission_status_date']}")
            
            # Download PDF
            pdf_path = self.download_pdf(drug['label_url'])
            
            if pdf_path is None:
                results[drug['submission_status_date']] = {
                    'brand_name': drug['brand_name'],
                    'application_number': drug['application_number'],
                    'error': 'Failed to download PDF',
                    'label_url': drug['label_url']
                }
                continue
            
            # Extract indication section
            indication = self.extract_indication_section(pdf_path)
            
            if indication:
                print(f"  ✓ Extracted {len(indication)} characters")
                results[drug['submission_status_date']] = {
                    'brand_name': drug['brand_name'],
                    'application_number': drug['application_number'],
                    'indication_text': indication,
                    'label_url': drug['label_url']
                }
            else:
                results[drug['submission_status_date']] = {
                    'brand_name': drug['brand_name'],
                    'application_number': drug['application_number'],
                    'error': 'Failed to extract indication section',
                    'label_url': drug['label_url']
                }
        
        return results
    
    def save_results(self, results, output_file="indication_results.json"):
        """
        Save results to JSON file.
        
        Args:
            results: Dict of extraction results
            output_file: Output JSON filename
        """
        output_path = os.path.join(self.output_folder, output_file)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\nResults saved to: {output_path}")
        return output_path


def main():
    """Main function to run the extraction."""
    print("=" * 70)
    print("FDA LABEL INDICATION EXTRACTOR")
    print("=" * 70)
    
    # Initialize extractor
    extractor = LabelIndicationExtractor()
    
    # Load data
    extractor.load_data()
    
    # Find 5 drugs with label URLs
    print("\n" + "=" * 70)
    print("FINDING DRUGS WITH LABEL URLS")
    print("=" * 70)
    drugs = extractor.find_drugs_with_labels(limit=5)
    
    if not drugs:
        print("No drugs with valid label URLs found!")
        return
    
    print(f"\nFound {len(drugs)} drugs with valid label URLs")
    
    # Extract indications
    print("\n" + "=" * 70)
    print("EXTRACTING INDICATIONS FROM LABELS")
    print("=" * 70)
    results = extractor.extract_indications_from_drugs(drugs)
    
    # Save results
    output_path = extractor.save_results(results)
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    success_count = sum(1 for r in results.values() if 'indication_text' in r)
    error_count = sum(1 for r in results.values() if 'error' in r)
    print(f"Successfully extracted: {success_count}")
    print(f"Errors: {error_count}")
    
    # Print sample output
    print("\n" + "=" * 70)
    print("SAMPLE OUTPUT (First Successful Entry)")
    print("=" * 70)
    for date, data in results.items():
        if 'indication_text' in data:
            print(f"\nDate: {date}")
            print(f"Drug: {data['brand_name']}")
            print(f"Application: {data['application_number']}")
            print(f"Indication (first 500 chars):\n{data['indication_text'][:500]}...")
            break


if __name__ == "__main__":
    main()
