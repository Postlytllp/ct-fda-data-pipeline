import json
import re
import csv
from typing import Dict, List, Set, Optional, Tuple
from difflib import SequenceMatcher
from collections import defaultdict
from datetime import datetime
from datetime import datetime
from dateutil import parser
from label_indication_extractor import LabelIndicationExtractor

class DrugHarmonizer:
    """
    Comprehensive drug harmonization across FDA data sources:
    - Drugs@FDA (drug-drugsfda-0001-of-0001.json)
    - NDC Directory (drug-ndc-0001-of-0001.json)
    - Orange Book (products.txt, patent.txt, exclusivity.txt)
    - Purple Book (purplebook-search-january-data-download.csv)
    - RxNorm (RXNCONSO.RRF) - for synonyms
    - Drug Label (drug-label-mini.json) - for additional information
    """
    
    def __init__(self, drugsfda_path: str, ndc_path: str, 
                 products_path: str, patent_path: str, exclusivity_path: str,
                 purplebook_path: str, rxnconso_path: str, drug_label_path: str = None):
        """Initialize with paths to FDA data files."""
        print("Loading Drugs@FDA data...")
        self.drugsfda_data = self._load_json(drugsfda_path)
        
        print("Loading NDC Directory data...")
        self.ndc_data = self._load_json(ndc_path)
        
        print("Loading Orange Book products...")
        self.products_data = self._load_orange_book_txt(products_path)
        
        print("Loading Orange Book patents...")
        self.patent_data = self._load_orange_book_txt(patent_path)
        
        print("Loading Orange Book exclusivity...")
        self.exclusivity_data = self._load_orange_book_txt(exclusivity_path)
        
        print("Loading Purple Book data...")
        self.purplebook_data = self._load_purplebook_csv(purplebook_path)
        
        print("Loading RxNorm data...")
        self.rxn_rxcuis_by_name, self.rxn_names_by_rxcui = self._load_rxnconso(rxnconso_path)
        
        if drug_label_path:
            print("Loading Drug Label data...")
            label_data = self._load_json(drug_label_path)
            # Handle both list and dict formats
            if isinstance(label_data, dict) and 'results' in label_data:
                self.drug_label_data = label_data['results']
            elif isinstance(label_data, list):
                self.drug_label_data = label_data
            else:
                self.drug_label_data = []
        else:
            self.drug_label_data = []
        
        print("Building search indices...")
        self._build_indices()
        
        print("Initializing Label Extraction...")
        self.indication_extractor = LabelIndicationExtractor(output_folder="label_pdf")
        
        print("Initialization complete!")
    
    def _load_json(self, filepath: str) -> List[Dict]:
        """Load JSON file from FDA downloads."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('results', [])
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
            return []
    
    def _load_orange_book_txt(self, filepath: str) -> List[Dict]:
        """Load Orange Book tilde-delimited text files."""
        data = []
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                if not lines:
                    return data
                
                # First line is header
                headers = [h.strip() for h in lines[0].split('~')]
                
                # Parse data lines
                for line in lines[1:]:
                    if line.strip():
                        values = [v.strip() for v in line.split('~')]
                        while len(values) < len(headers):
                            values.append('')
                        data.append(dict(zip(headers, values)))
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
        
        return data
    
    def _load_purplebook_csv(self, filepath: str) -> List[Dict]:
        """Load Purple Book CSV file."""
        data = []
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                # Skip header rows (first 3 lines are descriptive)
                for _ in range(3):
                    f.readline()
                
                # Read CSV data
                reader = csv.DictReader(f)
                for row in reader:
                    # Clean up row data
                    cleaned_row = {k.strip(): v.strip() if v else '' for k, v in row.items()}
                    if cleaned_row:
                        data.append(cleaned_row)
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
        
        return data

    def _load_rxnconso(self, filepath: str) -> Tuple[Dict[str, List[str]], Dict[str, Set[str]]]:
        """
        Load RXNCONSO.RRF file to build synonym indices.
        Returns:
            - rxcuis_by_name: normalized name -> list of RXCUIs
            - names_by_rxcui: RXCUI -> set of original names
        """
        rxcuis_by_name = defaultdict(list)
        names_by_rxcui = defaultdict(set)
        
        try:
            # RRF files are pipe-delimited
            # Col 0: RXCUI (Concept ID)
            # Col 14: STR (String/Name)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    values = line.split('|')
                    if len(values) >= 15:
                        rxcui = values[0]
                        name = values[14]
                        
                        # Build indices
                        if name and rxcui:
                            normalized = self._normalize_name(name)
                            rxcuis_by_name[normalized].append(rxcui)
                            names_by_rxcui[rxcui].add(name)
                            
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
            
        return rxcuis_by_name, names_by_rxcui

    
    def _build_indices(self):
        """Build lookup indices for faster searching."""
        # Drugs@FDA indices
        self.drugsfda_by_generic = defaultdict(list)
        self.drugsfda_by_brand = defaultdict(list)
        self.drugsfda_by_application = defaultdict(list)
        
        for drug in self.drugsfda_data:
            openfda = drug.get('openfda', {})
            
            # Index by generic names
            for name in openfda.get('generic_name', []):
                self.drugsfda_by_generic[name.lower()].append(drug)
            
            # Index by brand names
            for name in openfda.get('brand_name', []):
                self.drugsfda_by_brand[name.lower()].append(drug)
            
            # Index by application number
            app_no = drug.get('application_number')
            if app_no:
                self.drugsfda_by_application[app_no].append(drug)
        
        # NDC indices
        self.ndc_by_name = defaultdict(list)
        self.ndc_by_ndc = defaultdict(list)
        self.ndc_by_labeler = defaultdict(list)
        
        for ndc_entry in self.ndc_data:
            openfda = ndc_entry.get('openfda', {})
            
            # Index by names
            names = (openfda.get('brand_name', []) + 
                    openfda.get('generic_name', []) + 
                    [ndc_entry.get('brand_name', ''), 
                     ndc_entry.get('generic_name', '')])
            
            for name in names:
                if name:
                    self.ndc_by_name[name.lower()].append(ndc_entry)
            
            # Index by NDC codes
            product_ndc = ndc_entry.get('product_ndc')
            if product_ndc:
                self.ndc_by_ndc[product_ndc].append(ndc_entry)
            
            # Index by labeler
            labeler = ndc_entry.get('labeler_name')
            if labeler:
                self.ndc_by_labeler[labeler.lower()].append(ndc_entry)
        
        # Orange Book indices
        self.orangebook_by_ingredient = defaultdict(list)
        self.orangebook_by_trade = defaultdict(list)
        self.orangebook_by_appl = defaultdict(list)
        
        for product in self.products_data:
            ingredient = product.get('Ingredient', '').lower()
            trade_name = product.get('Trade_Name', '').lower()
            appl_no = product.get('Appl_No')
            
            if ingredient:
                self.orangebook_by_ingredient[ingredient].append(product)
            if trade_name:
                self.orangebook_by_trade[trade_name].append(product)
            if appl_no:
                self.orangebook_by_appl[appl_no].append(product)
        
        # Purple Book indices
        self.purplebook_by_proper_name = defaultdict(list)
        self.purplebook_by_proprietary_name = defaultdict(list)
        self.purplebook_by_bla = defaultdict(list)
        self.purplebook_by_applicant = defaultdict(list)
        
        for biologic in self.purplebook_data:
            # Index by proper name (generic)
            proper_name = biologic.get('Proper Name', '').lower()
            if proper_name:
                self.purplebook_by_proper_name[proper_name].append(biologic)
            
            # Index by proprietary name (brand) - can be multiple names separated by commas
            proprietary_name = biologic.get('Proprietary Name', '')
            if proprietary_name:
                # Handle multiple names (e.g., "prolastin,prolastin-c,prolastin-c liquid")
                for name in proprietary_name.split(','):
                    name = name.strip().lower()
                    if name:
                        self.purplebook_by_proprietary_name[name].append(biologic)
            
            # Index by BLA Number
            bla_number = biologic.get('BLA Number')
            if bla_number:
                self.purplebook_by_bla[bla_number].append(biologic)
            
            # Index by applicant
            applicant = biologic.get('Applicant', '').lower()
            if applicant:
                self.purplebook_by_applicant[applicant].append(biologic)
    
    def _normalize_name(self, name: str) -> str:
        """Normalize drug name for better matching."""
        name = name.lower().strip()
        # Remove common suffixes
        name = re.sub(r'\s+(tablet|capsule|injection|solution|oral|topical|cream|ointment)s?$', '', name)
        # Remove dosage information
        name = re.sub(r'\s+\d+\s*(mg|mcg|g|ml|%)', '', name)
        return name
    
    # Fuzzy matching removed - using exact match only
    # def _calculate_similarity(self, str1: str, str2: str) -> float:
    #     """Calculate similarity between two strings."""
    #     return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    # 
    # def _find_best_match(self, query: str, candidates: List[str], 
    #                     threshold: float = 0.75) -> Optional[str]:
    #     """Find best matching string from candidates."""
    #     query_norm = self._normalize_name(query)
    #     best_match = None
    #     best_score = 0.0
    #     
    #     for candidate in candidates:
    #         if not candidate:
    #             continue
    #         candidate_norm = self._normalize_name(candidate)
    #         score = self._calculate_similarity(query_norm, candidate_norm)
    #         
    #         if score > best_score and score >= threshold:
    #             best_score = score
    #             best_match = candidate
    #     
    #     return best_match
    
    def harmonize_drug(self, drug_name: str) -> Dict:
        """
        Harmonize a drug name across all FDA databases.
        
        Returns a comprehensive dictionary with all available drug information.
        """
        result = {
            "query_name": drug_name,
            "harmonized_drug_name": None,
            "harmonized_generic_name": None,
            "harmonized_brand_name": None,
            
            # Basic drug information
            "strength": [],
            "route": [],
            "dosage_form": [],
            "dose": [],
            "administration_details": [],
            
            # Names and identifiers
            "brand_names": [],
            "generic_names": [],
            "synonyms": [],
            "substance_names": [],
            
            # Drug codes and identifiers
            "atc_codes": [],
            "ndc_codes": [],
            "ndc_package_codes": [],
            "unii_codes": [],
            "cas_numbers": [],
            "rxcui_codes": [],
            "spl_set_ids": [],
            "application_numbers": [],
            "bla_numbers": [],
            
            # Linked Application History
            "applications": [], # List of dicts: {app_no, type, events: []}
            
            # Chemical and molecular information
            "molecular_formulas": [],
            "active_ingredients": [],
            
            # Manufacturer and labeler information
            "manufacturers": [],
            "labelers": [],
            "applicants": [],
            
            # Regulatory information
            "first_approval_date": None,
            "marketing_status": [],
            "application_type": [],
            "dea_schedule": [],
            "rx_otc": [],
            
            # Patent information
            "patents": [],
            
            # Exclusivity information
            "exclusivities": [],
            
            # Orange Book specific
            "reference_listed_drug": [],
            "reference_standard": [],
            "therapeutic_equivalence": [],
            
            # Purple Book specific
            "bla_type": [],
            "licensure_status": [],
            "license_number": [],
            "center": [],
            "date_of_first_licensure": [],
            "interchangeability": [],
            "reference_product": [],
            
            # NDC specific
            "packaging_info": [],
            "product_type": [],
            "finished_status": [],
            "listing_expiration_dates": [],
            
            # Pharmacological information
            "pharmacological_class": [],
            "mechanism_of_action": [],
            "physiologic_effect": [],
            "chemical_structure": [],
            
            # Drug Label data
            "indications_and_usage": [],
            "mechanism_of_action": [],
            "pharm_class_moa": [],
            "fda_pivotal_trials": [],  # NCT IDs extracted from clinical_studies
            
            # Drugs@FDA specific
            "sponsor_name": [],
            "indication_approval_history": [],  # Extracted from label PDFs in application history
            
            # Metadata
            "sources_used": [],
            "confidence_score": 0.0,
            "last_updated": datetime.now().strftime("%Y-%m-%d")
        }
        
        sources_used = set()
        
        # Search in Drugs@FDA
        drugsfda_matches = self._search_drugsfda(drug_name)
        if drugsfda_matches:
            sources_used.add("Drugs@FDA")
            self._extract_drugsfda_data(drugsfda_matches, result)
        
        # Search in NDC Directory
        ndc_matches = self._search_ndc(drug_name)
        if ndc_matches:
            sources_used.add("NDC Directory")
            self._extract_ndc_data(ndc_matches, result)
        
        # Search in Orange Book
        orangebook_matches = self._search_orangebook(drug_name)
        if orangebook_matches:
            sources_used.add("Orange Book")
            self._extract_orangebook_data(orangebook_matches, result)
        
        # Search in Purple Book
        purplebook_matches = self._search_purplebook(drug_name)
        if purplebook_matches:
            sources_used.add("Purple Book")
            self._extract_purplebook_data(purplebook_matches, result)

        # Search in RxNorm (Synonyms)
        rxnorm_synonyms = self._search_rxnconso(drug_name)
        if rxnorm_synonyms:
            sources_used.add("RxNorm")
            result['synonyms'].extend(rxnorm_synonyms)
        
        # Search in Drug Labels
        # Pass collected application numbers for fallback matching
        all_app_numbers = result.get('application_numbers', []) + result.get('bla_numbers', [])
        label_matches = self._search_drug_labels(drug_name, all_app_numbers)
        
        if label_matches:
            sources_used.add("Drug Labels")
            self._extract_drug_label_data(label_matches, result)
            
        # Cross-reference by application number if available
        if result['application_numbers'] or result['bla_numbers']:
            self._cross_reference_by_application(result)
        
        # Calculate confidence score
        result['confidence_score'] = self._calculate_confidence(result, sources_used)
        result['sources_used'] = sorted(list(sources_used))
        
        # Harmonize names
        self._harmonize_names(result)
        
        # Standardize dates
        self._standardize_dates(result)
        
        # Remove duplicates
        self._deduplicate_lists(result)
        
        # Extract indications from label PDFs (new step)
        self._extract_indications_from_history(result)
        
        return result

    
    def _parse_date_str(self, date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format."""
        if not date_str:
            return None
        try:
            dt = parser.parse(str(date_str))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def _standardize_dates(self, result: Dict):
        """Parse, sort, and standardize approval dates from application events."""
        # Extract all dates from application events
        all_dates = []
        for app in result.get('applications', []):
            for event in app.get('events', []):
                if event.get('date'):
                    all_dates.append(event['date'])
        
        if all_dates:
            # Sort and set first_approval_date (oldest)
            all_dates.sort()
            result['first_approval_date'] = all_dates[0]
        else:
            result['first_approval_date'] = None
            
        # Sort application events
        self._sort_application_events(result)

    def _add_application_event(self, result: Dict, app_no: str, app_type: str, 
                             date: str, event_type: str, status: str = None, 
                             description: str = None, source: str = None):
        """Add an event to the application history."""
        if not app_no:
            return
            
        # Normalize event data
        parsed_date = self._parse_date_str(date)
        if not parsed_date:
            return # Skip events without valid dates
            
        # Find or create application entry
        app_entry = None
        for entry in result['applications']:
            if entry['application_number'] == app_no:
                app_entry = entry
                break
        
        if not app_entry:
            app_entry = {
                'application_number': app_no,
                'application_type': app_type,
                'events': []
            }
            result['applications'].append(app_entry)
            
        # Add event if not duplicate
        event = {
            'date': parsed_date,
            'submission_type': event_type,
            'status': status,
            'description': description,
            'source': source
        }
        
        # Simple deduplication based on date and type
        is_duplicate = False
        for existing in app_entry['events']:
            if (existing['date'] == event['date'] and 
                existing['submission_type'] == event['submission_type']):
                is_duplicate = True
                break
        
        if not is_duplicate:
            app_entry['events'].append(event)
            
    def _sort_application_events(self, result: Dict):
        """Sort events for each application by date (descending)."""
        for app in result['applications']:
            app['events'].sort(key=lambda x: x['date'], reverse=True)
    
    def _search_purplebook(self, drug_name: str) -> List[Dict]:
        """Search in Purple Book database - exact match only."""
        matches = []
        normalized = self._normalize_name(drug_name)
        
        # Direct lookup by proper name (generic)
        matches.extend(self.purplebook_by_proper_name.get(normalized, []))
        
        # Direct lookup by proprietary name (brand)
        matches.extend(self.purplebook_by_proprietary_name.get(normalized, []))
        
        # Remove duplicates
        seen = set()
        unique_matches = []
        for match in matches:
            match_key = (match.get('BLA Number'), match.get('Product Number'))
            if match_key not in seen:
                seen.add(match_key)
                unique_matches.append(match)
        
        return unique_matches
    
    def _search_rxnconso(self, drug_name: str) -> List[str]:
        """Search in RxNorm data for synonyms - exact match on name."""
        normalized = self._normalize_name(drug_name)
        
        # 1. Find RXCUIs for this name
        rxcuis = self.rxn_rxcuis_by_name.get(normalized, [])
        
        # 2. Collect all names associated with these RXCUIs
        synonyms = set()
        for rxcui in rxcuis:
            related_names = self.rxn_names_by_rxcui.get(rxcui, set())
            synonyms.update(related_names)
            
        # Remove the query name itself from synonyms if present (optional, but cleaner)
        # Note: We keep original casing from the file in the returned list
        
        return sorted(list(synonyms))
    
    def _search_drug_labels(self, drug_name: str, app_numbers: List[str] = None) -> List[Dict]:
        """Search in Drug Label data - exact match on generic/brand names or application number."""
        matches = []
        normalized = self._normalize_name(drug_name)
        app_set = set(app_numbers) if app_numbers else set()
        
        for label in self.drug_label_data:
            openfda = label.get('openfda', {})
            
            # Strategy: If OpenFDA data is available, rely on it (Brand, Generic, App Number).
            # Only use SPL text search if OpenFDA data is missing or sparse.
            
            has_openfda_names = openfda.get('generic_name') or openfda.get('brand_name')
            
            if has_openfda_names:
                # 1. Check Generic Name (Relaxed match for salts/variants)
                found_generic = False
                for name in openfda.get('generic_name', []):
                    norm_name = self._normalize_name(name)
                    # Use substring matching to handle "Treprostinil" vs "Treprostinil Sodium"
                    if norm_name and (norm_name in normalized or normalized in norm_name):
                        matches.append(label)
                        found_generic = True
                        break
                if found_generic: continue

                # 2. Check Brand Name
                found_brand = False
                for brand in openfda.get('brand_name', []):
                    norm_brand = self._normalize_name(brand)
                    if norm_brand and (norm_brand in normalized or normalized in norm_brand):
                        matches.append(label)
                        found_brand = True
                        break
                if found_brand: continue
                
                # 3. Check Application Number (if specific app numbers provided)
                found_app = False
                if app_set:
                    label_apps = openfda.get('application_number', [])
                    for app_no in label_apps:
                        if app_no in app_set:
                            matches.append(label)
                            found_app = True
                            break
                        norm_label = app_no.replace(" ", "").upper()
                        if any(norm_label == a.replace(" ", "").upper() for a in app_set):
                            matches.append(label)
                            found_app = True
                            break
                if found_app: continue
                
                # If OpenFDA data exists but NO match found, we skip SPL search 
                # to avoid false positives (like Lyumjev matching via SPL text).
                continue
            
            # # --- Fallback to SPL Text Search if OpenFDA names are missing ---
            # found_spl = False
            # for elem in label.get('spl_product_data_elements', []):
            #     # Use strict word boundary check
            #     pattern = r'\b' + re.escape(normalized) + r'\b'
            #     if re.search(pattern, self._normalize_name(elem)):
            #         matches.append(label)
            #         found_spl = True
            #         break
            # if found_spl: continue
            
            # Application number check for non-OpenFDA labels (using spl_unclassified etc if needed? 
            # Or just skip if no OpenFDA app number?)
            # Usually app number is in OpenFDA or not structured. 
            # We'll leave it at that.

        
        return matches
    
    def _extract_drug_label_data(self, matches: List[Dict], result: Dict):
        """Extract data from Drug Label matches including NCT IDs."""
        for label in matches:
            openfda = label.get('openfda', {})
            
            # Extract basic info from OpenFDA if available
            if openfda:
                # Brand Names
                for brand in openfda.get('brand_name', []):
                    result['brand_names'].append(brand)
                    
                # Generic Names
                for generic in openfda.get('generic_name', []):
                    result['generic_names'].append(generic)
                    
                # Manufacturer / Sponsor
                for manuf in openfda.get('manufacturer_name', []):
                    result['applicants'].append(manuf)
                    
                # Route
                for route in openfda.get('route', []):
                    result['route'].append(route)
                    
                # Active Components (Substance Name)
                for substance in openfda.get('substance_name', []):
                    result['active_ingredients'].append(substance)
                    
                # Product NDCS (add to packaging info or separate?)
                # Adding to packaging info for visibility
                for ndc in openfda.get('product_ndc', []):
                    result['packaging_info'].append(f"NDC: {ndc}")

            # Indications and Usage
            for ind in label.get('indications_and_usage', []):
                result['indications_and_usage'].append(ind)
            
            # Mechanism of Action
            for moa in label.get('mechanism_of_action', []):
                result['mechanism_of_action'].append(moa)
            
            # Pharmacological class MOA from openfda
            result['pharm_class_moa'].extend(openfda.get('pharm_class_moa', []))
            
            # Extract NCT IDs from clinical_studies using regex
            for study in label.get('clinical_studies', []):
                nct_ids = re.findall(r'NCT\d{8}', study)  # Exactly 8 digits
                result['fda_pivotal_trials'].extend(nct_ids)
                # Remove duplicates immediately to keep list clean per label
                result['fda_pivotal_trials'] = list(set(result['fda_pivotal_trials']))

    
    def _extract_purplebook_data(self, matches: List[Dict], result: Dict):
        """Extract comprehensive data from Purple Book matches."""
        for biologic in matches:
            # BLA Number
            bla_number = biologic.get('BLA Number')
            if bla_number:
                result['bla_numbers'].append(bla_number)
            
            # Proper Name (generic)
            proper_name = biologic.get('Proper Name')
            if proper_name:
                result['generic_names'].append(proper_name)
            
            # Proprietary Name (brand) - can be multiple names
            proprietary_name = biologic.get('Proprietary Name')
            if proprietary_name:
                for name in proprietary_name.split(','):
                    name = name.strip()
                    if name:
                        result['brand_names'].append(name)
            
            # Applicant
            applicant = biologic.get('Applicant')
            if applicant:
                result['applicants'].append(applicant)
            
            # BLA Type
            bla_type = biologic.get('BLA Type')
            if bla_type:
                result['bla_type'].append(bla_type)
                # Also add to application_type for consistency
                result['application_type'].append(bla_type)
            
            # Strength
            strength = biologic.get('Strength')
            if strength:
                result['strength'].append(strength)
            
            # Dosage Form
            dosage_form = biologic.get('Dosage Form')
            if dosage_form:
                result['dosage_form'].append(dosage_form)
            
            # Route of Administration
            route = biologic.get('Route of Administration')
            if route:
                result['route'].append(route)
            
            # Product Presentation (packaging)
            product_presentation = biologic.get('Product Presentation')
            if product_presentation:
                result['packaging_info'].append(product_presentation)
            
            # Marketing Status
            marketing_status = biologic.get('Marketing Status')
            if marketing_status:
                result['marketing_status'].append(marketing_status)
            
            # Licensure Status
            licensure = biologic.get('Licensure')
            if licensure:
                result['licensure_status'].append(licensure)
            
            # Approval Date
            approval_date = biologic.get('Approval Date')
            first_licensure = biologic.get('Date of First Licensure')
            
            # Store dates for first_approval_date calculation (no events created)
            
            # Reference Product Information
            ref_product_proper = biologic.get('Ref. Product Proper Name')
            ref_product_proprietary = biologic.get('Ref. Product Proprietary Name')
            if ref_product_proper or ref_product_proprietary:
                ref_info = []
                if ref_product_proper:
                    ref_info.append(f"Proper: {ref_product_proper}")
                if ref_product_proprietary:
                    ref_info.append(f"Proprietary: {ref_product_proprietary}")
                result['reference_product'].append(", ".join(ref_info))
            
            # Supplement Number and Submission Type are now only used for event creation(if we were creating them)
            # or for informational purposes, but we don't store them in flat lists anymore.
            # supplement_number = biologic.get('Supplement Number')
            # submission_type = biologic.get('Submission Type')
            
            # License Number
            license_number = biologic.get('License Number')
            if license_number:
                result['license_number'].append(license_number)
            
            # Product Number
            product_number = biologic.get('Product Number')
            
            # Center (CBER/CDER)
            center = biologic.get('Center')
            if center:
                result['center'].append(center)
            
            # Date of First Licensure
            first_licensure = biologic.get('Date of First Licensure')
            if first_licensure:
                result['date_of_first_licensure'].append(first_licensure)
            
            # Exclusivity Dates
            exclusivity_exp = biologic.get('Exclusivity Expiration Date')
            first_interchangeable_exp = biologic.get('First Interchangeable Exclusivity Exp. Date')
            ref_product_excl_exp = biologic.get('Ref. Product Exclusivity Exp. Date')
            orphan_excl_exp = biologic.get('Orphan Exclusivity Exp. Date')
            
            if exclusivity_exp:
                result['exclusivities'].append({
                    'type': 'General Exclusivity',
                    'expiration_date': exclusivity_exp
                })
            
            if first_interchangeable_exp:
                result['exclusivities'].append({
                    'type': 'First Interchangeable Exclusivity',
                    'expiration_date': first_interchangeable_exp
                })
                result['interchangeability'].append('First Interchangeable')
            
            if ref_product_excl_exp:
                result['exclusivities'].append({
                    'type': 'Reference Product Exclusivity',
                    'expiration_date': ref_product_excl_exp
                })
            
            if orphan_excl_exp:
                result['exclusivities'].append({
                    'type': 'Orphan Exclusivity',
                    'expiration_date': orphan_excl_exp
                })
            
            # Check if it's an interchangeable biosimilar
            if '351(k) Interchangeable' in bla_type:
                result['interchangeability'].append('Interchangeable')
            elif '351(k) Biosimilar' in bla_type:
                result['interchangeability'].append('Biosimilar (Not Interchangeable)')
    
    def _search_drugsfda(self, drug_name: str) -> List[Dict]:
        """Search in Drugs@FDA database - exact match only."""
        matches = []
        normalized = self._normalize_name(drug_name)
        
        # Direct lookup
        matches.extend(self.drugsfda_by_generic.get(normalized, []))
        matches.extend(self.drugsfda_by_brand.get(normalized, []))
        
        # Remove duplicates
        seen = set()
        unique_matches = []
        for match in matches:
            match_id = id(match)
            if match_id not in seen:
                seen.add(match_id)
                unique_matches.append(match)
        
        return unique_matches
    
    def _search_ndc(self, drug_name: str) -> List[Dict]:
        """Search in NDC Directory - exact match only."""
        matches = []
        normalized = self._normalize_name(drug_name)
        
        # Direct lookup
        matches.extend(self.ndc_by_name.get(normalized, []))
        
        # Remove duplicates
        seen = set()
        unique_matches = []
        for match in matches:
            ndc = match.get('product_ndc')
            if ndc and ndc not in seen:
                seen.add(ndc)
                unique_matches.append(match)
        
        return unique_matches
    
    def _search_orangebook(self, drug_name: str) -> List[Dict]:
        """Search in Orange Book - exact match only."""
        matches = []
        normalized = self._normalize_name(drug_name)
        
        # Direct lookup
        matches.extend(self.orangebook_by_ingredient.get(normalized, []))
        matches.extend(self.orangebook_by_trade.get(normalized, []))
        
        # Remove duplicates based on application number + product number
        seen = set()
        unique_matches = []
        for match in matches:
            key = (match.get('Appl_No'), match.get('Product_No'))
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)
        
        return unique_matches
    
    def _extract_drugsfda_data(self, matches: List[Dict], result: Dict):
        """Extract comprehensive data from Drugs@FDA matches."""
        for match in matches:
            openfda = match.get('openfda', {})
            products = match.get('products', [])
            submissions = match.get('submissions', [])
            
            # Application information
            app_no = match.get('application_number')
            if app_no:
                result['application_numbers'].append(app_no)
            
            # Sponsor/Manufacturer
            sponsor = match.get('sponsor_name')
            if sponsor:
                result['sponsor_name'].append(sponsor)
            
            # Names
            result['generic_names'].extend(openfda.get('generic_name', []))
            result['brand_names'].extend(openfda.get('brand_name', []))
            result['substance_names'].extend(openfda.get('substance_name', []))
            
            # Identifiers
            result['unii_codes'].extend(openfda.get('unii', []))
            result['rxcui_codes'].extend(openfda.get('rxcui', []))
            result['spl_set_ids'].extend(openfda.get('spl_set_id', []))
            result['ndc_codes'].extend(openfda.get('product_ndc', []))
            
            # Pharmacological classification
            result['pharmacological_class'].extend(openfda.get('pharm_class_epc', []))
            result['pharmacological_class'].extend(openfda.get('pharm_class_cs', []))
            result['mechanism_of_action'].extend(openfda.get('pharm_class_moa', []))
            result['physiologic_effect'].extend(openfda.get('pharm_class_pe', []))
            
            # Route and dosage forms
            result['route'].extend(openfda.get('route', []))
            
            # Manufacturer
            result['manufacturers'].extend(openfda.get('manufacturer_name', []))
            
            # Extract from products
            for product in products:
                # Active ingredients
                if product.get('active_ingredients'):
                    for ing in product['active_ingredients']:
                        if ing.get('name'):
                            ing_info = ing['name']
                            if ing.get('strength'):
                                ing_info += f" ({ing['strength']})"
                            result['active_ingredients'].append(ing_info)
                            result['strength'].append(ing.get('strength', ''))
                
                # Dosage form and route
                if product.get('route'):
                    result['route'].append(product['route'])
                
                if product.get('dosage_form'):
                    result['dosage_form'].append(product['dosage_form'])
                
                # Marketing status
                if product.get('marketing_status'):
                    result['marketing_status'].append(product['marketing_status'])
                
                # Brand and generic names from products
                if product.get('brand_name'):
                    result['brand_names'].append(product['brand_name'])
                
                # Reference drugs
                if product.get('reference_drug'):
                    result['reference_listed_drug'].append(product['reference_drug'])
                
                # TE Code
                if product.get('te_code'):
                    result['therapeutic_equivalence'].append(product['te_code'])
            
            # Extract from submissions
            for submission in submissions:
                sub_date = submission.get('submission_status_date')
                sub_type = submission.get('submission_type')
                sub_status = submission.get('submission_status')
                review_priority = submission.get('review_priority')
                notes = submission.get('submission_public_notes')
                
                # Submission class code
                if submission.get('submission_class_code'):
                    result['application_type'].append(submission['submission_class_code'])
                    
                # Link to application history with enriched data
                if app_no and sub_date:
                    parsed_date = self._parse_date_str(sub_date)
                    if parsed_date:
                        # Find or create application entry
                        app_entry = None
                        for entry in result['applications']:
                            if entry['application_number'] == app_no:
                                app_entry = entry
                                break
                        
                        if not app_entry:
                            app_entry = {
                                'application_number': app_no,
                                'application_type': match.get('openfda', {}).get('application_type') or 'NDA/ANDA',
                                'events': []
                            }
                            result['applications'].append(app_entry)
                        
                        # Create enriched event
                        event = {
                            'date': parsed_date,
                            'submission_type': sub_type,
                            'submission_number': submission.get('submission_number'),
                            'submission_status': sub_status,
                            'submission_class_code': submission.get('submission_class_code'),
                            'submission_class_code_description': submission.get('submission_class_code_description'),
                            'review_priority': review_priority,
                            'submission_public_notes': notes,
                            'application_docs': submission.get('application_docs', []),
                            'source': 'Drugs@FDA'
                        }
                        
                        # Simple deduplication
                        is_duplicate = False
                        for existing in app_entry['events']:
                            if (existing['date'] == event['date'] and 
                                existing['submission_type'] == event['submission_type'] and
                                existing.get('submission_number') == event.get('submission_number')):
                                is_duplicate = True
                                break
                        
                        if not is_duplicate:
                            app_entry['events'].append(event)
    
    def _extract_ndc_data(self, matches: List[Dict], result: Dict):
        """Extract comprehensive data from NDC matches."""
        for match in matches:
            openfda = match.get('openfda', {})
            
            # NDC codes
            product_ndc = match.get('product_ndc')
            if product_ndc:
                result['ndc_codes'].append(product_ndc)
            
            # Package NDC codes
            packaging = match.get('packaging', [])
            for package in packaging:
                package_ndc = package.get('package_ndc')
                if package_ndc:
                    result['ndc_package_codes'].append(package_ndc)
                
                # Packaging description
                desc = package.get('description')
                if desc:
                    result['packaging_info'].append(desc)
                
                # Marketing dates (stored for first_approval_date calculation)
            
            # Names
            result['generic_names'].extend(openfda.get('generic_name', []))
            result['brand_names'].extend(openfda.get('brand_name', []))
            
            if match.get('brand_name'):
                result['brand_names'].append(match['brand_name'])
            
            if match.get('generic_name'):
                result['generic_names'].append(match['generic_name'])
            
            # Substance name
            substance_name = match.get('substance_name')
            if substance_name:
                result['substance_names'].append(substance_name)
            
            # Labeler/Manufacturer
            labeler = match.get('labeler_name')
            if labeler:
                result['labelers'].append(labeler)
            
            result['manufacturers'].extend(openfda.get('manufacturer_name', []))
            
            # Dosage form and route
            dosage_form = match.get('dosage_form')
            if dosage_form:
                result['dosage_form'].append(dosage_form)
            
            result['route'].extend(openfda.get('route', []))
            
            # Identifiers
            result['unii_codes'].extend(openfda.get('unii', []))
            result['rxcui_codes'].extend(openfda.get('rxcui', []))
            result['spl_set_ids'].extend(openfda.get('spl_set_id', []))
            result['ndc_codes'].extend(openfda.get('product_ndc', []))
            
            # DEA Schedule
            dea_schedule = match.get('dea_schedule')
            if dea_schedule:
                result['dea_schedule'].append(dea_schedule)
            
            # Product type
            product_type = match.get('product_type')
            if product_type:
                result['product_type'].append(product_type)
            
            # Marketing category
            marketing_category = match.get('marketing_category')
            if marketing_category:
                result['application_type'].append(marketing_category)
            
            # Finished status
            finished = match.get('finished')
            if finished is not None:
                result['finished_status'].append(str(finished))
            
            # Listing expiration
            listing_exp = match.get('listing_expiration_date')
            if listing_exp:
                result['listing_expiration_dates'].append(listing_exp)
            
            # Active ingredients
            active_ingredients = match.get('active_ingredients', [])
            for ing in active_ingredients:
                if ing.get('name'):
                    ing_info = ing['name']
                    if ing.get('strength'):
                        ing_info += f" ({ing['strength']})"
                        result['strength'].append(ing['strength'])
                    result['active_ingredients'].append(ing_info)
            
            # Pharmacological class
            result['pharmacological_class'].extend(openfda.get('pharm_class_epc', []))
            result['pharmacological_class'].extend(openfda.get('pharm_class_cs', []))
            result['mechanism_of_action'].extend(openfda.get('pharm_class_moa', []))
            result['physiologic_effect'].extend(openfda.get('pharm_class_pe', []))
    
    def _extract_orangebook_data(self, matches: List[Dict], result: Dict):
        """Extract comprehensive data from Orange Book matches."""
        for match in matches:
            # Ingredient (generic name)
            ingredient = match.get('Ingredient')
            if ingredient:
                # Split by semicolon for combination products
                ingredients = [i.strip() for i in ingredient.split(';')]
                result['generic_names'].extend(ingredients)
            
            # Trade name (brand name)
            trade_name = match.get('Trade_Name')
            if trade_name:
                result['brand_names'].append(trade_name)
            
            # Applicant
            applicant = match.get('Applicant')
            if applicant:
                result['applicants'].append(applicant)
            
            applicant_full = match.get('Applicant_Full_Name')
            if applicant_full:
                result['applicants'].append(applicant_full)
            
            # Strength
            strength = match.get('Strength')
            if strength:
                result['strength'].append(strength)
            
            # Dosage form and route
            df_route = match.get('Dosage_Form;_Route') or match.get('DF;Route')
            if df_route:
                parts = [p.strip() for p in df_route.split(';')]
                if len(parts) >= 1:
                    result['dosage_form'].append(parts[0])
                if len(parts) >= 2:
                    result['route'].append(parts[1])
            
            # Application number and type
            appl_no = match.get('Appl_No')
            if appl_no:
                result['application_numbers'].append(appl_no)
            
            appl_type = match.get('Appl_Type')
            if appl_type:
                result['application_type'].append(appl_type)
            
            # Approval date
            approval_date = match.get('Approval_Date')
            # Store dates for first_approval_date calculation (no events created)            
            # Product type (RX/OTC/DISCN)
            prod_type = match.get('Type')
            if prod_type:
                result['rx_otc'].append(prod_type)
            
            # Reference Listed Drug
            rld = match.get('RLD')
            if rld:
                result['reference_listed_drug'].append(rld)
            
            # Reference Standard
            rs = match.get('RS')
            if rs:
                result['reference_standard'].append(rs)
            
            # Therapeutic Equivalence Code
            te_code = match.get('TE_Code')
            if te_code:
                result['therapeutic_equivalence'].append(te_code)
            
            # Marketing status
            marketing_status = match.get('Marketing_Status')
            if marketing_status:
                result['marketing_status'].append(marketing_status)
            
            # Get patent and exclusivity information
            product_no = match.get('Product_No')
            if appl_no and product_no:
                self._add_patent_info(appl_no, product_no, result)
                self._add_exclusivity_info(appl_no, product_no, result)
    
    def _add_patent_info(self, appl_no: str, product_no: str, result: Dict):
        """Add detailed patent information from Orange Book."""
        for patent in self.patent_data:
            if (patent.get('Appl_No') == appl_no and 
                patent.get('Product_No') == product_no):
                
                patent_info = {}
                
                # Patent number
                patent_no = patent.get('Patent_No')
                if patent_no:
                    patent_info['patent_number'] = patent_no
                
                # Patent expiration date
                patent_expire = patent.get('Patent_Expire_Date')
                if patent_expire:
                    patent_info['expiration_date'] = patent_expire
                
                # Drug substance flag
                drug_substance = patent.get('Drug_Substance_Flag')
                if drug_substance:
                    patent_info['drug_substance'] = drug_substance
                
                # Drug product flag
                drug_product = patent.get('Drug_Product_Flag')
                if drug_product:
                    patent_info['drug_product'] = drug_product
                
                # Patent use code
                use_code = patent.get('Patent_Use_Code')
                if use_code:
                    patent_info['use_code'] = use_code
                
                # Delist request flag
                delist = patent.get('Patent_Delist_Request_Flag')
                if delist:
                    patent_info['delist_requested'] = delist
                
                # Submission date
                submission_date = patent.get('Submission_Date')
                if submission_date:
                    patent_info['submission_date'] = submission_date
                    
                    # Link to application history for patent submission
                    self._add_application_event(
                        result=result,
                        app_no=appl_no,
                        app_type='NDA', # Patents mostly for NDAs
                        date=submission_date,
                        event_type='PATENT SUBMISSION',
                        status='Submitted',
                        description=f"Patent: {patent_no}",
                        source="Orange Book (Patents)"
                    )
                
                if patent_info:
                    result['patents'].append(patent_info)
    
    def _add_exclusivity_info(self, appl_no: str, product_no: str, result: Dict):
        """Add detailed exclusivity information from Orange Book."""
        for exclusivity in self.exclusivity_data:
            if (exclusivity.get('Appl_No') == appl_no and 
                exclusivity.get('Product_No') == product_no):
                
                excl_info = {}
                
                # Exclusivity code
                excl_code = exclusivity.get('Exclusivity_Code')
                if excl_code:
                    excl_info['code'] = excl_code
                
                # Exclusivity date
                excl_date = exclusivity.get('Exclusivity_Date')
                if excl_date:
                    excl_info['expiration_date'] = excl_date
                
                if excl_info:
                    result['exclusivities'].append(excl_info)
    
    def _cross_reference_by_application(self, result: Dict):
        """Cross-reference data across sources using application numbers."""
        # Cross-reference by NDA/ANDA numbers
        for app_no in result['application_numbers']:
            # Get additional Orange Book data
            ob_products = self.orangebook_by_appl.get(app_no, [])
            for product in ob_products:
                # Extract any missing information
                if product.get('Trade_Name') and product['Trade_Name'] not in result['brand_names']:
                    result['brand_names'].append(product['Trade_Name'])
                
                if product.get('Ingredient'):
                    ingredients = [i.strip() for i in product['Ingredient'].split(';')]
                    for ing in ingredients:
                        if ing not in result['generic_names']:
                            result['generic_names'].append(ing)
        
        # Cross-reference by BLA numbers
        for bla_no in result['bla_numbers']:
            # Get additional Purple Book data
            pb_products = self.purplebook_by_bla.get(bla_no, [])
            for product in pb_products:
                # Extract any missing information
                proprietary_name = product.get('Proprietary Name')
                if proprietary_name:
                    for name in proprietary_name.split(','):
                        name = name.strip()
                        if name and name not in result['brand_names']:
                            result['brand_names'].append(name)
                
                proper_name = product.get('Proper Name')
                if proper_name and proper_name not in result['generic_names']:
                    result['generic_names'].append(proper_name)
    
    def _harmonize_names(self, result: Dict):
        """Determine harmonized drug names."""
        # Harmonized generic name: most common generic name
        if result['generic_names']:
            name_counts = defaultdict(int)
            for name in result['generic_names']:
                name_counts[name] += 1
            result['harmonized_generic_name'] = max(name_counts.items(), 
                                                    key=lambda x: x[1])[0]
        
        # Harmonized brand name: most common brand name
        if result['brand_names']:
            name_counts = defaultdict(int)
            for name in result['brand_names']:
                name_counts[name] += 1
            result['harmonized_brand_name'] = max(name_counts.items(), 
                                                  key=lambda x: x[1])[0]
        
        # Harmonized drug name: prefer generic, fallback to brand
        result['harmonized_drug_name'] = (result['harmonized_generic_name'] or 
                                          result['harmonized_brand_name'] or 
                                          result['query_name'])
    
    def _deduplicate_lists(self, result: Dict):
        """Remove duplicates from all list fields while preserving order."""
        # Generic deduplication for string/simple lists
        for key in result:
            if isinstance(result[key], list) and key not in ['patents', 'exclusivities', 'applications', 'application_docs']:
                # Preserve order while removing duplicates
                seen = set()
                deduped = []
                for item in result[key]:
                    if not item:
                        continue
                        
                    # Handle unhashable types (dicts)
                    if isinstance(item, dict):
                        # Convert to tuple of sorted items for hashing
                        # Recursive conversion not implemented, assumes flat dicts
                        try:
                            item_hash = tuple(sorted((k, str(v)) for k, v in item.items()))
                        except Exception:
                            # Fallback: keep all dicts if unhashable
                            deduped.append(item)
                            continue
                            
                        if item_hash not in seen:
                            seen.add(item_hash)
                            deduped.append(item)
                    else:
                        # String/simple types
                        item_lower = str(item).lower().strip()
                        if item_lower and item_lower not in seen:
                            seen.add(item_lower)
                            deduped.append(item)
                result[key] = deduped
    
    def _extract_indications_from_history(self, result: Dict):
        """
        Extract indication text from label PDFs found in application history.
        Populates 'indication_approval_history' with {date, indication, url}.
        """
        if not self.indication_extractor:
            return
            
        print("  Checking for label PDFs in application history...")
        history_entries = []
        
        # Collect all potential labels from application events
        for app in result.get('applications', []):
            for event in app.get('events', []):
                event_date = event.get('date')
                
                # Check application docs for labels
                for doc in event.get('application_docs', []):
                    if doc.get('type') == 'Label' and doc.get('url', '').endswith('.pdf'):
                        url = doc['url']
                        
                        # Fix URL if needed
                        if url.startswith('http://'):
                            url = url.replace('http://', 'https://')
                        
                        print(f"  Found label URL: {url} (Date: {event_date})")
                        
                        # Download and extract
                        pdf_path = self.indication_extractor.download_pdf(url)
                        if pdf_path:
                            indication = self.indication_extractor.extract_indication_section(pdf_path)
                            if indication:
                                entry = {
                                    'date': event_date,
                                    'indication': indication,
                                    'url': url,
                                    'submission_type': event.get('submission_type'),
                                    'submission_number': event.get('submission_number')
                                }
                                history_entries.append(entry)
                                print(f"  ✓ Extracted indication ({len(indication)} chars)")
        
        # Sort by date (oldest first)
        history_entries.sort(key=lambda x: x.get('date', '0000-00-00'))
        
        result['indication_approval_history'] = history_entries

    def _calculate_confidence(self, result: Dict, sources: Set[str]) -> float:
        """Calculate confidence score based on data completeness."""
        score = 0.0
        
        # Base score from number of sources (max 40%)
        source_score = len(sources) / 4.0  # Updated to 4 sources
        score += source_score * 0.4
        
        # Score from critical fields (max 40%)
        critical_fields = {
            'harmonized_generic_name': 10,
            'harmonized_brand_name': 10,
            'ndc_codes': 8,
            'application_numbers': 5,
            'bla_numbers': 5,
            'active_ingredients': 7
        }
        
        critical_score = 0
        max_critical = sum(critical_fields.values())
        
        for field, weight in critical_fields.items():
            if result.get(field):
                critical_score += weight
        
        score += (critical_score / max_critical) * 0.4
        
        # Score from additional fields (max 20%)
        additional_fields = [
            'strength', 'route', 'dosage_form', 'unii_codes',
            'pharmacological_class', 'therapeutic_equivalence',
            'manufacturers', 'patents', 'exclusivities',
            'bla_type', 'licensure_status', 'interchangeability'
        ]
        
        fields_with_data = sum(1 for field in additional_fields if result.get(field))
        additional_score = fields_with_data / len(additional_fields)
        score += additional_score * 0.2
        
        return round(min(score, 1.0), 2)
    
    def harmonize_drug_list(self, drug_list_path: str, output_path: str = None) -> Dict:
        """
        Harmonize a list of drugs from a file.
        
        Args:
            drug_list_path: Path to drugs.txt (one drug per line)
            output_path: Optional path to save JSON output
            
        Returns:
            Dictionary mapping drug names to harmonized data
        """
        results = {}
        
        try:
            with open(drug_list_path, 'r', encoding='utf-8') as f:
                drugs = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Error reading drug list: {e}")
            return results
        
        total = len(drugs)
        for idx, drug in enumerate(drugs, 1):
            print(f"Harmonizing [{idx}/{total}]: {drug}")
            results[drug] = self.harmonize_drug(drug)
        
        if output_path:
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                print(f"\nResults saved to: {output_path}")
            except Exception as e:
                print(f"Error saving results: {e}")
        
        return results
    
    def get_statistics(self, results: Dict) -> Dict:
        """
        Get statistics about the harmonization results.
        
        Args:
            results: Dictionary of harmonized results
            
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_drugs': len(results),
            'successfully_harmonized': 0,
            'avg_confidence': 0.0,
            'sources_coverage': defaultdict(int),
            'fields_coverage': defaultdict(int),
            'high_confidence': 0,  # >= 0.8
            'medium_confidence': 0,  # 0.5-0.8
            'low_confidence': 0  # < 0.5
        }
        
        confidence_scores = []
        
        for drug_name, data in results.items():
            confidence = data.get('confidence_score', 0)
            confidence_scores.append(confidence)
            
            if confidence >= 0.8:
                stats['high_confidence'] += 1
            elif confidence >= 0.5:
                stats['medium_confidence'] += 1
            else:
                stats['low_confidence'] += 1
            
            if data.get('harmonized_drug_name'):
                stats['successfully_harmonized'] += 1
            
            for source in data.get('sources_used', []):
                stats['sources_coverage'][source] += 1
            
            # Track field coverage
            for field, value in data.items():
                if field not in ['query_name', 'sources_used', 'confidence_score', 'last_updated']:
                    if value and (not isinstance(value, list) or len(value) > 0):
                        stats['fields_coverage'][field] += 1
        
        if confidence_scores:
            stats['avg_confidence'] = round(sum(confidence_scores) / len(confidence_scores), 2)
        
        stats['sources_coverage'] = dict(stats['sources_coverage'])
        stats['fields_coverage'] = dict(sorted(
            stats['fields_coverage'].items(),
            key=lambda x: x[1],
            reverse=True
        ))
        
        return stats
    
    def export_to_csv(self, results: Dict, output_path: str):
        """
        Export harmonized results to CSV format.
        
        Args:
            results: Dictionary of harmonized results
            output_path: Path to save CSV file
        """
        import csv
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                # Define CSV columns
                columns = [
                    'query_name', 'harmonized_drug_name', 'harmonized_generic_name',
                    'harmonized_brand_name', 'strength', 'route', 'dosage_form',
                    'ndc_codes', 'application_numbers', 'bla_numbers', 'unii_codes',
                    'manufacturers', 'pharmacological_class', 'marketing_status',
                    'therapeutic_equivalence', 'dea_schedule', 'bla_type',
                    'licensure_status', 'interchangeability', 'confidence_score',
                    'sources_used'
                ]
                
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                
                for drug_name, data in results.items():
                    row = {}
                    for col in columns:
                        value = data.get(col, '')
                        if isinstance(value, list):
                            row[col] = '; '.join(str(v) for v in value if v)
                        else:
                            row[col] = str(value) if value else ''
                    writer.writerow(row)
            
            print(f"CSV exported to: {output_path}")
        except Exception as e:
            print(f"Error exporting to CSV: {e}")


# Example usage and demonstration
if __name__ == "__main__":
    print("=" * 70)
    print("FDA Drug Harmonization System (with Purple Book)")
    print("=" * 70)
    
    # Initialize harmonizer
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
    
    print("\n" + "=" * 70)
    print("Example 1: Harmonize TREPROSTINIL SODIUM")
    print("=" * 70)
    
    result = harmonizer.harmonize_drug("TREPROSTINIL SODIUM")
    print(json.dumps(result, indent=2))
    
    print("\n" + "=" * 70)
    print("Example 2: Harmonize Humira")
    print("=" * 70)
    
    result2 = harmonizer.harmonize_drug("Humira")
    print(json.dumps(result2, indent=2))

