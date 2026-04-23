"""
openFDA API Client
Integrates with openFDA for drug information and harmonization
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class FDADrugInfo:
    """Data class for FDA drug information"""
    application_number: Optional[str]
    brand_name: str
    generic_name: str
    manufacturer_name: Optional[str]
    product_ndc: List[str]
    active_ingredients: List[Dict[str, str]]
    dosage_form: Optional[str]
    route: List[str]
    marketing_status: Optional[str]
    unii: List[str]

class OpenFDAClient:
    """Client for openFDA API integration"""
    
    def __init__(self, base_url: str = "https://api.fda.gov/drug", 
                 request_delay: float = 0.5, max_retries: int = 3):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Clinical-Trials-Data-Pipeline/1.0'
        })
        self._drug_cache = {}
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                # Rate limiting - openFDA has rate limits
                time.sleep(self.request_delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch data after {self.max_retries} attempts")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def search_drug_by_name(self, drug_name: str, limit: int = 10) -> Optional[List[FDADrugInfo]]:
        """Search for drugs by name in the NDC database"""
        url = f"{self.base_url}/ndc.json"
        
        # Use combined OR query for brand_name and generic_name
        search_query = f'brand_name:"{drug_name}" OR generic_name:"{drug_name}"'
        
        params = {
            'search': search_query,
            'limit': limit
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data:
            return None
        
        all_drugs = []
        seen_ndcs = set()
        
        for result in data.get('results', []):
            ndc = result.get('product_ndc', '')
            if ndc and ndc not in seen_ndcs:
                seen_ndcs.add(ndc)
                drug_info = self._parse_drug_info(result)
                if drug_info:
                    all_drugs.append(drug_info)
        
        return all_drugs if all_drugs else None
    
    def _parse_drug_info(self, result: Dict) -> Optional[FDADrugInfo]:
        """Parse FDA drug information from API result"""
        try:
            openfda = result.get('openfda', {})
            
            # Extract active ingredients
            active_ingredients = []
            for ingredient in result.get('active_ingredients', []):
                active_ingredients.append({
                    'name': ingredient.get('name', ''),
                    'strength': ingredient.get('strength', '')
                })
            
            # Extract UNII codes
            unii_list = openfda.get('unii', [])
            if not isinstance(unii_list, list):
                unii_list = [unii_list] if unii_list else []
            
            drug = FDADrugInfo(
                application_number=openfda.get('application_number', [None])[0] if openfda.get('application_number') else None,
                brand_name=result.get('brand_name', '') or openfda.get('brand_name', [''])[0],
                generic_name=result.get('generic_name', '') or openfda.get('generic_name', [''])[0],
                manufacturer_name=openfda.get('manufacturer_name', [None])[0] if openfda.get('manufacturer_name') else None,
                product_ndc=[result.get('product_ndc', '')],
                active_ingredients=active_ingredients,
                dosage_form=result.get('dosage_form', '') or openfda.get('dosage_form', [''])[0],
                route=result.get('route', []) or openfda.get('route', []),
                marketing_status=result.get('marketing_status', ''),
                unii=unii_list
            )
            
            return drug
            
        except Exception as e:
            logger.error(f"Error parsing FDA drug info: {e}")
            return None
    
    def search_drug_labels(self, drug_name: str, limit: int = 5) -> Optional[List[Dict]]:
        """Search drug labels for detailed information"""
        url = f"{self.base_url}/label.json"
        
        params = {
            'search': f'openfda.brand_name:"{drug_name}" OR openfda.generic_name:"{drug_name}"',
            'limit': limit
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data:
            return None
        
        labels = []
        for result in data.get('results', []):
            label_info = {
                'product_type': result.get('product_type', [''])[0] if result.get('product_type') else '',
                'indications_and_usage': result.get('indications_and_usage', [''])[0] if result.get('indications_and_usage') else '',
                'warnings': result.get('warnings', [''])[0] if result.get('warnings') else '',
                'adverse_reactions': result.get('adverse_reactions', [''])[0] if result.get('adverse_reactions') else '',
                'openfda': result.get('openfda', {})
            }
            labels.append(label_info)
        
        return labels if labels else None
    
    def search_adverse_events(self, drug_name: str, limit: int = 100) -> Optional[Dict]:
        """Search drug adverse events"""
        url = f"{self.base_url}/event.json"
        
        params = {
            'search': f'patient.drug.openfda.brand_name:"{drug_name}" OR patient.drug.openfda.generic_name:"{drug_name}"',
            'count': 'patient.reaction.reactionmeddrapt.exact',
            'limit': limit
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data:
            return None
        
        # Aggregate adverse event counts
        adverse_events = {}
        for result in data.get('results', []):
            term = result.get('term', '')
            count = result.get('count', 0)
            if term:
                adverse_events[term] = count
        
        return adverse_events if adverse_events else None
    
    def get_drug_by_ndc(self, ndc: str) -> Optional[FDADrugInfo]:
        """Get drug information by NDC code"""
        url = f"{self.base_url}/ndc.json"
        
        params = {
            'search': f'product_ndc:"{ndc}"',
            'limit': 1
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data or not data['results']:
            return None
        
        return self._parse_drug_info(data['results'][0])
    
    def normalize_drug_names(self, drug_names: List[str]) -> Dict[str, Optional[List[FDADrugInfo]]]:
        """Normalize a list of drug names using openFDA"""
        normalized_drugs = {}
        
        for drug_name in drug_names:
            if not drug_name or drug_name.lower() in ['placebo', 'control']:
                continue
            
            logger.info(f"Searching FDA database for: {drug_name}")
            drugs = self.search_drug_by_name(drug_name)
            normalized_drugs[drug_name] = drugs
            
            if not drugs:
                logger.warning(f"Could not find drug in FDA database: {drug_name}")
        
        return normalized_drugs

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = OpenFDAClient()
    
    # Test drug search
    drugs = client.search_drug_by_name("pirfenidone")
    if drugs:
        print(f"Found {len(drugs)} drugs for 'pirfenidone' in FDA database")
        for drug in drugs[:3]:
            print(f"  - {drug.brand_name} ({drug.generic_name})")
            print(f"    NDC: {', '.join(drug.product_ndc)}")
            print(f"    UNII: {', '.join(drug.unii)}")
    
    # Test drug labels
    labels = client.search_drug_labels("pirfenidone")
    if labels:
        print(f"\nFound {len(labels)} labels for 'pirfenidone'")
