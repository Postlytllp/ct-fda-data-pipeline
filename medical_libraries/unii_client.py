"""
UNII (Unique Ingredient Identifier) Client
Integrates with FDA UNII substance registry for drug substance identification
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class UNIISubstance:
    """Data class for UNII substance information"""
    unii: str
    display_name: str
    preferred_term: str
    substance_type: Optional[str]
    cas_number: Optional[str]
    molecular_formula: Optional[str]
    inchi_key: Optional[str]
    smiles: Optional[str]
    grade: Optional[str]

class UNIIClient:
    """Client for UNII substance registry integration"""
    
    def __init__(self, base_url: str = "https://api.fda.gov/other", 
                 request_delay: float = 0.5, max_retries: int = 3):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Clinical-Trials-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })
        self._substance_cache = {}
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                # Rate limiting
                time.sleep(self.request_delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to fetch data after {self.max_retries} attempts")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return None
    
    def search_substance_by_name(self, substance_name: str) -> Optional[List[UNIISubstance]]:
        """Search for substances by name using openFDA API"""
        url = f"{self.base_url}/unii.json"
        
        # Use combined OR query for substance_name and synonym
        search_query = f'substance_name:"{substance_name}" OR synonym:"{substance_name}"'
        
        params = {
            'search': search_query,
            'limit': 20
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data:
            return None
        
        substances = []
        try:
            for item in data.get('results', []):
                substance = self._parse_substance(item)
                if substance:
                    substances.append(substance)
        except Exception as e:
            logger.error(f"Error parsing UNII search results: {e}")
        
        return substances if substances else None
    
    def get_substance_by_unii(self, unii: str) -> Optional[UNIISubstance]:
        """Get substance information by UNII code using openFDA API"""
        if unii in self._substance_cache:
            return self._substance_cache[unii]
        
        url = f"{self.base_url}/unii.json"
        
        params = {
            'search': f'unii:"{unii}"',
            'limit': 1
        }
        
        data = self._make_request(url, params)
        if not data or 'results' not in data or not data['results']:
            return None
        
        substance = self._parse_substance(data['results'][0])
        if substance:
            self._substance_cache[unii] = substance
        
        return substance
    
    def _parse_substance(self, data: Dict) -> Optional[UNIISubstance]:
        """Parse UNII substance data from openFDA API response"""
        try:
            # openFDA UNII API returns simplified structure
            substance = UNIISubstance(
                unii=data.get('unii', ''),
                display_name=data.get('display_name', '') or data.get('substance_name', ''),
                preferred_term=data.get('substance_name', ''),
                substance_type=data.get('substance_class', ''),
                cas_number=data.get('cas_number'),
                molecular_formula=data.get('molecular_formula'),
                inchi_key=data.get('inchi_key'),
                smiles=data.get('smiles'),
                grade=None  # Not provided by openFDA UNII API
            )
            
            return substance
            
        except Exception as e:
            logger.error(f"Error parsing UNII substance: {e}")
            return None
    
    def normalize_drug_names(self, drug_names: List[str]) -> Dict[str, Optional[List[UNIISubstance]]]:
        """Normalize a list of drug names using UNII registry"""
        normalized_substances = {}
        
        for drug_name in drug_names:
            if not drug_name or drug_name.lower() in ['placebo', 'control']:
                continue
            
            logger.info(f"Searching UNII registry for: {drug_name}")
            substances = self.search_substance_by_name(drug_name)
            normalized_substances[drug_name] = substances
            
            if not substances:
                logger.warning(f"Could not find substance in UNII: {drug_name}")
        
        return normalized_substances

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = UNIIClient()
    
    # Test substance search
    substances = client.search_substance_by_name("pirfenidone")
    if substances:
        print(f"Found {len(substances)} substances for 'pirfenidone'")
        for substance in substances[:3]:
            print(f"  - {substance.display_name}")
            print(f"    UNII: {substance.unii}")
            print(f"    CAS: {substance.cas_number}")
            print(f"    Formula: {substance.molecular_formula}")
