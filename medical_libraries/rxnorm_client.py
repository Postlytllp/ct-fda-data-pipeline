"""
RxNorm API Client
Integrates with RxNorm for drug information and normalization
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
import json
import re

logger = logging.getLogger(__name__)

@dataclass
class RxNormDrug:
    """Data class for RxNorm drug information"""
    rxcui: str
    name: str
    synonym: Optional[str]
    tty: str  # Term Type
    language: str
    suppress: str
    umlscui: Optional[str]

@dataclass
class DrugInfo:
    """Enhanced drug information"""
    rxcui: str
    name: str
    brand_names: List[str]
    generic_name: str
    drug_class: Optional[str]
    ndc_codes: List[str]
    atc_codes: List[str]
    synonyms: List[str]
    dosage_forms: List[str]
    ingredients: List[str]

class RxNormClient:
    """Client for RxNorm API integration"""
    
    def __init__(self, base_url: str = "https://rxnav.nlm.nih.gov/REST", 
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
    
    def search_drug_by_name(self, drug_name: str) -> Optional[List[RxNormDrug]]:
        """Search for drugs by name using findRxcuiByString endpoint"""
        url = f"{self.base_url}/rxcui.json"
        params = {
            'name': drug_name,
            'search': '2'  # Search type 2 = normalized string search
        }
        
        data = self._make_request(url, params)
        if not data or 'idGroup' not in data:
            return None
        
        drugs = []
        try:
            id_group = data.get('idGroup', {})
            rxnorm_ids = id_group.get('rxnormId', [])
            
            # Handle both list and single value responses
            if not isinstance(rxnorm_ids, list):
                rxnorm_ids = [rxnorm_ids] if rxnorm_ids else []
            
            for rxcui in rxnorm_ids:
                # Get properties for each RXCUI
                props_url = f"{self.base_url}/rxcui/{rxcui}/properties.json"
                props_data = self._make_request(props_url)
                
                if props_data and 'properties' in props_data:
                    prop = props_data['properties']
                    drug = RxNormDrug(
                        rxcui=prop.get('rxcui', rxcui),
                        name=prop.get('name', ''),
                        synonym=prop.get('synonym', ''),
                        tty=prop.get('tty', ''),
                        language=prop.get('language', 'ENG'),
                        suppress=prop.get('suppress', 'N'),
                        umlscui=prop.get('umlscui')
                    )
                    drugs.append(drug)
        except Exception as e:
            logger.error(f"Error parsing drug search results: {e}")
        
        return drugs if drugs else None
    
    def get_drug_info(self, rxcui: str) -> Optional[DrugInfo]:
        """Get comprehensive drug information by RxCUI"""
        if rxcui in self._drug_cache:
            return self._drug_cache[rxcui]
        
        # Get basic drug properties
        drug_info = self._get_drug_properties(rxcui)
        if not drug_info:
            return None
        
        # Get additional information
        brand_names = self._get_brand_names(rxcui)
        ndc_codes = self._get_ndc_codes(rxcui)
        atc_codes = self._get_atc_codes(rxcui)
        synonyms = self._get_synonyms(rxcui)
        ingredients = self._get_ingredients(rxcui)
        
        enhanced_info = DrugInfo(
            rxcui=rxcui,
            name=drug_info.get('name', ''),
            brand_names=brand_names or [],
            generic_name=drug_info.get('name', ''),
            drug_class=drug_info.get('drugClass'),
            ndc_codes=ndc_codes or [],
            atc_codes=atc_codes or [],
            synonyms=synonyms or [],
            dosage_forms=[],
            ingredients=ingredients or []
        )
        
        # Cache the result
        self._drug_cache[rxcui] = enhanced_info
        
        return enhanced_info
    
    def _get_drug_properties(self, rxcui: str) -> Optional[Dict]:
        """Get basic drug properties"""
        url = f"{self.base_url}/rxcui/{rxcui}/properties.json"
        data = self._make_request(url)
        
        if not data or 'properties' not in data:
            return None
        
        properties = data.get('properties', {})
        return {
            'name': properties.get('name', ''),
            'rxcui': properties.get('rxcui', rxcui),
            'tty': properties.get('tty', ''),
            'synonym': properties.get('synonym', '')
        }
    
    def _get_brand_names(self, rxcui: str) -> Optional[List[str]]:
        """Get brand names for a drug using related concepts"""
        brand_names = []
        
        # Query for different brand-related term types separately
        term_types = ['BN', 'SBD', 'BPCK']  # Brand Name, Branded Drug, Brand Pack
        
        for tty in term_types:
            url = f"{self.base_url}/rxcui/{rxcui}/related.json"
            params = {'tty': tty}
            data = self._make_request(url, params)
            
            if data and 'relatedGroup' in data:
                related_group = data.get('relatedGroup', {})
                concept_groups = related_group.get('conceptGroup', [])
                
                for group in concept_groups:
                    concepts = group.get('conceptProperties', [])
                    for concept in concepts:
                        name = concept.get('name', '')
                        if name and name not in brand_names:
                            brand_names.append(name)
        
        return brand_names if brand_names else None
    
    def _get_ndc_codes(self, rxcui: str) -> Optional[List[str]]:
        """Get NDC codes for a drug"""
        url = f"{self.base_url}/rxcui/{rxcui}/ndcs.json"
        data = self._make_request(url)
        
        if not data or 'ndcGroup' not in data:
            return None
        
        ndc_group = data.get('ndcGroup', {})
        ndc_list = ndc_group.get('ndcList', [])
        
        if isinstance(ndc_list, list):
            return [ndc for ndc in ndc_list if ndc]
        
        return None
    
    def _get_atc_codes(self, rxcui: str) -> Optional[List[str]]:
        """Get ATC codes for a drug using RxClass API"""
        url = f"{self.base_url}/rxcui/{rxcui}/property.json"
        params = {'propName': 'ATC'}
        data = self._make_request(url, params)
        
        if not data:
            return None
        
        atc_codes = []
        prop_concept_group = data.get('propConceptGroup', {})
        prop_concepts = prop_concept_group.get('propConcept', [])
        
        for prop in prop_concepts:
            if prop.get('propName') == 'ATC':
                atc_codes.append(prop.get('propValue', ''))
        
        return atc_codes if atc_codes else None
    
    def _get_synonyms(self, rxcui: str) -> Optional[List[str]]:
        """Get synonyms for a drug using allrelated endpoint"""
        url = f"{self.base_url}/rxcui/{rxcui}/allrelated.json"
        data = self._make_request(url)
        
        if not data or 'allRelatedGroup' not in data:
            return None
        
        synonyms = []
        all_related = data.get('allRelatedGroup', {})
        concept_groups = all_related.get('conceptGroup', [])
        
        for group in concept_groups:
            concepts = group.get('conceptProperties', [])
            for concept in concepts:
                name = concept.get('name', '')
                if name and name not in synonyms:
                    synonyms.append(name)
        
        return synonyms if synonyms else None
    
    def _get_ingredients(self, rxcui: str) -> Optional[List[str]]:
        """Get ingredients for a drug"""
        url = f"{self.base_url}/rxcui/{rxcui}/related.json"
        params = {'tty': 'IN'}  # Ingredient
        data = self._make_request(url, params)
        
        if not data or 'relatedGroup' not in data:
            return None
        
        related_group = data.get('relatedGroup', {})
        concept_groups = related_group.get('conceptGroup', [])
        
        ingredients = []
        for group in concept_groups:
            if group.get('tty') == 'IN':
                concepts = group.get('conceptProperties', [])
                for concept in concepts:
                    ingredients.append(concept.get('name', ''))
        
        return ingredients if ingredients else None
    
    def normalize_drug_names(self, drug_names: List[str]) -> Dict[str, Optional[DrugInfo]]:
        """Normalize a list of drug names"""
        normalized_drugs = {}
        
        for drug_name in drug_names:
            if not drug_name or drug_name.lower() in ['placebo', 'control']:
                continue
            
            # Search for the drug
            drugs = self.search_drug_by_name(drug_name)
            if drugs:
                # Get the first match's detailed info
                drug_info = self.get_drug_info(drugs[0].rxcui)
                normalized_drugs[drug_name] = drug_info
            else:
                logger.warning(f"Could not find drug: {drug_name}")
                normalized_drugs[drug_name] = None
        
        return normalized_drugs
    
    def get_ipf_related_drugs(self) -> Dict[str, DrugInfo]:
        """Get information about commonly used IPF drugs"""
        ipf_drugs = [
            'pirfenidone',
            'nintedanib',
            'esbriet',
            'ofev',
            'prednisone',
            'azathioprine',
            'cyclophosphamide',
            'mycophenolate',
            'n-acetylcysteine',
            'sildenafil'
        ]
        
        logger.info(f"Fetching information for {len(ipf_drugs)} IPF-related drugs")
        
        drug_info = self.normalize_drug_names(ipf_drugs)
        
        # Filter out None values
        valid_drugs = {name: info for name, info in drug_info.items() if info is not None}
        
        logger.info(f"Successfully retrieved information for {len(valid_drugs)} drugs")
        
        return valid_drugs

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = RxNormClient()
    
    # Test drug search
    drugs = client.search_drug_by_name("pirfenidone")
    if drugs:
        print(f"Found {len(drugs)} drugs for 'pirfenidone'")
        for drug in drugs[:3]:
            print(f"  - {drug.name} (RxCUI: {drug.rxcui})")
    
    # Test drug info
    if drugs:
        drug_info = client.get_drug_info(drugs[0].rxcui)
        if drug_info:
            print(f"\nDrug Info for {drug_info.name}:")
            print(f"  Brand Names: {', '.join(drug_info.brand_names)}")
            print(f"  Ingredients: {', '.join(drug_info.ingredients)}")
    
    # Test IPF drugs
    ipf_drugs = client.get_ipf_related_drugs()
    print(f"\nFound {len(ipf_drugs)} IPF-related drugs:")
    for name, info in list(ipf_drugs.items())[:5]:
        print(f"  - {name}: {info.name if info else 'Not found'}")
