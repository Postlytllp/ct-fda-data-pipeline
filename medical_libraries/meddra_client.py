"""
MedDRA API Client
Integrates with MedDRA (Medical Dictionary for Regulatory Activities) for adverse event coding
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
class MedDRATerm:
    """Data class for MedDRA term information"""
    meddra_code: str
    term: str
    soc: str  # System Organ Class
    hlgt: str  # High Level Group Term
    hlt: str  # High Level Term
    pt: str  # Preferred Term
    llt: str  # Lowest Level Term
    level: int  # 1=SOC, 2=HLGT, 3=HLT, 4=PT, 5=LLT

@dataclass
class AdverseEvent:
    """Data class for adverse event information"""
    event_code: str
    event_term: str
    severity: Optional[str]
    frequency: Optional[str]
    onset: Optional[str]
    outcome: Optional[str]
    related_to_drug: bool
    meddra_info: Optional[MedDRATerm]

class MedDRAClient:
    """Client for MedDRA API integration"""
    
    def __init__(self, base_url: str = "https://api.fda.gov/drug/label.json", 
                 request_delay: float = 0.5, max_retries: int = 3):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Clinical-Trials-Data-Pipeline/1.0'
        })
        self._meddra_cache = {}
        
        # Initialize with common IPF-related MedDRA codes
        self._initialize_ipf_codes()
    
    def _initialize_ipf_codes(self):
        """Initialize with common IPF-related MedDRA codes"""
        self.ipf_related_codes = {
            # Respiratory system disorders
            '10017911': 'Idiopathic pulmonary fibrosis',
            '10026115': 'Pulmonary fibrosis',
            '10007986': 'Interstitial lung disease',
            '10033135': 'Respiratory failure',
            '10041144': 'Dyspnoea',
            '10021454': 'Cough',
            '10038278': 'Oxygen saturation decreased',
            
            # Common adverse events in IPF treatments
            '10027159': 'Nausea',
            '10022855': 'Fatigue',
            '10017916': 'Diarrhoea',
            '10038285': 'Photosensitivity reaction',
            '10021457': 'Rash',
            '10037781': 'Liver function test abnormal',
            '10042365': 'Weight decreased',
            '10041040': 'Decreased appetite',
            
            # Laboratory abnormalities
            '10037781': 'Liver function test abnormal',
            '10042365': 'Weight decreased',
            '10033135': 'Respiratory failure',
            '10041144': 'Dyspnoea'
        }
    
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
    
    def search_meddra_terms(self, query: str) -> Optional[List[MedDRATerm]]:
        """Search for MedDRA terms"""
        # Since direct MedDRA API requires subscription, we'll use FDA OpenFDA
        # which includes some MedDRA coding in drug labels
        
        params = {
            'search': f'adverse_reactions.reactionmeddrapt:"{query}"',
            'limit': '10'
        }
        
        data = self._make_request(self.base_url, params)
        if not data:
            return None
        
        terms = []
        try:
            results = data.get('results', [])
            for result in results:
                adverse_reactions = result.get('adverse_reactions', [])
                for reaction in adverse_reactions:
                    meddra_term = reaction.get('reactionmeddrapt', '')
                    if meddra_term and query.lower() in meddra_term.lower():
                        # Create a simplified MedDRA term
                        term = MedDRATerm(
                            meddra_code='',  # Not available in OpenFDA
                            term=meddra_term,
                            soc='',  # Not available in OpenFDA
                            hlgt='',
                            hlt='',
                            pt=meddra_term,
                            llt=meddra_term,
                            level=4  # PT level
                        )
                        terms.append(term)
        except Exception as e:
            logger.error(f"Error parsing MedDRA search results: {e}")
        
        return terms if terms else None
    
    def get_ipf_adverse_events(self) -> Dict[str, MedDRATerm]:
        """Get IPF-related adverse events"""
        logger.info("Fetching IPF-related adverse events")
        
        ipf_events = {}
        for code, term in self.ipf_related_codes.items():
            meddra_term = MedDRATerm(
                meddra_code=code,
                term=term,
                soc=self._categorize_by_soc(term),
                hlgt='',
                hlt='',
                pt=term,
                llt=term,
                level=4
            )
            ipf_events[code] = meddra_term
        
        logger.info(f"Loaded {len(ipf_events)} IPF-related adverse events")
        return ipf_events
    
    def _categorize_by_soc(self, term: str) -> str:
        """Categorize term by System Organ Class"""
        respiratory_keywords = ['pulmonary', 'respiratory', 'lung', 'breath', 'cough', 'dyspnoea']
        gastrointestinal_keywords = ['nausea', 'diarrhoea', 'vomiting', 'abdominal', 'gastro']
        hepatic_keywords = ['liver', 'hepatic', 'transaminase', 'bilirubin']
        dermatological_keywords = ['rash', 'skin', 'photosensitivity', 'dermatitis']
        general_keywords = ['fatigue', 'weight', 'appetite', 'fever', 'pain']
        laboratory_keywords = ['test', 'laboratory', 'abnormal', 'decreased', 'increased']
        
        term_lower = term.lower()
        
        if any(keyword in term_lower for keyword in respiratory_keywords):
            return 'Respiratory, thoracic and mediastinal disorders'
        elif any(keyword in term_lower for keyword in gastrointestinal_keywords):
            return 'Gastrointestinal disorders'
        elif any(keyword in term_lower for keyword in hepatic_keywords):
            return 'Hepatobiliary disorders'
        elif any(keyword in term_lower for keyword in dermatological_keywords):
            return 'Skin and subcutaneous tissue disorders'
        elif any(keyword in term_lower for keyword in laboratory_keywords):
            return 'Investigations'
        elif any(keyword in term_lower for keyword in general_keywords):
            return 'General disorders and administration site conditions'
        else:
            return 'Other'
    
    def extract_adverse_events_from_text(self, text: str) -> List[AdverseEvent]:
        """Extract and code adverse events from text"""
        if not text:
            return []
        
        events = []
        text_lower = text.lower()
        
        # Look for common adverse event keywords
        adverse_keywords = {
            'nausea': '10027159',
            'vomiting': '10042365',
            'diarrhea': '10017916',
            'diarrhoea': '10017916',
            'rash': '10021457',
            'fatigue': '10022855',
            'cough': '10021454',
            'dyspnea': '10041144',
            'dyspnoea': '10041144',
            'headache': '10019233',
            'dizziness': '10009388',
            'fever': '10017912'
        }
        
        for keyword, meddra_code in adverse_keywords.items():
            if keyword in text_lower:
                meddra_term = self.ipf_related_codes.get(meddra_code, keyword.title())
                
                event = AdverseEvent(
                    event_code=meddra_code,
                    event_term=meddra_term,
                    severity=None,
                    frequency=None,
                    onset=None,
                    outcome=None,
                    related_to_drug=True,
                    meddra_info=MedDRATerm(
                        meddra_code=meddra_code,
                        term=meddra_term,
                        soc=self._categorize_by_soc(meddra_term),
                        hlgt='',
                        hlt='',
                        pt=meddra_term,
                        llt=meddra_term,
                        level=4
                    )
                )
                events.append(event)
        
        return events
    
    def code_adverse_events(self, events: List[str]) -> List[AdverseEvent]:
        """Code a list of adverse event terms"""
        coded_events = []
        
        for event_text in events:
            # Try to find matching MedDRA term
            meddra_terms = self.search_meddra_terms(event_text)
            
            if meddra_terms:
                meddra_term = meddra_terms[0]
            else:
                # Create a basic term from the text
                meddra_term = MedDRATerm(
                    meddra_code='',
                    term=event_text,
                    soc=self._categorize_by_soc(event_text),
                    hlgt='',
                    hlt='',
                    pt=event_text,
                    llt=event_text,
                    level=4
                )
            
            event = AdverseEvent(
                event_code=meddra_term.meddra_code,
                event_term=meddra_term.term,
                severity=None,
                frequency=None,
                onset=None,
                outcome=None,
                related_to_drug=True,
                meddra_info=meddra_term
            )
            coded_events.append(event)
        
        return coded_events
    
    def get_drug_adverse_events(self, drug_name: str) -> List[AdverseEvent]:
        """Get adverse events for a specific drug"""
        params = {
            'search': f'openfda.generic_name:"{drug_name}" OR openfda.brand_name:"{drug_name}"',
            'limit': '5'
        }
        
        data = self._make_request(self.base_url, params)
        if not data:
            return []
        
        all_events = []
        
        try:
            results = data.get('results', [])
            for result in results:
                adverse_reactions = result.get('adverse_reactions', [])
                for reaction in adverse_reactions:
                    event_term = reaction.get('reactionmeddrapt', '')
                    if event_term:
                        event = AdverseEvent(
                            event_code='',
                            event_term=event_term,
                            severity=None,
                            frequency=None,
                            onset=None,
                            outcome=None,
                            related_to_drug=True,
                            meddra_info=MedDRATerm(
                                meddra_code='',
                                term=event_term,
                                soc=self._categorize_by_soc(event_term),
                                hlgt='',
                                hlt='',
                                pt=event_term,
                                llt=event_term,
                                level=4
                            )
                        )
                        all_events.append(event)
        except Exception as e:
            logger.error(f"Error parsing drug adverse events: {e}")
        
        return all_events
    
    def analyze_adverse_event_patterns(self, trials_data: List[Dict]) -> Dict[str, Any]:
        """Analyze adverse event patterns across trials"""
        all_events = []
        
        for trial in trials_data:
            # Extract adverse events from trial data
            description = trial.get('description', '')
            eligibility = trial.get('eligibility_criteria', '')
            
            events = self.extract_adverse_events_from_text(f"{description} {eligibility}")
            all_events.extend(events)
        
        # Group by System Organ Class
        soc_counts = {}
        for event in all_events:
            if event.meddra_info:
                soc = event.meddra_info.soc
                if soc not in soc_counts:
                    soc_counts[soc] = 0
                soc_counts[soc] += 1
        
        # Most common events
        event_counts = {}
        for event in all_events:
            term = event.event_term
            if term not in event_counts:
                event_counts[term] = 0
            event_counts[term] += 1
        
        return {
            'total_events': len(all_events),
            'soc_distribution': soc_counts,
            'most_common_events': dict(sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        }

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = MedDRAClient()
    
    # Test IPF adverse events
    ipf_events = client.get_ipf_adverse_events()
    print(f"Loaded {len(ipf_events)} IPF-related adverse events")
    
    # Test adverse event extraction
    sample_text = "Patients experienced nausea, diarrhea, and rash during treatment."
    events = client.extract_adverse_events_from_text(sample_text)
    print(f"\nExtracted {len(events)} events from sample text:")
    for event in events:
        print(f"  - {event.event_term} ({event.meddra_info.soc if event.meddra_info else 'Unknown'})")
    
    # Test drug adverse events
    drug_events = client.get_drug_adverse_events("pirfenidone")
    print(f"\nFound {len(drug_events)} adverse events for pirfenidone")
    for event in drug_events[:5]:
        print(f"  - {event.event_term}")
