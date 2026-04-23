"""
ClinicalTrials.gov API Client
Fetches clinical trial data for IPF (Idiopathic Pulmonary Fibrosis)
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from urllib.parse import urlencode
import json

logger = logging.getLogger(__name__)

@dataclass
class ClinicalTrial:
    """Data class for clinical trial information"""
    nct_id: str
    title: str
    condition: str
    status: str
    phase: Optional[str]
    study_type: str
    sponsor: str
    start_date: Optional[str]
    completion_date: Optional[str]
    enrollment: Optional[int]
    intervention_names: List[str]
    location_countries: List[str]
    url: str
    description: Optional[str]
    eligibility_criteria: Optional[str]
    raw_study: Optional[Dict[str, Any]] = None

class ClinicalTrialsGovClient:
    """Client for fetching data from ClinicalTrials.gov API"""
    
    def __init__(self, base_url: str = "https://clinicaltrials.gov/api/v2/studies", 
                 request_delay: float = 1.0, max_retries: int = 3):
        self.base_url = base_url
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Clinical-Trials-Data-Pipeline/1.0'
        })
    
    def _make_request(self, url: str, params: Dict[str, Any]) -> Optional[Dict]:
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
    
    def search_ipf_trials(self, limit: int = 1000) -> List[ClinicalTrial]:
        """Search for IPF clinical trials (interventional only)"""
        trials = []
        
        # Search query for IPF using API v2 - filter for interventional trials only
        query_params = {
            'query.cond': 'Idiopathic Pulmonary Fibrosis',
            'query.term': 'AREA[StudyType]Interventional',
            'pageSize': min(limit, 1000),
            'format': 'json'
        }
        
        logger.info(f"Searching for IPF trials with limit {limit}")
        
        data = self._make_request(self.base_url, query_params)
        if not data:
            logger.error("Failed to fetch initial search results")
            return trials
        
        # Extract study information from v2 API response
        studies = data.get('studies', [])
        
        for study in studies:
            try:
                trial = self._parse_study_v2(study)
                if trial:
                    trials.append(trial)
            except Exception as e:
                logger.error(f"Error parsing study: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(trials)} IPF trials")
        return trials
    
    def _parse_study_v2(self, study: Dict) -> Optional[ClinicalTrial]:
        """Parse study data from API v2 format"""
        try:
            protocol_section = study.get('protocolSection', {})
            identification_module = protocol_section.get('identificationModule', {})
            status_module = protocol_section.get('statusModule', {})
            design_module = protocol_section.get('designModule', {})
            arms_interventions_module = protocol_section.get('armsInterventionsModule', {})
            contacts_locations_module = protocol_section.get('contactsLocationsModule', {})
            description_module = protocol_section.get('descriptionModule', {})
            eligibility_module = protocol_section.get('eligibilityModule', {})
            
            # Extract intervention names
            interventions = []
            interventions_list = arms_interventions_module.get('interventions', [])
            for intervention in interventions_list:
                intervention_name = intervention.get('name', '')
                if intervention_name:
                    interventions.append(intervention_name)
            
            # Extract countries
            countries = []
            locations = contacts_locations_module.get('locations', [])
            for location in locations:
                country = location.get('country', '')
                if country and country not in countries:
                    countries.append(country)
            
            # Extract conditions
            conditions_module = protocol_section.get('conditionsModule', {})
            conditions = conditions_module.get('conditions', [])
            condition_str = ', '.join(conditions) if conditions else ''
            
            # Extract phases
            phases = design_module.get('phases', [])
            phase = phases[0] if phases else None
            
            # Extract enrollment
            enrollment_info = design_module.get('enrollmentInfo', {})
            enrollment = enrollment_info.get('count') if enrollment_info else None
            
            return ClinicalTrial(
                nct_id=identification_module.get('nctId', ''),
                title=identification_module.get('officialTitle', identification_module.get('briefTitle', '')),
                condition=condition_str,
                status=status_module.get('overallStatus', ''),
                phase=phase,
                study_type=design_module.get('studyType', ''),
                sponsor=protocol_section.get('sponsorCollaboratorsModule', {}).get('leadSponsor', {}).get('name', ''),
                start_date=status_module.get('startDateStruct', {}).get('date', ''),
                completion_date=status_module.get('completionDateStruct', {}).get('date', ''),
                enrollment=enrollment,
                intervention_names=interventions,
                location_countries=countries,
                url=f"https://clinicaltrials.gov/study/{identification_module.get('nctId', '')}",
                description=description_module.get('briefSummary', ''),
                eligibility_criteria=eligibility_module.get('eligibilityCriteria', ''),
                raw_study=study
            )
            
        except Exception as e:
            logger.error(f"Error parsing study data: {e}")
            return None
    
    def _parse_study(self, study: Dict) -> Optional[ClinicalTrial]:
        """Parse individual study data"""
        try:
            protocol_section = study.get('ProtocolSection', {})
            identification_module = protocol_section.get('IdentificationModule', {})
            status_module = protocol_section.get('StatusModule', {})
            design_module = protocol_section.get('DesignModule', {})
            arms_interventions_module = protocol_section.get('ArmsInterventionsModule', {})
            contacts_locations_module = protocol_section.get('ContactsLocationsModule', {})
            
            # Description section
            description_section = study.get('DescriptionSection', {})
            brief_summary = description_section.get('BriefSummary', {})
            eligibility_module = description_section.get('EligibilityModule', {})
            
            # Extract intervention names
            interventions = []
            for arm in arms_interventions_module.get('ArmGroupList', {}).get('ArmGroup', []):
                for intervention in arm.get('InterventionList', {}).get('Intervention', []):
                    intervention_name = intervention.get('Name', '')
                    if intervention_name:
                        interventions.append(intervention_name)
            
            # Extract countries
            countries = []
            for location in contacts_locations_module.get('LocationList', {}).get('Location', []):
                geo_point = location.get('GeoPoint', {})
                country = geo_point.get('Country', '')
                if country and country not in countries:
                    countries.append(country)
            
            return ClinicalTrial(
                nct_id=identification_module.get('NCTId', ''),
                title=identification_module.get('OfficialTitle', ''),
                condition=self._extract_condition(protocol_section),
                status=status_module.get('OverallStatus', ''),
                phase=design_module.get('PhaseList', {}).get('Phase', [None])[0],
                study_type=design_module.get('StudyType', ''),
                sponsor=protocol_section.get('SponsorCollaboratorsModule', {}).get('LeadSponsor', {}).get('LeadSponsorName', ''),
                start_date=status_module.get('StartDateStruct', {}).get('StartDate', ''),
                completion_date=status_module.get('CompletionDateStruct', {}).get('CompletionDate', ''),
                enrollment=self._parse_enrollment(status_module.get('StudyFirstPostDateStruct', {})),
                intervention_names=interventions,
                location_countries=countries,
                url=f"https://clinicaltrials.gov/study/{identification_module.get('NCTId', '')}",
                description=brief_summary.get('Textblock', ''),
                eligibility_criteria=eligibility_module.get('EligibilityCriteria', {}).get('Textblock', ''),
                raw_study=study
            )
            
        except Exception as e:
            logger.error(f"Error parsing study data: {e}")
            return None
    
    def _extract_condition(self, protocol_section: Dict) -> str:
        """Extract condition from protocol section"""
        condition_list = protocol_section.get('ConditionModule', {}).get('ConditionList', {}).get('Condition', [])
        if condition_list and isinstance(condition_list, list):
            return ', '.join(condition_list)
        return condition_list if isinstance(condition_list, str) else ''
    
    def _parse_enrollment(self, enrollment_data: Dict) -> Optional[int]:
        """Parse enrollment count"""
        try:
            enrollment = enrollment_data.get('EnrollmentCount', {})
            if isinstance(enrollment, dict):
                return enrollment.get('Value')
            return enrollment
        except:
            return None
    
    def get_trial_details(self, nct_id: str) -> Optional[ClinicalTrial]:
        """Get detailed information for a specific trial"""
        url = f"https://clinicaltrials.gov/api/query/full_studies"
        params = {
            'fmt': 'json',
            'expr': f'{nct_id}[NCTId]'
        }
        
        data = self._make_request(url, params)
        if not data:
            return None
        
        studies = data.get('FullStudiesResponse', {}).get('FullStudies', [])
        if studies:
            return self._parse_study(studies[0].get('Study', {}))
        
        return None

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = ClinicalTrialsGovClient()
    trials = client.search_ipf_trials(limit=10)
    
    for trial in trials[:3]:
        print(f"NCT ID: {trial.nct_id}")
        print(f"Title: {trial.title}")
        print(f"Status: {trial.status}")
        print(f"Phase: {trial.phase}")
        print("-" * 50)
