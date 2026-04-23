"""
EU Clinical Trials API Client
Fetches clinical trial data from https://euclinicaltrials.eu CTIS JSON API for IPF
"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class EUClinicalTrial:
    """Data class for EU clinical trial information"""
    eudract_number: str
    title: str
    medical_condition: str
    trial_status: str
    trial_phase: Optional[str]
    trial_type: str
    sponsor: str
    start_date: Optional[str]
    completion_date: Optional[str]
    enrollment: Optional[int]
    therapeutic_area: str
    countries: List[str]
    url: str
    description: Optional[str]
    inclusion_criteria: Optional[str]
    exclusion_criteria: Optional[str]
    intervention_names: List[str] = None  # Drug/intervention names
    raw_trial: Optional[Dict[str, Any]] = None

class EUClinicalTrialsClient:
    """Client for fetching data from EU Clinical Trials CTIS JSON API"""
    
    def __init__(self, base_url: str = "https://euclinicaltrials.eu", 
                 request_delay: float = 1.5, max_retries: int = 3):
        self.base_url = base_url
        self.api_search_url = f"{base_url}/ctis-public-api/search"
        self.api_retrieve_url = f"{base_url}/ctis-public-api/retrieve"
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Clinical-Trials-Data-Pipeline/1.0',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def _make_post_request(self, url: str, payload: Dict) -> Optional[Dict]:
        """Make HTTP POST request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(url, json=payload, timeout=30)
                response.raise_for_status()
                
                # Rate limiting
                time.sleep(self.request_delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"POST request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to POST data after {self.max_retries} attempts")
                    return None
                time.sleep(2 ** attempt)
        
        return None
    
    def _make_get_request(self, url: str) -> Optional[Dict]:
        """Make HTTP GET request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Rate limiting
                time.sleep(self.request_delay)
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"GET request attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to GET data after {self.max_retries} attempts")
                    return None
                time.sleep(2 ** attempt)
        
        return None
    
    def search_ipf_trials(self, max_pages: int = 10) -> List[EUClinicalTrial]:
        """Search for IPF clinical trials using CTIS API (interventional only)"""
        trials = []
        
        logger.info("Searching EU Clinical Trials CTIS API for IPF studies (interventional only)")
        
        # Search payload for IPF trials - filter for interventional trials only (category code 2)
        search_payload = {
            "pagination": {
                "page": 1,
                "size": 100
            },
            "sort": {
                "property": "decisionDate",
                "direction": "DESC"
            },
            "searchCriteria": {
                "containAll": None,
                "containAny": None,
                "containNot": None,
                "title": None,
                "number": None,
                "status": None,
                "medicalCondition": "Idiopathic Pulmonary Fibrosis",  # IPF search
                "sponsor": None,
                "endPoint": None,
                "productName": None,
                "productRole": None,
                "populationType": None,
                "orphanDesignation": None,
                "msc": None,
                "ageGroupCode": None,
                "therapeuticAreaCode": None,
                "trialPhaseCode": None,
                "trialCategoryCode": "2",  # Filter for interventional trials only (2 = Interventional)
                "sponsorTypeCode": None,
                "gender": None,
                "protocolCode": None,
                "rareDisease": None,
                "pip": None,
                "haveOrphanDesignation": None,
                "hasStudyResults": None,
                "hasClinicalStudyReport": None,
                "isLowIntervention": None,
                "hasSeriousBreach": None,
                "hasUnexpectedEvent": None,
                "hasUrgentSafetyMeasure": None,
                "isTransitioned": None,
                "eudraCtCode": None,
                "trialRegion": None,
                "vulnerablePopulation": None,
                "mscStatus": None
            }
        }
        
        # Paginate through results
        page = 1
        has_next_page = True
        
        while has_next_page and page <= max_pages:
            search_payload["pagination"]["page"] = page
            
            logger.info(f"Fetching page {page} of EU Clinical Trials")
            response_data = self._make_post_request(self.api_search_url, search_payload)
            
            if not response_data:
                logger.error(f"Failed to fetch page {page}")
                break
            
            # Extract trial overview data
            trial_overviews = response_data.get('data', [])
            logger.info(f"Found {len(trial_overviews)} trials on page {page}")
            
            # Fetch full details for each trial
            for trial_overview in trial_overviews:
                ct_number = trial_overview.get('ctNumber')
                if not ct_number:
                    continue
                
                full_trial = self._fetch_full_trial(ct_number)
                if full_trial:
                    trials.append(full_trial)
            
            # Check pagination
            pagination = response_data.get('pagination', {})
            has_next_page = pagination.get('nextPage', False)
            page += 1
        
        # Post-process filter: ensure all trials are interventional
        interventional_trials = [trial for trial in trials if trial.trial_type == 'Interventional']
        observational_count = len(trials) - len(interventional_trials)
        
        if observational_count > 0:
            logger.warning(f"Filtered out {observational_count} observational trial(s) that passed API filter")
        
        logger.info(f"Successfully fetched {len(interventional_trials)} interventional IPF EU trials")
        return interventional_trials
    
    def _fetch_full_trial(self, ct_number: str) -> Optional[EUClinicalTrial]:
        """Fetch full trial details using CTIS API"""
        full_trial_url = f"{self.api_retrieve_url}/{ct_number}"
        
        logger.debug(f"Fetching full trial details for {ct_number}")
        trial_data = self._make_get_request(full_trial_url)
        
        if not trial_data:
            logger.warning(f"Failed to fetch full trial data for {ct_number}")
            return None
        
        try:
            return self._parse_full_trial(trial_data)
        except Exception as e:
            logger.error(f"Error parsing full trial {ct_number}: {e}")
            return None
    
    def _parse_full_trial(self, trial_data: Dict) -> Optional[EUClinicalTrial]:
        """Parse full trial data from CTIS API response"""
        try:
            ct_number = trial_data.get('ctNumber', '')
            ct_status = trial_data.get('ctStatus', '')
            
            # Log available keys for debugging
            logger.debug(f"Trial data keys for {ct_number}: {list(trial_data.keys())}")
            
            # Get authorized application data - try multiple possible locations
            auth_app = trial_data.get('authorizedApplication', {})
            if not auth_app:
                auth_app = trial_data.get('application', {})
            
            auth_part1 = auth_app.get('authorizedPartI', {})
            if not auth_part1:
                auth_part1 = auth_app.get('partI', {})
            
            logger.debug(f"Auth Part I keys for {ct_number}: {list(auth_part1.keys()) if auth_part1 else 'None'}")
            
            # Extract trial details section (main data location)
            trial_details = auth_part1.get('trialDetails', {})
            logger.debug(f"Trial details keys: {list(trial_details.keys()) if trial_details else 'None'}")
            
            # Extract title from clinicalTrialIdentifiers
            clinical_ids = trial_details.get('clinicalTrialIdentifiers', {})
            title = (clinical_ids.get('fullTitle') or 
                    clinical_ids.get('publicTitle') or 
                    clinical_ids.get('shortTitle') or '')
            
            # Extract medical condition from trialInformation
            trial_info = trial_details.get('trialInformation', {})
            medical_condition_data = trial_info.get('medicalCondition', {})
            part1_conditions = medical_condition_data.get('partIMedicalConditions', [])
            if part1_conditions and isinstance(part1_conditions, list):
                medical_condition = part1_conditions[0].get('medicalCondition', '') if part1_conditions[0] else ''
            else:
                medical_condition = ''
            
            # Extract sponsor info from sponsors array
            sponsors_list = auth_part1.get('sponsors', [])
            sponsor = 'Not specified'
            if sponsors_list and isinstance(sponsors_list, list):
                for sponsor_data in sponsors_list:
                    if isinstance(sponsor_data, dict):
                        # Try to get organization data
                        org = sponsor_data.get('organisation', {}) or sponsor_data.get('organization', {})
                        if org:
                            sponsor = (org.get('organisationFullName') or 
                                     org.get('name') or 
                                     org.get('organisationName') or '')
                        # Fallback to direct fields
                        if not sponsor:
                            sponsor = (sponsor_data.get('organisationName') or 
                                     sponsor_data.get('name') or '')
                        if sponsor:
                            break
            
            # Extract phase from trialCategory
            trial_category = trial_info.get('trialCategory', {})
            phase_code = trial_category.get('trialPhase')
            phase_map = {
                '1': 'Phase I',
                '2': 'Phase II',
                '3': 'Phase III',
                '4': 'Phase IV',
                '5': 'Phase I/II',
                '6': 'Phase II/III'
            }
            trial_phase = phase_map.get(str(phase_code), None)
            
            # Extract study type from trial category
            trial_category_code = trial_category.get('trialCategory')
            trial_type = 'Interventional' if trial_category_code == '2' else 'Observational'
            
            # Extract therapeutic area
            therapeutic_areas = auth_part1.get('therapeuticAreas', []) or auth_part1.get('partOneTherapeuticAreas', [])
            if therapeutic_areas and isinstance(therapeutic_areas, list):
                therapeutic_area = str(therapeutic_areas[0]) if therapeutic_areas[0] else ''
            else:
                therapeutic_area = ''
            
            # Extract countries from rowCountriesInfo
            countries_info = auth_part1.get('rowCountriesInfo', [])
            countries = []
            if countries_info and isinstance(countries_info, list):
                for country_data in countries_info:
                    if isinstance(country_data, dict):
                        country_name = country_data.get('name', '')
                        if country_name:
                            countries.append(country_name)
            
            # Extract enrollment from rowSubjectCount
            enrollment = auth_part1.get('rowSubjectCount', None)
            if enrollment and not isinstance(enrollment, int):
                enrollment = self._parse_enrollment(enrollment)
            
            # Extract dates
            decision_date = trial_data.get('decisionDate', '')
            start_date = decision_date.split('T')[0] if decision_date else None
            
            # Extract description from trial objective (nested in trialInformation)
            trial_objective = trial_info.get('trialObjective', {})
            main_objective = trial_objective.get('mainObjective', '')
            secondary_objectives = trial_objective.get('secondaryObjectives', [])
            
            description = main_objective
            if secondary_objectives and isinstance(secondary_objectives, list):
                sec_obj_texts = []
                for obj in secondary_objectives:
                    if isinstance(obj, dict):
                        obj_text = obj.get('secondaryObjective', '') or obj.get('objective', '')
                        if obj_text:
                            sec_obj_texts.append(obj_text)
                if sec_obj_texts:
                    description = f"{main_objective}; Secondary: {'; '.join(sec_obj_texts)}"
            
            # Extract inclusion/exclusion criteria (nested in trialInformation)
            eligibility_criteria = trial_info.get('eligibilityCriteria', {})
            
            # Extract principal inclusion criteria
            principal_inclusion = eligibility_criteria.get('principalInclusionCriteria', [])
            inclusion_list = []
            if principal_inclusion and isinstance(principal_inclusion, list):
                for criterion in principal_inclusion:
                    if isinstance(criterion, dict):
                        crit_text = criterion.get('principalInclusionCriteria', '')
                        if crit_text:
                            inclusion_list.append(crit_text)
            inclusion_criteria = '\n'.join(inclusion_list) if inclusion_list else ''
            
            # Extract principal exclusion criteria
            principal_exclusion = eligibility_criteria.get('principalExclusionCriteria', [])
            exclusion_list = []
            if principal_exclusion and isinstance(principal_exclusion, list):
                for criterion in principal_exclusion:
                    if isinstance(criterion, dict):
                        crit_text = criterion.get('principalExclusionCriteria', '')
                        if crit_text:
                            exclusion_list.append(crit_text)
            exclusion_criteria = '\n'.join(exclusion_list) if exclusion_list else ''
            
            # Extract intervention/drug information
            intervention_names = self._extract_interventions(auth_part1, trial_data)
            
            return EUClinicalTrial(
                eudract_number=ct_number,
                title=title or f"EU Clinical Trial {ct_number}",
                medical_condition=medical_condition or 'Not specified',
                trial_status=ct_status,
                trial_phase=trial_phase,
                trial_type=trial_type,
                sponsor=sponsor or 'Not specified',
                start_date=start_date,
                completion_date=None,  # Not typically in initial authorization
                enrollment=enrollment,
                therapeutic_area=therapeutic_area,
                countries=countries,
                url=f"{self.base_url}/ctis-public/view/{ct_number}",
                description=description,
                inclusion_criteria=inclusion_criteria,
                exclusion_criteria=exclusion_criteria,
                intervention_names=intervention_names,
                raw_trial=trial_data
            )
            
        except Exception as e:
            logger.error(f"Error parsing trial data: {e}")
            return None
    
    def _extract_interventions(self, auth_part1: Dict, trial_data: Dict) -> List[str]:
        """Extract intervention/drug names from CTIS API response"""
        interventions = []
        
        try:
            logger.debug(f"Extracting interventions. Auth Part I keys: {list(auth_part1.keys()) if auth_part1 else 'None'}")
            
            # Extract from products array (main location for interventions)
            imps = auth_part1.get('products', [])
            
            if imps and isinstance(imps, list):
                logger.debug(f"Found {len(imps)} investigational medicinal products")
                for imp in imps:
                    if isinstance(imp, dict):
                        # Extract from productDictionaryInfo first
                        product_dict = imp.get('productDictionaryInfo', {})
                        drug_name = None
                        
                        if product_dict:
                            drug_name = (product_dict.get('prodName') or 
                                       product_dict.get('productName') or
                                       product_dict.get('activeSubstanceName') or
                                       product_dict.get('sponsorProductCode'))
                        
                        # Fallback to direct product fields
                        if not drug_name:
                            drug_name = (imp.get('productName') or 
                                       imp.get('name') or
                                       imp.get('tradeName'))
                        
                        if drug_name and isinstance(drug_name, str):
                            interventions.append(drug_name.strip())
                            logger.debug(f"Found drug name: {drug_name}")
            
            # Try to extract from interventions field - multiple locations
            trial_interventions = (auth_part1.get('interventions') or 
                                 auth_part1.get('trialInterventions') or 
                                 auth_part1.get('studyInterventions') or [])
            
            if trial_interventions and isinstance(trial_interventions, list):
                logger.debug(f"Found {len(trial_interventions)} interventions")
                for intervention in trial_interventions:
                    if isinstance(intervention, dict):
                        name = (intervention.get('interventionName') or 
                               intervention.get('name') or
                               intervention.get('description') or
                               intervention.get('productName'))
                        if name:
                            interventions.append(str(name).strip())
                            logger.debug(f"Found intervention: {name}")
                    elif isinstance(intervention, str):
                        interventions.append(intervention.strip())
            
            # Try to extract from treatment arms
            arms = (auth_part1.get('treatmentArms') or 
                   auth_part1.get('arms') or 
                   auth_part1.get('studyArms') or [])
            
            if arms and isinstance(arms, list):
                logger.debug(f"Found {len(arms)} treatment arms")
                for arm in arms:
                    if isinstance(arm, dict):
                        arm_name = (arm.get('armName') or 
                                  arm.get('name') or 
                                  arm.get('description') or
                                  arm.get('interventionDescription'))
                        if arm_name:
                            # Extract drug names from arm description
                            import re
                            words = re.findall(r'\b[A-Z][a-z]+(?:[A-Z][a-z]+)*\b', str(arm_name))
                            interventions.extend(words)
                            logger.debug(f"Found arm: {arm_name}")
            
            # Try to extract from trial design section
            trial_design = auth_part1.get('trialDesign', {})
            if trial_design:
                design_interventions = (trial_design.get('interventions') or 
                                       trial_design.get('products') or [])
                if design_interventions:
                    for interv in design_interventions:
                        if isinstance(interv, dict):
                            name = interv.get('name') or interv.get('productName')
                            if name:
                                interventions.append(str(name).strip())
                        elif isinstance(interv, str):
                            interventions.append(interv.strip())
            
            # Extract from title or description if nothing found
            if not interventions:
                logger.debug("No interventions found, trying title/description")
                title = (auth_part1.get('trialMainObjective', '') or 
                        auth_part1.get('briefTitle', '') or
                        auth_part1.get('publicTitle', ''))
                if title:
                    import re
                    # Common IPF drugs to look for
                    ipf_keywords = [
                        'pirfenidone', 'esbriet', 'nintedanib', 'ofev',
                        'prednisone', 'azathioprine', 'cyclophosphamide',
                        'mycophenolate', 'n-acetylcysteine', 'sildenafil',
                        'acetylcysteine', 'prednisolone'
                    ]
                    title_lower = title.lower()
                    for keyword in ipf_keywords:
                        if keyword in title_lower:
                            interventions.append(keyword)
                            logger.debug(f"Found keyword in title: {keyword}")
            
            # Deduplicate and clean
            interventions = list(set([i for i in interventions if i and len(i) > 2]))
            logger.debug(f"Final interventions list: {interventions}")
            
        except Exception as e:
            logger.warning(f"Error extracting interventions: {e}")
        
        return interventions
    
    def _parse_enrollment(self, enrollment_text: Any) -> Optional[int]:
        """Parse enrollment number from text"""
        if isinstance(enrollment_text, int):
            return enrollment_text
        
        if not enrollment_text:
            return None
        
        try:
            # Extract numbers from string
            import re
            numbers = re.findall(r'\d+', str(enrollment_text))
            if numbers:
                return int(numbers[0])
        except:
            pass
        
        return None

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    client = EUClinicalTrialsClient()
    trials = client.search_ipf_trials(max_pages=2)
    
    for trial in trials[:3]:
        print(f"EUCT Number: {trial.eudract_number}")
        print(f"Title: {trial.title}")
        print(f"Status: {trial.trial_status}")
        print(f"Phase: {trial.trial_phase}")
        print(f"Countries: {', '.join(trial.countries)}")
        print("-" * 50)
