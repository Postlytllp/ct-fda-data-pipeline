"""
Data Processing Pipeline
Processes and enriches clinical trial data with medical library information
"""

import logging
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
import json
import pandas as pd
from datetime import datetime
import re

from data_sources.clinical_trials_gov import ClinicalTrial, ClinicalTrialsGovClient
from data_sources.eu_clinical_trials import EUClinicalTrial, EUClinicalTrialsClient
from medical_libraries.rxnorm_client import RxNormClient, DrugInfo
from medical_libraries.meddra_client import MedDRAClient, AdverseEvent
from medical_libraries.drug_harmonizer import DrugHarmonizer, HarmonizedDrugInfo

logger = logging.getLogger(__name__)

@dataclass
class EnrichedClinicalTrial:
    """Enriched clinical trial with medical library data"""
    # Basic trial information
    trial_id: str
    source: str  # 'clinicaltrials.gov' or 'euclinicaltrials.eu'
    title: str
    condition: str
    status: str
    phase: Optional[str]
    study_type: str
    sponsor: str
    start_date: Optional[str]
    completion_date: Optional[str]
    enrollment: Optional[int]
    url: str
    description: Optional[str]
    eligibility_criteria: Optional[str]
    
    # Location information
    countries: List[str]
    
    # Intervention information
    interventions: List[str]
    normalized_drugs: Dict[str, Optional[DrugInfo]]  # Legacy RxNorm only
    
    # Harmonized drug information (multi-source)
    harmonized_drugs: Dict[str, HarmonizedDrugInfo] = None
    
    # Adverse events
    adverse_events: List[AdverseEvent] = None
    meddra_codes: List[str] = None
    
    # Additional metadata
    data_collection_date: str = None
    processing_timestamp: str = None

    # Raw source record
    raw_source_record: Optional[Dict[str, Any]] = None

class DataProcessor:
    """Main data processing pipeline"""
    
    def __init__(self, use_drug_harmonization: bool = True):
        self.rxnorm_client = RxNormClient()
        self.meddra_client = MedDRAClient()
        self.use_drug_harmonization = use_drug_harmonization
        
        # Initialize drug harmonizer for multi-source drug data
        if use_drug_harmonization:
            logger.info("Initializing DrugHarmonizer with RxNorm, openFDA, and UNII")
            self.drug_harmonizer = DrugHarmonizer(
                use_rxnorm=True,
                use_openfda=True,
                use_unii=True
            )
        else:
            self.drug_harmonizer = None
        
        self.processed_trials = []
        
    def process_clinical_trials_data(self, trials: List[ClinicalTrial]) -> List[EnrichedClinicalTrial]:
        """Process ClinicalTrials.gov data"""
        logger.info(f"Processing {len(trials)} ClinicalTrials.gov trials")
        
        enriched_trials = []
        
        for i, trial in enumerate(trials):
            if i % 10 == 0:
                logger.info(f"Processing trial {i + 1}/{len(trials)}")
            
            try:
                enriched_trial = self._enrich_us_trial(trial)
                if enriched_trial:
                    enriched_trials.append(enriched_trial)
            except Exception as e:
                logger.error(f"Error processing trial {trial.nct_id}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(enriched_trials)} US trials")
        return enriched_trials
    
    def process_eu_trials_data(self, trials: List[EUClinicalTrial]) -> List[EnrichedClinicalTrial]:
        """Process EU Clinical Trials data"""
        logger.info(f"Processing {len(trials)} EU Clinical Trials")
        
        enriched_trials = []
        
        for i, trial in enumerate(trials):
            if i % 10 == 0:
                logger.info(f"Processing EU trial {i + 1}/{len(trials)}")
            
            try:
                enriched_trial = self._enrich_eu_trial(trial)
                if enriched_trial:
                    enriched_trials.append(enriched_trial)
            except Exception as e:
                logger.error(f"Error processing EU trial {trial.eudract_number}: {e}")
                continue
        
        logger.info(f"Successfully processed {len(enriched_trials)} EU trials")
        return enriched_trials
    
    def _enrich_us_trial(self, trial: ClinicalTrial) -> Optional[EnrichedClinicalTrial]:
        """Enrich US clinical trial data"""
        # Legacy RxNorm normalization (for backward compatibility)
        normalized_drugs = self.rxnorm_client.normalize_drug_names(trial.intervention_names)
        
        # Harmonize drug names using multi-source harmonization
        harmonized_drugs = {}
        if self.use_drug_harmonization and self.drug_harmonizer and trial.intervention_names:
            try:
                harmonized_drugs = self.drug_harmonizer.harmonize_drug_list(trial.intervention_names)
                logger.debug(f"Harmonized {len(harmonized_drugs)} drugs for trial {trial.nct_id}")
            except Exception as e:
                logger.warning(f"Drug harmonization failed for trial {trial.nct_id}: {e}")
        
        # Extract adverse events
        adverse_events = self._extract_adverse_events(trial)
        
        # Get MedDRA codes
        meddra_codes = list(set([event.event_code for event in adverse_events if event.event_code]))
        
        return EnrichedClinicalTrial(
            trial_id=trial.nct_id,
            source='clinicaltrials.gov',
            title=trial.title,
            condition=trial.condition,
            status=trial.status,
            phase=trial.phase,
            study_type=trial.study_type,
            sponsor=trial.sponsor,
            start_date=trial.start_date,
            completion_date=trial.completion_date,
            enrollment=trial.enrollment,
            url=trial.url,
            description=trial.description,
            eligibility_criteria=trial.eligibility_criteria,
            countries=trial.location_countries,
            interventions=trial.intervention_names,
            normalized_drugs=normalized_drugs,
            harmonized_drugs=harmonized_drugs,
            adverse_events=adverse_events,
            meddra_codes=meddra_codes,
            data_collection_date=datetime.now().strftime('%Y-%m-%d'),
            processing_timestamp=datetime.now().isoformat(),
            raw_source_record=trial.raw_study
        )
    
    def _enrich_eu_trial(self, trial: EUClinicalTrial) -> Optional[EnrichedClinicalTrial]:
        """Enrich EU clinical trial data"""
        # Use intervention_names from EU API if available, otherwise extract from text
        interventions = trial.intervention_names if trial.intervention_names else []
        
        # If no interventions found, try extracting from description as fallback
        if not interventions and trial.description:
            interventions = self._extract_interventions_from_text(trial.description)
        
        # Legacy RxNorm normalization (for backward compatibility)
        normalized_drugs = self.rxnorm_client.normalize_drug_names(interventions)
        
        # Harmonize drug names using multi-source harmonization
        harmonized_drugs = {}
        if self.use_drug_harmonization and self.drug_harmonizer and interventions:
            try:
                harmonized_drugs = self.drug_harmonizer.harmonize_drug_list(interventions)
                logger.debug(f"Harmonized {len(harmonized_drugs)} drugs for EU trial {trial.eudract_number}")
            except Exception as e:
                logger.warning(f"Drug harmonization failed for EU trial {trial.eudract_number}: {e}")
        
        # Extract adverse events
        adverse_events = self._extract_adverse_events_from_text(
            f"{trial.description} {trial.inclusion_criteria} {trial.exclusion_criteria}"
        )
        
        # Get MedDRA codes
        meddra_codes = list(set([event.event_code for event in adverse_events if event.event_code]))
        
        return EnrichedClinicalTrial(
            trial_id=trial.eudract_number,
            source='euclinicaltrials.eu',
            title=trial.title,
            condition=trial.medical_condition,
            status=trial.trial_status,
            phase=trial.trial_phase,
            study_type=trial.trial_type,
            sponsor=trial.sponsor,
            start_date=trial.start_date,
            completion_date=trial.completion_date,
            enrollment=trial.enrollment,
            url=trial.url,
            description=trial.description,
            eligibility_criteria=f"Inclusion: {trial.inclusion_criteria or ''}\nExclusion: {trial.exclusion_criteria or ''}",
            countries=trial.countries,
            interventions=interventions,
            normalized_drugs=normalized_drugs,
            harmonized_drugs=harmonized_drugs,
            adverse_events=adverse_events,
            meddra_codes=meddra_codes,
            data_collection_date=datetime.now().strftime('%Y-%m-%d'),
            processing_timestamp=datetime.now().isoformat(),
            raw_source_record=None
        )
    
    def _extract_interventions_from_text(self, text: str) -> List[str]:
        """Extract intervention names from text"""
        if not text:
            return []
        
        # Common IPF drug names
        ipf_drugs = [
            'pirfenidone', 'esbriet', 'nintedanib', 'ofev',
            'prednisone', 'azathioprine', 'cyclophosphamide',
            'mycophenolate', 'n-acetylcysteine', 'sildenafil'
        ]
        
        text_lower = text.lower()
        found_drugs = []
        
        for drug in ipf_drugs:
            if drug in text_lower:
                found_drugs.append(drug)
        
        # Also look for capitalized drug names
        capitalized_pattern = r'\b[A-Z][a-z]+(?:[A-Z][a-z]+)*\b'
        potential_drugs = re.findall(capitalized_pattern, text)
        
        for drug in potential_drugs:
            if len(drug) > 3 and drug.lower() not in [d.lower() for d in found_drugs]:
                found_drugs.append(drug)
        
        return list(set(found_drugs))
    
    def _extract_adverse_events(self, trial: Union[ClinicalTrial, EUClinicalTrial]) -> List[AdverseEvent]:
        """Extract adverse events from trial data"""
        text = f"{trial.description or ''} {getattr(trial, 'eligibility_criteria', '') or ''}"
        return self.meddra_client.extract_adverse_events_from_text(text)
    
    def _extract_adverse_events_from_text(self, text: str) -> List[AdverseEvent]:
        """Extract adverse events from text"""
        return self.meddra_client.extract_adverse_events_from_text(text)
    
    def merge_trial_data(self, us_trials: List[EnrichedClinicalTrial], 
                        eu_trials: List[EnrichedClinicalTrial]) -> List[EnrichedClinicalTrial]:
        """Merge US and EU trial data"""
        all_trials = us_trials + eu_trials
        
        # Remove duplicates based on title similarity
        unique_trials = []
        seen_titles = set()
        
        for trial in all_trials:
            title_normalized = re.sub(r'\s+', ' ', trial.title.lower()).strip()
            if title_normalized not in seen_titles:
                seen_titles.add(title_normalized)
                unique_trials.append(trial)
        
        logger.info(f"Merged {len(all_trials)} trials into {len(unique_trials)} unique trials")
        return unique_trials
    
    def generate_summary_statistics(self, trials: List[EnrichedClinicalTrial]) -> Dict[str, Any]:
        """Generate summary statistics for processed trials"""
        if not trials:
            return {}
        
        # Basic counts
        total_trials = len(trials)
        us_trials = len([t for t in trials if t.source == 'clinicaltrials.gov'])
        eu_trials = len([t for t in trials if t.source == 'euclinicaltrials.eu'])
        
        # Status distribution
        status_counts = {}
        for trial in trials:
            status = trial.status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Phase distribution
        phase_counts = {}
        for trial in trials:
            phase = trial.phase or 'Unknown'
            phase_counts[phase] = phase_counts.get(phase, 0) + 1
        
        # Country distribution
        all_countries = []
        for trial in trials:
            all_countries.extend(trial.countries)
        country_counts = {}
        for country in all_countries:
            country_counts[country] = country_counts.get(country, 0) + 1
        
        # Drug analysis
        all_drugs = []
        for trial in trials:
            all_drugs.extend(trial.interventions)
        drug_counts = {}
        for drug in all_drugs:
            drug_counts[drug] = drug_counts.get(drug, 0) + 1
        
        # Adverse events analysis
        all_adverse_events = []
        for trial in trials:
            all_adverse_events.extend(trial.adverse_events)
        
        meddra_analysis = self.meddra_client.analyze_adverse_event_patterns(
            [asdict(trial) for trial in trials]
        )
        
        # Enrollment statistics
        enrollments = [t.enrollment for t in trials if t.enrollment]
        total_enrollment = sum(enrollments) if enrollments else 0
        avg_enrollment = total_enrollment / len(enrollments) if enrollments else 0
        
        return {
            'total_trials': total_trials,
            'us_trials': us_trials,
            'eu_trials': eu_trials,
            'status_distribution': status_counts,
            'phase_distribution': phase_counts,
            'country_distribution': dict(sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'drug_distribution': dict(sorted(drug_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'adverse_events_analysis': meddra_analysis,
            'enrollment_stats': {
                'total_enrollment': total_enrollment,
                'average_enrollment': avg_enrollment,
                'trials_with_enrollment_data': len(enrollments)
            },
            'data_collection_date': datetime.now().strftime('%Y-%m-%d')
        }
    
    def export_to_dataframe(self, trials: List[EnrichedClinicalTrial]) -> pd.DataFrame:
        """Export trials to pandas DataFrame"""
        data = []
        
        for trial in trials:
            row = {
                'trial_id': trial.trial_id,
                'source': trial.source,
                'title': trial.title,
                'condition': trial.condition,
                'status': trial.status,
                'phase': trial.phase,
                'study_type': trial.study_type,
                'sponsor': trial.sponsor,
                'start_date': trial.start_date,
                'completion_date': trial.completion_date,
                'enrollment': trial.enrollment,
                'url': trial.url,
                'countries': ', '.join(trial.countries),
                'interventions': ', '.join(trial.interventions),
                'normalized_drug_count': len([d for d in trial.normalized_drugs.values() if d is not None]),
                'harmonized_drug_count': len(trial.harmonized_drugs) if trial.harmonized_drugs else 0,
                'adverse_event_count': len(trial.adverse_events),
                'meddra_codes': ', '.join(trial.meddra_codes),
                'data_collection_date': trial.data_collection_date
            }
            data.append(row)
        
        return pd.DataFrame(data)
    
    def export_detailed_data(self, trials: List[EnrichedClinicalTrial], output_file: str):
        """Export detailed trial data to JSON"""
        detailed_data = []
        
        for trial in trials:
            # Manually convert to dict to handle nested dataclasses
            trial_dict = {
                'trial_id': trial.trial_id,
                'source': trial.source,
                'title': trial.title,
                'condition': trial.condition,
                'status': trial.status,
                'phase': trial.phase,
                'study_type': trial.study_type,
                'sponsor': trial.sponsor,
                'start_date': trial.start_date,
                'completion_date': trial.completion_date,
                'enrollment': trial.enrollment,
                'url': trial.url,
                'description': trial.description,
                'eligibility_criteria': trial.eligibility_criteria,
                'countries': trial.countries,
                'interventions': trial.interventions,
                'raw_source_record': trial.raw_source_record,
                'normalized_drugs': {
                    name: asdict(drug) if drug else None
                    for name, drug in trial.normalized_drugs.items()
                },
                'harmonized_drugs': {
                    name: {
                        'harmonized_drug_name': drug.harmonized_drug_name,
                        'harmonized_generic_name': drug.harmonized_generic_name,
                        'harmonized_brand_name': drug.harmonized_brand_name,
                        'strength': drug.strength,
                        'route': drug.route,
                        'dosage_form': drug.dosage_form,
                        'rxcui': drug.rxcui,
                        'atc_codes': drug.atc_codes,
                        'unii_codes': drug.unii_codes,
                        'ndc_codes': drug.ndc_codes,
                        'confidence_score': drug.confidence_score,
                        'sources_used': drug.sources_used
                    }
                    for name, drug in (trial.harmonized_drugs or {}).items()
                },
                'adverse_events': [
                    {
                        'event_code': event.event_code,
                        'event_term': event.event_term,
                        'severity': event.severity,
                        'frequency': event.frequency,
                        'onset': event.onset,
                        'outcome': event.outcome,
                        'related_to_drug': event.related_to_drug,
                        'meddra_info': asdict(event.meddra_info) if event.meddra_info else None
                    }
                    for event in trial.adverse_events
                ],
                'meddra_codes': trial.meddra_codes,
                'data_collection_date': trial.data_collection_date,
                'processing_timestamp': trial.processing_timestamp
            }
            
            detailed_data.append(trial_dict)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(detailed_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported detailed data to {output_file}")
    
    def export_harmonized_drugs(self, trials: List[EnrichedClinicalTrial], output_file: str):
        """Export harmonized drug information to JSON file"""
        if not self.use_drug_harmonization:
            logger.warning("Drug harmonization is disabled, skipping harmonized drugs export")
            return
        
        harmonized_export = {}
        
        for trial in trials:
            if trial.harmonized_drugs:
                trial_drugs = {}
                for drug_name, drug_info in trial.harmonized_drugs.items():
                    trial_drugs[drug_name] = {
                        'harmonized_drug_name': drug_info.harmonized_drug_name,
                        'harmonized_generic_name': drug_info.harmonized_generic_name,
                        'harmonized_brand_name': drug_info.harmonized_brand_name,
                        'strength': drug_info.strength,
                        'route': drug_info.route,
                        'dosage_form': drug_info.dosage_form,
                        'dose': drug_info.dose,
                        'rxcui': drug_info.rxcui,
                        'atc_codes': drug_info.atc_codes,
                        'unii_codes': drug_info.unii_codes,
                        'ndc_codes': drug_info.ndc_codes[:5] if drug_info.ndc_codes else [],  # Limit NDC codes
                        'brand_names': list(drug_info.all_brand_names),
                        'generic_names': list(drug_info.all_generic_names),
                        'confidence_score': drug_info.confidence_score,
                        'sources_used': drug_info.sources_used
                    }
                
                harmonized_export[trial.trial_id] = {
                    'trial_title': trial.title,
                    'source': trial.source,
                    'drugs': trial_drugs
                }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(harmonized_export, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported harmonized drugs for {len(harmonized_export)} trials to {output_file}")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    processor = DataProcessor()
    
    # This would be used with actual data from the clients
    print("Data processor initialized successfully")
