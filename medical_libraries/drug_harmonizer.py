"""
Unified Drug Harmonization System
Combines RxNorm, openFDA, and UNII for comprehensive drug name harmonization
"""

import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
import json

from medical_libraries.rxnorm_client import RxNormClient, DrugInfo as RxNormDrugInfo
from medical_libraries.openfda_client import OpenFDAClient, FDADrugInfo
from medical_libraries.unii_client import UNIIClient, UNIISubstance

logger = logging.getLogger(__name__)

@dataclass
class HarmonizedDrugInfo:
    """Comprehensive harmonized drug information from multiple sources"""
    query_name: str
    
    # RxNorm data
    rxcui: Optional[str] = None
    rxnorm_name: Optional[str] = None
    rxnorm_brand_names: List[str] = field(default_factory=list)
    rxnorm_generic_name: Optional[str] = None
    rxnorm_ingredients: List[str] = field(default_factory=list)
    rxnorm_synonyms: List[str] = field(default_factory=list)
    
    # FDA data
    fda_application_numbers: List[str] = field(default_factory=list)
    fda_brand_names: List[str] = field(default_factory=list)
    fda_generic_names: List[str] = field(default_factory=list)
    fda_manufacturers: List[str] = field(default_factory=list)
    ndc_codes: List[str] = field(default_factory=list)
    
    # UNII data
    unii_codes: List[str] = field(default_factory=list)
    cas_numbers: List[str] = field(default_factory=list)
    molecular_formulas: List[str] = field(default_factory=list)
    
    # ATC codes
    atc_codes: List[str] = field(default_factory=list)
    
    # Consolidated information
    all_brand_names: Set[str] = field(default_factory=set)
    all_generic_names: Set[str] = field(default_factory=set)
    all_synonyms: Set[str] = field(default_factory=set)
    
    # Harmonized fields for standardized drug information
    harmonized_drug_name: Optional[str] = None  # Primary standardized name
    harmonized_generic_name: Optional[str] = None  # Primary generic name
    harmonized_brand_name: Optional[str] = None  # Primary brand name
    strength: List[str] = field(default_factory=list)  # Drug strengths (e.g., "500 mg")
    route: List[str] = field(default_factory=list)  # Routes of administration (oral, IV, etc.)
    dosage_form: List[str] = field(default_factory=list)  # Dosage forms (tablet, capsule, etc.)
    dose: List[str] = field(default_factory=list)  # Typical dose information
    administration_details: List[str] = field(default_factory=list)  # Administration instructions
    
    # Metadata
    sources_used: List[str] = field(default_factory=list)
    confidence_score: float = 0.0

class DrugHarmonizer:
    """
    Unified drug harmonization system that combines multiple authoritative sources:
    - RxNorm: NLM's normalized drug naming system
    - openFDA: FDA's drug information database
    - UNII: FDA's unique ingredient identifier registry
    """
    
    def __init__(self, use_rxnorm: bool = True, use_openfda: bool = True, use_unii: bool = True):
        self.use_rxnorm = use_rxnorm
        self.use_openfda = use_openfda
        self.use_unii = use_unii
        
        # Initialize clients
        self.rxnorm_client = RxNormClient() if use_rxnorm else None
        self.fda_client = OpenFDAClient() if use_openfda else None
        self.unii_client = UNIIClient() if use_unii else None
        
        logger.info(f"DrugHarmonizer initialized with: RxNorm={use_rxnorm}, openFDA={use_openfda}, UNII={use_unii}")
    
    def harmonize_drug(self, drug_name: str) -> Optional[HarmonizedDrugInfo]:
        """
        Harmonize a single drug name across all available sources
        
        Args:
            drug_name: The drug name to harmonize
            
        Returns:
            HarmonizedDrugInfo object with consolidated information from all sources
        """
        if not drug_name or drug_name.lower() in ['placebo', 'control']:
            return None
        
        logger.info(f"Harmonizing drug: {drug_name}")
        
        harmonized = HarmonizedDrugInfo(query_name=drug_name)
        
        # Query RxNorm
        if self.use_rxnorm and self.rxnorm_client:
            try:
                self._add_rxnorm_data(drug_name, harmonized)
            except Exception as e:
                logger.error(f"Error querying RxNorm for {drug_name}: {e}")
        
        # Query openFDA
        if self.use_openfda and self.fda_client:
            try:
                self._add_fda_data(drug_name, harmonized)
            except Exception as e:
                logger.error(f"Error querying openFDA for {drug_name}: {e}")
        
        # Query UNII
        if self.use_unii and self.unii_client:
            try:
                self._add_unii_data(drug_name, harmonized)
            except Exception as e:
                logger.error(f"Error querying UNII for {drug_name}: {e}")
        
        # Consolidate and deduplicate information
        self._consolidate_data(harmonized)
        
        # Calculate confidence score
        harmonized.confidence_score = self._calculate_confidence(harmonized)
        
        return harmonized
    
    def _add_rxnorm_data(self, drug_name: str, harmonized: HarmonizedDrugInfo) -> None:
        """Add RxNorm data to harmonized result"""
        drugs = self.rxnorm_client.search_drug_by_name(drug_name)
        
        if drugs and len(drugs) > 0:
            # Get detailed info for the first match
            drug_info = self.rxnorm_client.get_drug_info(drugs[0].rxcui)
            
            if drug_info:
                harmonized.rxcui = drug_info.rxcui
                harmonized.rxnorm_name = drug_info.name
                harmonized.rxnorm_brand_names = drug_info.brand_names or []
                harmonized.rxnorm_generic_name = drug_info.generic_name
                harmonized.rxnorm_ingredients = drug_info.ingredients or []
                harmonized.rxnorm_synonyms = drug_info.synonyms or []
                harmonized.atc_codes.extend(drug_info.atc_codes or [])
                harmonized.ndc_codes.extend(drug_info.ndc_codes or [])
                harmonized.sources_used.append('RxNorm')
                
                logger.info(f"RxNorm found: {drug_info.name} (RXCUI: {drug_info.rxcui})")
    
    def _add_fda_data(self, drug_name: str, harmonized: HarmonizedDrugInfo) -> None:
        """Add openFDA data to harmonized result"""
        drugs = self.fda_client.search_drug_by_name(drug_name)
        
        if drugs:
            for drug in drugs:
                if drug.brand_name:
                    harmonized.fda_brand_names.append(drug.brand_name)
                if drug.generic_name:
                    harmonized.fda_generic_names.append(drug.generic_name)
                if drug.manufacturer_name:
                    harmonized.fda_manufacturers.append(drug.manufacturer_name)
                if drug.application_number:
                    harmonized.fda_application_numbers.append(drug.application_number)
                
                # Extract strength from active ingredients
                for ingredient in drug.active_ingredients:
                    strength_info = ingredient.get('strength', '')
                    if strength_info:
                        harmonized.strength.append(strength_info)
                
                # Extract route of administration
                if drug.route:
                    harmonized.route.extend(drug.route)
                
                # Extract dosage form
                if drug.dosage_form:
                    harmonized.dosage_form.append(drug.dosage_form)
                
                harmonized.ndc_codes.extend(drug.product_ndc)
                harmonized.unii_codes.extend(drug.unii)
            
            harmonized.sources_used.append('openFDA')
            logger.info(f"openFDA found {len(drugs)} matches for {drug_name}")
    
    def _add_unii_data(self, drug_name: str, harmonized: HarmonizedDrugInfo) -> None:
        """Add UNII data to harmonized result"""
        substances = self.unii_client.search_substance_by_name(drug_name)
        
        if substances:
            for substance in substances:
                if substance.unii:
                    harmonized.unii_codes.append(substance.unii)
                if substance.cas_number:
                    harmonized.cas_numbers.append(substance.cas_number)
                if substance.molecular_formula:
                    harmonized.molecular_formulas.append(substance.molecular_formula)
            
            harmonized.sources_used.append('UNII')
            logger.info(f"UNII found {len(substances)} matches for {drug_name}")
    
    def _consolidate_data(self, harmonized: HarmonizedDrugInfo) -> None:
        """Consolidate and deduplicate information from all sources"""
        # Consolidate brand names
        harmonized.all_brand_names.update(harmonized.rxnorm_brand_names)
        harmonized.all_brand_names.update(harmonized.fda_brand_names)
        harmonized.all_brand_names.discard('')
        
        # Consolidate generic names
        if harmonized.rxnorm_generic_name:
            harmonized.all_generic_names.add(harmonized.rxnorm_generic_name)
        harmonized.all_generic_names.update(harmonized.fda_generic_names)
        harmonized.all_generic_names.discard('')
        
        # Consolidate synonyms
        harmonized.all_synonyms.update(harmonized.rxnorm_synonyms)
        harmonized.all_synonyms.update(harmonized.all_brand_names)
        harmonized.all_synonyms.update(harmonized.all_generic_names)
        harmonized.all_synonyms.discard('')
        
        # Deduplicate lists
        harmonized.ndc_codes = list(set(harmonized.ndc_codes))
        harmonized.unii_codes = list(set(harmonized.unii_codes))
        harmonized.atc_codes = list(set(harmonized.atc_codes))
        harmonized.cas_numbers = list(set(harmonized.cas_numbers))
        harmonized.molecular_formulas = list(set(harmonized.molecular_formulas))
        harmonized.strength = list(set(harmonized.strength))
        harmonized.route = list(set(harmonized.route))
        harmonized.dosage_form = list(set(harmonized.dosage_form))
        
        # Set harmonized primary fields
        # Priority: RxNorm > FDA generic name > first available
        if harmonized.rxnorm_generic_name:
            harmonized.harmonized_generic_name = harmonized.rxnorm_generic_name
        elif harmonized.all_generic_names:
            harmonized.harmonized_generic_name = sorted(harmonized.all_generic_names)[0]
        
        # Set harmonized brand name (first alphabetically for consistency)
        if harmonized.all_brand_names:
            harmonized.harmonized_brand_name = sorted(harmonized.all_brand_names)[0]
        
        # Set harmonized drug name (prefer generic, fallback to brand, then RxNorm name)
        if harmonized.harmonized_generic_name:
            harmonized.harmonized_drug_name = harmonized.harmonized_generic_name
        elif harmonized.harmonized_brand_name:
            harmonized.harmonized_drug_name = harmonized.harmonized_brand_name
        elif harmonized.rxnorm_name:
            harmonized.harmonized_drug_name = harmonized.rxnorm_name
        else:
            harmonized.harmonized_drug_name = harmonized.query_name
    
    def _calculate_confidence(self, harmonized: HarmonizedDrugInfo) -> float:
        """
        Calculate confidence score based on data completeness and source agreement
        
        Score components:
        - Number of sources: 0-30 points (10 per source)
        - Data completeness: 0-40 points
        - Cross-validation: 0-30 points
        """
        score = 0.0
        
        # Source coverage (max 30 points)
        score += len(harmonized.sources_used) * 10
        
        # Data completeness (max 40 points)
        if harmonized.rxcui:
            score += 10
        if harmonized.all_brand_names:
            score += 10
        if harmonized.all_generic_names:
            score += 10
        if harmonized.unii_codes or harmonized.atc_codes:
            score += 10
        
        # Cross-validation (max 30 points)
        # Check if multiple sources agree on generic names
        if len(harmonized.sources_used) >= 2:
            if len(harmonized.all_generic_names) > 0:
                score += 15
            if harmonized.ndc_codes or harmonized.unii_codes:
                score += 15
        
        return min(score, 100.0)
    
    def harmonize_drug_list(self, drug_names: List[str]) -> Dict[str, HarmonizedDrugInfo]:
        """
        Harmonize a list of drug names
        
        Args:
            drug_names: List of drug names to harmonize
            
        Returns:
            Dictionary mapping drug names to their harmonized information
        """
        results = {}
        
        total = len(drug_names)
        logger.info(f"Starting harmonization of {total} drugs")
        
        for i, drug_name in enumerate(drug_names, 1):
            logger.info(f"Processing {i}/{total}: {drug_name}")
            
            harmonized = self.harmonize_drug(drug_name)
            if harmonized:
                results[drug_name] = harmonized
            
        logger.info(f"Harmonization complete. Successfully processed {len(results)}/{total} drugs")
        
        return results
    
    def export_to_json(self, harmonized_data: Dict[str, HarmonizedDrugInfo], filepath: str) -> None:
        """Export harmonized data to JSON file"""
        export_data = {}
        
        for drug_name, info in harmonized_data.items():
            export_data[drug_name] = {
                'query_name': info.query_name,
                
                # Harmonized standardized fields
                'harmonized_drug_name': info.harmonized_drug_name,
                'harmonized_generic_name': info.harmonized_generic_name,
                'harmonized_brand_name': info.harmonized_brand_name,
                'strength': info.strength,
                'route': info.route,
                'dosage_form': info.dosage_form,
                'dose': info.dose,
                'administration_details': info.administration_details,
                
                # Source-specific data
                'rxcui': info.rxcui,
                'rxnorm_name': info.rxnorm_name,
                'brand_names': list(info.all_brand_names),
                'generic_names': list(info.all_generic_names),
                'synonyms': list(info.all_synonyms),
                
                # Identifiers and codes
                'atc_codes': info.atc_codes,
                'ndc_codes': info.ndc_codes,
                'unii_codes': info.unii_codes,
                'cas_numbers': info.cas_numbers,
                'molecular_formulas': info.molecular_formulas,
                
                # Metadata
                'sources_used': info.sources_used,
                'confidence_score': info.confidence_score
            }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported harmonized data to {filepath}")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    harmonizer = DrugHarmonizer()
    
    # Test single drug
    result = harmonizer.harmonize_drug("pirfenidone")
    if result:
        print(f"\n{'='*60}")
        print(f"Harmonized Drug Information for: {result.query_name}")
        print(f"{'='*60}")
        print(f"RxNorm RXCUI: {result.rxcui}")
        print(f"Brand Names: {', '.join(result.all_brand_names)}")
        print(f"Generic Names: {', '.join(result.all_generic_names)}")
        print(f"ATC Codes: {', '.join(result.atc_codes)}")
        print(f"UNII Codes: {', '.join(result.unii_codes)}")
        print(f"Sources Used: {', '.join(result.sources_used)}")
        print(f"Confidence Score: {result.confidence_score:.1f}/100")
    
    # Test multiple drugs
    drugs = ["nintedanib", "prednisone", "azathioprine"]
    results = harmonizer.harmonize_drug_list(drugs)
    
    print(f"\n{'='*60}")
    print(f"Batch Harmonization Results")
    print(f"{'='*60}")
    for drug, info in results.items():
        print(f"\n{drug}:")
        print(f"  Confidence: {info.confidence_score:.1f}/100")
        print(f"  Sources: {', '.join(info.sources_used)}")
