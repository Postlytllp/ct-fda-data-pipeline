"""
Script to harmonize all drugs from drugs.txt using DrugHarmonizer
Reads drug names from drugs.txt and generates harmonized_drugs_output.json
"""

import logging
import json
from datetime import datetime
from pathlib import Path
from medical_libraries.drug_harmonizer import DrugHarmonizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def load_drugs_from_file(filepath: str) -> list:
    """Load drug names from text file, one per line"""
    drugs = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            drug = line.strip()
            if drug:  # Skip empty lines
                drugs.append(drug)
    return drugs

def main():
    # File paths
    input_file = "drugs.txt"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/harmonized_drugs_{timestamp}.json"
    
    logger.info(f"Starting drug harmonization from {input_file}")
    
    # Load drug names
    logger.info("Loading drug names from file...")
    drug_names = load_drugs_from_file(input_file)
    logger.info(f"Loaded {len(drug_names)} drug names")
    
    # Initialize harmonizer
    logger.info("Initializing DrugHarmonizer...")
    harmonizer = DrugHarmonizer(
        use_rxnorm=True,
        use_openfda=True,
        use_unii=True
    )
    
    # Harmonize all drugs
    logger.info(f"Starting harmonization of {len(drug_names)} drugs...")
    harmonized_results = harmonizer.harmonize_drug_list(drug_names)
    
    # Export to JSON
    logger.info(f"Exporting results to {output_file}...")
    harmonizer.export_to_json(harmonized_results, output_file)
    
    # Print summary statistics
    logger.info("\n" + "="*60)
    logger.info("HARMONIZATION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total drugs processed: {len(drug_names)}")
    logger.info(f"Successfully harmonized: {len(harmonized_results)}")
    logger.info(f"Failed: {len(drug_names) - len(harmonized_results)}")
    
    # Statistics by confidence score
    if harmonized_results:
        high_conf = sum(1 for info in harmonized_results.values() if info.confidence_score >= 70)
        med_conf = sum(1 for info in harmonized_results.values() if 40 <= info.confidence_score < 70)
        low_conf = sum(1 for info in harmonized_results.values() if info.confidence_score < 40)
        
        logger.info(f"\nConfidence Distribution:")
        logger.info(f"  High (>=70): {high_conf} ({high_conf/len(harmonized_results)*100:.1f}%)")
        logger.info(f"  Medium (40-69): {med_conf} ({med_conf/len(harmonized_results)*100:.1f}%)")
        logger.info(f"  Low (<40): {low_conf} ({low_conf/len(harmonized_results)*100:.1f}%)")
        
        # Average confidence
        avg_conf = sum(info.confidence_score for info in harmonized_results.values()) / len(harmonized_results)
        logger.info(f"\nAverage confidence score: {avg_conf:.1f}/100")
    
    logger.info(f"\nOutput saved to: {output_file}")
    logger.info("="*60)

if __name__ == "__main__":
    main()
