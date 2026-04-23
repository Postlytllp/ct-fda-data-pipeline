"""
Main Pipeline Orchestrator
Coordinates the entire clinical trials data collection and processing pipeline
"""

import logging
import asyncio
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
from pathlib import Path
import pandas as pd

from data_sources.clinical_trials_gov import ClinicalTrialsGovClient
from data_sources.eu_clinical_trials import EUClinicalTrialsClient
from pipeline.data_processor import DataProcessor, EnrichedClinicalTrial
from storage.database import DatabaseManager

logger = logging.getLogger(__name__)

class ClinicalTrialsOrchestrator:
    """Main orchestrator for the clinical trials data pipeline"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or self._default_config()
        
        # Initialize components
        self.us_client = ClinicalTrialsGovClient(
            request_delay=self.config.get('request_delay', 1.0),
            max_retries=self.config.get('max_retries', 3)
        )
        
        self.eu_client = EUClinicalTrialsClient(
            request_delay=self.config.get('request_delay', 2.0),
            max_retries=self.config.get('max_retries', 3)
        )
        
        self.processor = DataProcessor()
        
        self.db_manager = DatabaseManager(
            sqlite_path=self.config.get('sqlite_path', 'clinical_trials.db'),
            mongodb_uri=self.config.get('mongodb_uri')
        )
        
        # Pipeline state
        self.pipeline_stats = {}
        self.start_time = None
        self.end_time = None
    
    def _default_config(self) -> Dict:
        """Default configuration"""
        return {
            'request_delay': 1.0,
            'max_retries': 3,
            'sqlite_path': 'clinical_trials.db',
            'mongodb_uri': None,
            'export_csv': True,
            'export_json': True,
            'max_us_trials': 1000,
            'max_eu_trials': 500,
            'output_dir': 'data'
        }
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete data collection and processing pipeline"""
        logger.info("Starting clinical trials data pipeline")
        self.start_time = datetime.now()
        
        try:
            # Step 1: Collect data from sources
            logger.info("Step 1: Collecting data from clinical trials sources")
            us_trials, eu_trials = self._collect_data()
            
            # Step 2: Process and enrich data
            logger.info("Step 2: Processing and enriching trial data")
            enriched_trials = self._process_data(us_trials, eu_trials)
            
            # Step 3: Store data
            logger.info("Step 3: Storing processed data")
            stored_count = self._store_data(enriched_trials)
            
            # Step 4: Generate exports
            logger.info("Step 4: Generating exports")
            exports = self._generate_exports(enriched_trials)
            
            # Step 5: Generate summary
            logger.info("Step 5: Generating pipeline summary")
            summary = self._generate_summary(enriched_trials, stored_count, exports)
            
            self.end_time = datetime.now()
            summary['pipeline_duration'] = (self.end_time - self.start_time).total_seconds()
            
            logger.info("Pipeline completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.end_time = datetime.now()
            return {
                'status': 'failed',
                'error': str(e),
                'pipeline_duration': (self.end_time - self.start_time).total_seconds() if self.start_time else 0
            }
    
    def _collect_data(self) -> tuple:
        """Collect data from all sources"""
        # Collect US trials
        logger.info("Collecting ClinicalTrials.gov data...")
        us_trials = self.us_client.search_ipf_trials(limit=self.config.get('max_us_trials', 1000))
        logger.info(f"Collected {len(us_trials)} US trials")
        
        # Collect EU trials
        logger.info("Collecting EU Clinical Trials data...")
        eu_trials = self.eu_client.search_ipf_trials(max_pages=self.config.get('max_eu_pages', 10))
        logger.info(f"Collected {len(eu_trials)} EU trials")
        
        return us_trials, eu_trials
    
    def _process_data(self, us_trials: List, eu_trials: List) -> List[EnrichedClinicalTrial]:
        """Process and enrich trial data"""
        # Process US trials
        logger.info("Processing US trial data...")
        enriched_us_trials = self.processor.process_clinical_trials_data(us_trials)
        
        # Process EU trials
        logger.info("Processing EU trial data...")
        enriched_eu_trials = self.processor.process_eu_trials_data(eu_trials)
        
        # Merge and deduplicate
        logger.info("Merging and deduplicating trials...")
        all_trials = self.processor.merge_trial_data(enriched_us_trials, enriched_eu_trials)
        
        logger.info(f"Processed {len(all_trials)} total trials")
        return all_trials
    
    def _store_data(self, trials: List[EnrichedClinicalTrial]) -> int:
        """Store processed data"""
        stored_count = self.db_manager.store_trials_batch(trials)
        logger.info(f"Stored {stored_count} trials in database")
        return stored_count
    
    def _generate_exports(self, trials: List[EnrichedClinicalTrial]) -> Dict[str, str]:
        """Generate various export formats"""
        output_dir = Path(self.config.get('output_dir', 'data'))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        exports = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # CSV export
        if self.config.get('export_csv', True):
            csv_file = output_dir / f"clinical_trials_{timestamp}.csv"
            df = self.processor.export_to_dataframe(trials)
            df.to_csv(csv_file, index=False, encoding='utf-8')
            exports['csv'] = str(csv_file)
            logger.info(f"Exported CSV to {csv_file}")
        
        # Detailed JSON export
        if self.config.get('export_json', True):
            json_file = output_dir / f"clinical_trials_detailed_{timestamp}.json"
            self.processor.export_detailed_data(trials, str(json_file))
            exports['json'] = str(json_file)
            logger.info(f"Exported detailed JSON to {json_file}")

        # Raw ClinicalTrials.gov v2 study JSON export (all fields)
        ctgov_raw = {
            t.trial_id: t.raw_source_record
            for t in trials
            if t.source == 'clinicaltrials.gov' and t.raw_source_record is not None
        }
        if ctgov_raw:
            ctgov_raw_file = output_dir / f"clinical_trials_ctgov_raw_{timestamp}.json"
            with open(ctgov_raw_file, 'w', encoding='utf-8') as f:
                json.dump(ctgov_raw, f, indent=2, ensure_ascii=False)
            exports['ctgov_raw_json'] = str(ctgov_raw_file)
            logger.info(f"Exported raw ClinicalTrials.gov JSON to {ctgov_raw_file}")
        
        # Database export
        db_csv_file = output_dir / f"clinical_trials_db_export_{timestamp}.csv"
        self.db_manager.export_to_csv(str(db_csv_file))
        exports['database_csv'] = str(db_csv_file)
        logger.info(f"Exported database CSV to {db_csv_file}")
        
        # Harmonized drugs export (multi-source drug data)
        harmonized_drugs_file = output_dir / f"harmonized_drugs_{timestamp}.json"
        self.processor.export_harmonized_drugs(trials, str(harmonized_drugs_file))
        exports['harmonized_drugs'] = str(harmonized_drugs_file)
        logger.info(f"Exported harmonized drugs to {harmonized_drugs_file}")
        
        return exports
    
    def _generate_summary(self, trials: List[EnrichedClinicalTrial], 
                         stored_count: int, exports: Dict[str, str]) -> Dict[str, Any]:
        """Generate pipeline execution summary"""
        # Get statistics from processor
        stats = self.processor.generate_summary_statistics(trials)
        
        # Get database statistics
        db_stats = self.db_manager.get_statistics()
        
        # Pipeline statistics
        pipeline_stats = {
            'status': 'completed',
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_trials_collected': len(trials),
            'trials_stored': stored_count,
            'exports_generated': list(exports.keys()),
            'export_files': exports,
            'data_collection_stats': stats,
            'database_stats': db_stats
        }
        
        # Save summary to file
        output_dir = Path(self.config.get('output_dir', 'data'))
        summary_file = output_dir / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(pipeline_stats, f, indent=2, ensure_ascii=False)
        
        pipeline_stats['summary_file'] = str(summary_file)
        
        return pipeline_stats
    
    def run_incremental_update(self) -> Dict[str, Any]:
        """Run incremental update to get new trials since last run"""
        logger.info("Starting incremental update")
        
        try:
            # Get last collection date from database
            last_date = self._get_last_collection_date()
            logger.info(f"Last collection date: {last_date}")
            
            # For simplicity, run full pipeline but could be optimized
            # to only fetch new trials based on dates
            result = self.run_full_pipeline()
            result['update_type'] = 'incremental'
            
            return result
            
        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'update_type': 'incremental'
            }
    
    def _get_last_collection_date(self) -> Optional[str]:
        """Get the date of last data collection"""
        try:
            # This would typically be stored in a metadata table
            # For now, return None to trigger full collection
            return None
        except:
            return None
    
    def search_trials(self, **filters) -> List[EnrichedClinicalTrial]:
        """Search stored trials"""
        return self.db_manager.search_trials(**filters)
    
    def get_trial_details(self, trial_id: str) -> Optional[EnrichedClinicalTrial]:
        """Get detailed information for a specific trial"""
        return self.db_manager.get_trial(trial_id)
    
    def export_custom_query(self, output_file: str, **filters):
        """Export custom query results"""
        self.db_manager.export_to_csv(output_file, **filters)
        logger.info(f"Custom query exported to {output_file}")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        db_stats = self.db_manager.get_statistics()
        
        return {
            'database_stats': db_stats,
            'last_run': {
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None
            },
            'configuration': self.config
        }
    
    def cleanup(self):
        """Cleanup resources"""
        self.db_manager.close()
        logger.info("Pipeline cleanup completed")

def main():
    """Main function for running the pipeline"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configuration
    config = {
        'request_delay': 1.0,
        'max_retries': 3,
        'sqlite_path': 'clinical_trials.db',
        'mongodb_uri': None,
        'export_csv': True,
        'export_json': True,
        'max_us_trials': 100,
        'max_eu_trials': 50,
        'output_dir': 'data'
    }
    
    # Create orchestrator and run pipeline
    orchestrator = ClinicalTrialsOrchestrator(config)
    
    try:
        # Run the full pipeline
        result = orchestrator.run_full_pipeline()
        
        print("\n" + "="*50)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*50)
        print(f"Status: {result.get('status', 'Unknown')}")
        print(f"Total Trials Collected: {result.get('total_trials_collected', 0)}")
        print(f"Trials Stored: {result.get('trials_stored', 0)}")
        print(f"Pipeline Duration: {result.get('pipeline_duration', 0):.2f} seconds")
        
        if result.get('data_collection_stats'):
            stats = result['data_collection_stats']
            print(f"\nData Collection Stats:")
            print(f"  US Trials: {stats.get('us_trials', 0)}")
            print(f"  EU Trials: {stats.get('eu_trials', 0)}")
            print(f"  Countries: {len(stats.get('country_distribution', {}))}")
            print(f"  Drugs: {len(stats.get('drug_distribution', {}))}")
        
        if result.get('export_files'):
            print(f"\nExport Files:")
            for export_type, file_path in result['export_files'].items():
                print(f"  {export_type}: {file_path}")
        
        print(f"\nSummary File: {result.get('summary_file', 'N/A')}")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        orchestrator.cleanup()

if __name__ == "__main__":
    main()
