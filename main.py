#!/usr/bin/env python3
"""
Main entry point for the Clinical Trials Data Pipeline
"""

import argparse
import sys
import logging
from pathlib import Path

from config import Config
from pipeline.orchestrator import ClinicalTrialsOrchestrator

DEFAULT_OUTPUT_DIR = Path('D:\\CT_FDA\\data_pipeline\\data')

def setup_logging(log_level='INFO', log_file=None):
    """Setup logging configuration"""
    level = getattr(logging, log_level.upper())
    
    handlers = [logging.StreamHandler()]
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Clinical Trials Data Pipeline for IPF Research"
    )
    
    parser.add_argument('--us-trials', type=int, default=100, help='Maximum US trials to collect')
    parser.add_argument('--eu-trials', type=int, default=50, help='Maximum EU trials to collect')
    parser.add_argument('--request-delay', type=float, default=1.0, help='Delay between requests (seconds)')
    parser.add_argument('--output-dir', type=str, default=str(DEFAULT_OUTPUT_DIR), help='Output directory')
    parser.add_argument('--log-level', type=str, default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Configuration
    config = {
        'request_delay': args.request_delay,
        'max_retries': 3,
        'sqlite_path': str(output_dir / 'clinical_trials.db'),
        'mongodb_uri': None,
        'export_csv': True,
        'export_json': True,
        'max_us_trials': args.us_trials,
        'max_eu_trials': args.eu_trials,
        'output_dir': str(output_dir)
    }
    
    logger.info("Starting Clinical Trials Data Pipeline")
    logger.info(f"Configuration: {config}")
    
    # Create orchestrator and run pipeline
    orchestrator = ClinicalTrialsOrchestrator(config)
    
    try:
        result = orchestrator.run_full_pipeline()
        
        print("\n" + "=" * 60)
        print("PIPELINE RESULTS")
        print("=" * 60)
        print(f"Status: {result.get('status', 'Unknown')}")
        print(f"Total Trials: {result.get('total_trials_collected', 0)}")
        print(f"Trials Stored: {result.get('trials_stored', 0)}")
        print(f"Duration: {result.get('pipeline_duration', 0):.2f} seconds")
        
        if result.get('data_collection_stats'):
            stats = result['data_collection_stats']
            print(f"\nUS Trials: {stats.get('us_trials', 0)}")
            print(f"EU Trials: {stats.get('eu_trials', 0)}")
        
        if result.get('export_files'):
            print(f"\nExport Files:")
            for export_type, file_path in result['export_files'].items():
                print(f"  {export_type}: {file_path}")
        
        if result.get('status') == 'failed':
            print(f"\nError: {result.get('error', 'Unknown error')}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        orchestrator.cleanup()

if __name__ == "__main__":
    main()
