"""
Configuration Management
Handles loading and managing configuration settings
"""

import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for the clinical trials pipeline"""
    
    def __init__(self, config_file: Optional[str] = None):
        # Load environment variables
        load_dotenv()
        
        # Default configuration
        self.config = self._get_default_config()
        
        # Override with environment variables
        self._load_from_env()
        
        # Override with config file if provided
        if config_file and Path(config_file).exists():
            self._load_from_file(config_file)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            # API Configuration
            'clinical_trials_api_url': 'https://clinicaltrials.gov/api/query',
            'eu_clinical_trials_url': 'https://euclinicaltrials.eu',
            'rxnorm_api_url': 'https://clinicaltables.nlm.nih.gov/api/rxterms/v2',
            'meddra_api_url': 'https://api.fda.gov/drug/label.json',
            
            # Rate Limiting
            'request_delay': 1.0,
            'max_retries': 3,
            'timeout': 30,
            
            # Data Collection
            'max_us_trials': 1000,
            'max_eu_trials': 500,
            'max_eu_pages': 10,
            
            # Database Configuration
            'sqlite_path': 'clinical_trials.db',
            'mongodb_uri': None,
            
            # PostgreSQL Configuration (SCD Type 2)
            'postgres_host': 'localhost',
            'postgres_port': 5432,
            'postgres_database': 'ct_fda_pipeline',
            'postgres_user': 'postgres',
            'postgres_password': 'Admin',
            'use_scd2': True,
            
            # Output Configuration
            'output_dir': 'data',
            'export_csv': True,
            'export_json': True,
            'export_excel': False,
            
            # Logging Configuration
            'log_level': 'INFO',
            'log_file': 'clinical_trials_pipeline.log',
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            
            # Processing Configuration
            'enable_drug_normalization': True,
            'enable_adverse_event_extraction': True,
            'enable_meddra_coding': True,
            'deduplicate_trials': True,
            
            # Cache Configuration
            'enable_caching': True,
            'cache_ttl': 86400,  # 24 hours
            
            # Monitoring
            'enable_metrics': True,
            'metrics_port': 8080,
        }
    
    def _load_from_env(self):
        """Load configuration from environment variables"""
        env_mappings = {
            'CLINICAL_TRIALS_API_BASE_URL': 'clinical_trials_api_url',
            'EU_CLINICAL_TRIALS_BASE_URL': 'eu_clinical_trials_url',
            'RXNORM_API_BASE_URL': 'rxnorm_api_url',
            'MEDDRA_API_BASE_URL': 'meddra_api_url',
            'REQUEST_DELAY': 'request_delay',
            'MAX_RETRIES': 'max_retries',
            'DATABASE_URL': 'sqlite_path',
            'MONGODB_URI': 'mongodb_uri',
            'LOG_LEVEL': 'log_level',
            'OUTPUT_DIR': 'output_dir',
        }
        
        for env_var, config_key in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Type conversion
                if config_key in ['request_delay']:
                    value = float(value)
                elif config_key in ['max_retries', 'max_us_trials', 'max_eu_trials', 'max_eu_pages']:
                    value = int(value)
                elif config_key in ['export_csv', 'export_json', 'export_excel', 'enable_drug_normalization', 
                                  'enable_adverse_event_extraction', 'enable_meddra_coding', 
                                  'deduplicate_trials', 'enable_caching', 'enable_metrics']:
                    value = value.lower() in ('true', '1', 'yes', 'on')
                
                self.config[config_key] = value
                logger.debug(f"Loaded {config_key} from environment: {value}")
    
    def _load_from_file(self, config_file: str):
        """Load configuration from JSON file"""
        try:
            import json
            
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
            
            # Merge with existing config
            self.config.update(file_config)
            logger.info(f"Loaded configuration from {config_file}")
            
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        self.config[key] = value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            'sqlite_path': self.get('sqlite_path'),
            'mongodb_uri': self.get('mongodb_uri'),
        }
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration"""
        return {
            'clinical_trials_api_url': self.get('clinical_trials_api_url'),
            'eu_clinical_trials_url': self.get('eu_clinical_trials_url'),
            'rxnorm_api_url': self.get('rxnorm_api_url'),
            'meddra_api_url': self.get('meddra_api_url'),
            'request_delay': self.get('request_delay'),
            'max_retries': self.get('max_retries'),
            'timeout': self.get('timeout'),
        }
    
    def get_data_collection_config(self) -> Dict[str, Any]:
        """Get data collection configuration"""
        return {
            'max_us_trials': self.get('max_us_trials'),
            'max_eu_trials': self.get('max_eu_trials'),
            'max_eu_pages': self.get('max_eu_pages'),
        }
    
    def get_output_config(self) -> Dict[str, Any]:
        """Get output configuration"""
        return {
            'output_dir': self.get('output_dir'),
            'export_csv': self.get('export_csv'),
            'export_json': self.get('export_json'),
            'export_excel': self.get('export_excel'),
        }
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return {
            'enable_drug_normalization': self.get('enable_drug_normalization'),
            'enable_adverse_event_extraction': self.get('enable_adverse_event_extraction'),
            'enable_meddra_coding': self.get('enable_meddra_coding'),
            'deduplicate_trials': self.get('deduplicate_trials'),
        }
    
    def get_cache_config(self) -> Dict[str, Any]:
        """Get cache configuration"""
        return {
            'enable_caching': self.get('enable_caching'),
            'cache_ttl': self.get('cache_ttl'),
        }
    
    def get_postgres_config(self) -> Dict[str, Any]:
        """Get PostgreSQL configuration for SCD Type 2"""
        return {
            'host': self.get('postgres_host'),
            'port': self.get('postgres_port'),
            'database': self.get('postgres_database'),
            'user': self.get('postgres_user'),
            'password': self.get('postgres_password'),
        }
    
    def validate(self) -> bool:
        """Validate configuration"""
        errors = []
        
        # Check required directories
        output_dir = Path(self.get('output_dir'))
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                errors.append(f"Cannot create output directory {output_dir}: {e}")
        
        # Check numeric values
        numeric_fields = ['request_delay', 'max_retries', 'timeout', 'max_us_trials', 
                         'max_eu_trials', 'max_eu_pages', 'cache_ttl']
        for field in numeric_fields:
            value = self.get(field)
            if value is not None and (not isinstance(value, (int, float)) or value < 0):
                errors.append(f"Invalid value for {field}: {value}")
        
        # Check boolean fields
        boolean_fields = ['export_csv', 'export_json', 'export_excel', 'enable_drug_normalization',
                         'enable_adverse_event_extraction', 'enable_meddra_coding', 'deduplicate_trials',
                         'enable_caching', 'enable_metrics']
        for field in boolean_fields:
            value = self.get(field)
            if value is not None and not isinstance(value, bool):
                errors.append(f"Invalid value for {field}: {value}")
        
        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("Configuration validation passed")
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as dictionary"""
        return self.config.copy()
    
    def save_to_file(self, config_file: str):
        """Save current configuration to file"""
        try:
            import json
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Configuration saved to {config_file}")
            
        except Exception as e:
            logger.error(f"Error saving config file {config_file}: {e}")

def setup_logging(config: Config) -> None:
    """Setup logging configuration"""
    log_level = getattr(logging, config.get('log_level', 'INFO').upper())
    log_file = config.get('log_file')
    log_format = config.get('log_format')
    
    # Create logs directory if needed
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console output
            logging.FileHandler(log_file) if log_file else logging.NullHandler(),  # File output
        ]
    )
    
    logger.info(f"Logging configured at level {log_level}")

# Global configuration instance
_config = None

def get_config(config_file: Optional[str] = None) -> Config:
    """Get global configuration instance"""
    global _config
    if _config is None:
        _config = Config(config_file)
    return _config

def init_config(config_file: Optional[str] = None) -> Config:
    """Initialize global configuration"""
    global _config
    _config = Config(config_file)
    return _config

if __name__ == "__main__":
    # Example usage
    config = Config()
    
    print("Default Configuration:")
    for key, value in config.config.items():
        print(f"  {key}: {value}")
    
    # Test validation
    print(f"\nConfiguration valid: {config.validate()}")
    
    # Test environment loading (if .env file exists)
    print(f"\nAPI Config: {config.get_api_config()}")
    print(f"Database Config: {config.get_database_config()}")
