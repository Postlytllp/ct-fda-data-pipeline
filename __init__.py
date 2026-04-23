"""
Clinical Trials Data Pipeline
A comprehensive pipeline for collecting and processing IPF clinical trial data
"""

__version__ = "1.0.0"
__author__ = "Clinical Trials Pipeline Team"
__description__ = "Data pipeline for IPF clinical trials from ClinicalTrials.gov and EU Clinical Trials"

from .config import Config, get_config, setup_logging
from .pipeline.orchestrator import ClinicalTrialsOrchestrator
from .pipeline.data_processor import DataProcessor, EnrichedClinicalTrial
from .storage.database import DatabaseManager

__all__ = [
    'Config',
    'get_config', 
    'setup_logging',
    'ClinicalTrialsOrchestrator',
    'DataProcessor',
    'EnrichedClinicalTrial',
    'DatabaseManager'
]
