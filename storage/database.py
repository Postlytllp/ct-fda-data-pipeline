"""
Database Storage Module
Handles storage of clinical trial data in SQLite and MongoDB
"""

import logging
import sqlite3
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import pandas as pd
from pathlib import Path

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False
    logging.warning("MongoDB not available. Only SQLite storage will be supported.")

from pipeline.data_processor import EnrichedClinicalTrial

logger = logging.getLogger(__name__)

def dataclass_to_dict(obj):
    """Convert dataclass instance to dict, handling nested dataclasses"""
    if hasattr(obj, '__dict__'):
        result = {}
        for key, value in obj.__dict__.items():
            if hasattr(value, '__dict__'):
                result[key] = dataclass_to_dict(value)
            elif isinstance(value, list):
                result[key] = [dataclass_to_dict(item) if hasattr(item, '__dict__') else item for item in value]
            elif isinstance(value, dict):
                result[key] = {k: dataclass_to_dict(v) if hasattr(v, '__dict__') else v for k, v in value.items()}
            else:
                result[key] = value
        return result
    return obj

class DatabaseManager:
    """Manages database operations for clinical trials data"""
    
    def __init__(self, sqlite_path: str = "clinical_trials.db", 
                 mongodb_uri: Optional[str] = None):
        self.sqlite_path = sqlite_path
        self.mongodb_uri = mongodb_uri
        self.mongodb_client = None
        self.mongodb_db = None
        
        # Initialize SQLite
        self._init_sqlite()
        
        # Initialize MongoDB if available and URI provided
        if mongodb_uri and MONGODB_AVAILABLE:
            self._init_mongodb()
    
    def _init_sqlite(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            # Create trials table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trials (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trial_id TEXT UNIQUE NOT NULL,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    condition TEXT,
                    status TEXT,
                    phase TEXT,
                    study_type TEXT,
                    sponsor TEXT,
                    start_date TEXT,
                    completion_date TEXT,
                    enrollment INTEGER,
                    url TEXT,
                    description TEXT,
                    eligibility_criteria TEXT,
                    countries TEXT,
                    interventions TEXT,
                    normalized_drugs TEXT,
                    adverse_events TEXT,
                    meddra_codes TEXT,
                    raw_source_record TEXT,
                    data_collection_date TEXT,
                    processing_timestamp TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Add raw_source_record column if it doesn't exist (migration for existing DBs)
            try:
                cursor.execute('ALTER TABLE trials ADD COLUMN raw_source_record TEXT')
                logger.info("Added raw_source_record column to existing trials table")
            except sqlite3.OperationalError:
                # Column already exists
                pass
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_trial_id ON trials(trial_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON trials(source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON trials(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_phase ON trials(phase)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_condition ON trials(condition)')
            
            conn.commit()
            conn.close()
            
            logger.info(f"SQLite database initialized at {self.sqlite_path}")
            
        except Exception as e:
            logger.error(f"Error initializing SQLite database: {e}")
            raise
    
    def _init_mongodb(self):
        """Initialize MongoDB connection"""
        if not MONGODB_AVAILABLE:
            logger.warning("MongoDB not available, skipping MongoDB initialization")
            return
        
        try:
            self.mongodb_client = MongoClient(self.mongodb_uri)
            # Test connection
            self.mongodb_client.admin.command('ping')
            
            # Get database
            db_name = self.mongodb_uri.split('/')[-1] if '/' in self.mongodb_uri else 'clinical_trials'
            self.mongodb_db = self.mongodb_client[db_name]
            
            # Create indexes
            self.mongodb_db.trials.create_index('trial_id', unique=True)
            self.mongodb_db.trials.create_index('source')
            self.mongodb_db.trials.create_index('status')
            self.mongodb_db.trials.create_index('phase')
            self.mongodb_db.trials.create_index('condition')
            
            logger.info(f"MongoDB connection established: {self.mongodb_uri}")
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.mongodb_client = None
        except Exception as e:
            logger.error(f"Error initializing MongoDB: {e}")
            self.mongodb_client = None
    
    def store_trial(self, trial: EnrichedClinicalTrial) -> bool:
        """Store a single trial"""
        success = False
        
        # Store in SQLite
        sqlite_success = self._store_trial_sqlite(trial)
        
        # Store in MongoDB if available
        mongodb_success = True
        if self.mongodb_client:
            mongodb_success = self._store_trial_mongodb(trial)
        
        success = sqlite_success and mongodb_success
        
        if success:
            logger.debug(f"Successfully stored trial {trial.trial_id}")
        else:
            logger.error(f"Failed to store trial {trial.trial_id}")
        
        return success
    
    def _store_trial_sqlite(self, trial: EnrichedClinicalTrial) -> bool:
        """Store trial in SQLite"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO trials (
                    trial_id, source, title, condition, status, phase, study_type,
                    sponsor, start_date, completion_date, enrollment, url,
                    description, eligibility_criteria, countries, interventions,
                    normalized_drugs, adverse_events, meddra_codes, raw_source_record,
                    data_collection_date, processing_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trial.trial_id,
                trial.source,
                trial.title,
                trial.condition,
                trial.status,
                trial.phase,
                trial.study_type,
                trial.sponsor,
                trial.start_date,
                trial.completion_date,
                trial.enrollment,
                trial.url,
                trial.description,
                trial.eligibility_criteria,
                json.dumps(trial.countries),
                json.dumps(trial.interventions),
                json.dumps({k: dataclass_to_dict(v) if v else None for k, v in trial.normalized_drugs.items()}),
                json.dumps([dataclass_to_dict(event) for event in trial.adverse_events]),
                json.dumps(trial.meddra_codes),
                json.dumps(trial.raw_source_record) if trial.raw_source_record else None,
                trial.data_collection_date,
                trial.processing_timestamp
            ))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing trial in SQLite: {e}")
            return False
    
    def _store_trial_mongodb(self, trial: EnrichedClinicalTrial) -> bool:
        """Store trial in MongoDB"""
        if not self.mongodb_db:
            return True  # Not an error if MongoDB is not available
        
        try:
            trial_dict = trial.__dict__.copy()
            
            # Convert complex objects to dictionaries
            trial_dict['normalized_drugs'] = {
                k: v.__dict__ if v else None for k, v in trial_dict['normalized_drugs'].items()
            }
            trial_dict['adverse_events'] = [
                event.__dict__ for event in trial_dict['adverse_events']
            ]
            
            # Store in MongoDB
            self.mongodb_db.trials.replace_one(
                {'trial_id': trial.trial_id},
                trial_dict,
                upsert=True
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing trial in MongoDB: {e}")
            return False
    
    def store_trials_batch(self, trials: List[EnrichedClinicalTrial]) -> int:
        """Store multiple trials in batch"""
        success_count = 0
        
        for i, trial in enumerate(trials):
            if i % 10 == 0:
                logger.info(f"Storing trial {i + 1}/{len(trials)}")
            
            if self.store_trial(trial):
                success_count += 1
        
        logger.info(f"Successfully stored {success_count}/{len(trials)} trials")
        return success_count
    
    def get_trial(self, trial_id: str) -> Optional[EnrichedClinicalTrial]:
        """Get a specific trial by ID"""
        # Try SQLite first
        trial = self._get_trial_sqlite(trial_id)
        if trial:
            return trial
        
        # Try MongoDB if SQLite didn't find it
        if self.mongodb_db:
            trial = self._get_trial_mongodb(trial_id)
        
        return trial
    
    def _get_trial_sqlite(self, trial_id: str) -> Optional[EnrichedClinicalTrial]:
        """Get trial from SQLite"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM trials WHERE trial_id = ?', (trial_id,))
            row = cursor.fetchone()
            
            conn.close()
            
            if row:
                return self._row_to_trial(row)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting trial from SQLite: {e}")
            return None
    
    def _get_trial_mongodb(self, trial_id: str) -> Optional[EnrichedClinicalTrial]:
        """Get trial from MongoDB"""
        if not self.mongodb_db:
            return None
        
        try:
            doc = self.mongodb_db.trials.find_one({'trial_id': trial_id})
            
            if doc:
                return self._doc_to_trial(doc)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting trial from MongoDB: {e}")
            return None
    
    def _row_to_trial(self, row) -> EnrichedClinicalTrial:
        """Convert SQLite row to EnrichedClinicalTrial"""
        columns = [
            'id', 'trial_id', 'source', 'title', 'condition', 'status', 'phase',
            'study_type', 'sponsor', 'start_date', 'completion_date', 'enrollment',
            'url', 'description', 'eligibility_criteria', 'countries',
            'interventions', 'normalized_drugs', 'adverse_events', 'meddra_codes',
            'raw_source_record', 'data_collection_date', 'processing_timestamp', 'created_at'
        ]
        
        trial_dict = dict(zip(columns, row))
        
        # Parse JSON fields
        trial_dict['countries'] = json.loads(trial_dict['countries'])
        trial_dict['interventions'] = json.loads(trial_dict['interventions'])
        trial_dict['normalized_drugs'] = json.loads(trial_dict['normalized_drugs'])
        trial_dict['adverse_events'] = json.loads(trial_dict['adverse_events'])
        trial_dict['meddra_codes'] = json.loads(trial_dict['meddra_codes'])
        # Handle raw_source_record which may be NULL or empty string
        raw_rec = trial_dict.get('raw_source_record')
        trial_dict['raw_source_record'] = json.loads(raw_rec) if raw_rec and raw_rec != 'null' else None
        
        # Remove database-specific fields
        trial_dict.pop('id', None)
        trial_dict.pop('created_at', None)
        
        return EnrichedClinicalTrial(**trial_dict)
    
    def _doc_to_trial(self, doc: Dict) -> EnrichedClinicalTrial:
        """Convert MongoDB document to EnrichedClinicalTrial"""
        # Remove MongoDB-specific fields
        doc.pop('_id', None)
        
        return EnrichedClinicalTrial(**doc)
    
    def search_trials(self, **filters) -> List[EnrichedClinicalTrial]:
        """Search trials with filters"""
        # For now, implement SQLite search
        return self._search_trials_sqlite(**filters)
    
    def _search_trials_sqlite(self, **filters) -> List[EnrichedClinicalTrial]:
        """Search trials in SQLite"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            query = "SELECT * FROM trials WHERE 1=1"
            params = []
            
            if 'source' in filters:
                query += " AND source = ?"
                params.append(filters['source'])
            
            if 'status' in filters:
                query += " AND status = ?"
                params.append(filters['status'])
            
            if 'phase' in filters:
                query += " AND phase = ?"
                params.append(filters['phase'])
            
            if 'condition' in filters:
                query += " AND condition LIKE ?"
                params.append(f"%{filters['condition']}%")
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            conn.close()
            
            trials = [self._row_to_trial(row) for row in rows]
            return trials
            
        except Exception as e:
            logger.error(f"Error searching trials in SQLite: {e}")
            return []
    
    def export_to_csv(self, output_file: str, **filters):
        """Export trials to CSV file"""
        trials = self.search_trials(**filters)
        
        if not trials:
            logger.warning("No trials found for export")
            return
        
        # Convert to DataFrame
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
                'countries': '; '.join(trial.countries),
                'interventions': '; '.join(trial.interventions),
                'adverse_event_count': len(trial.adverse_events),
                'meddra_codes': '; '.join(trial.meddra_codes),
                'data_collection_date': trial.data_collection_date
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logger.info(f"Exported {len(trials)} trials to {output_file}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            # Total trials
            cursor.execute("SELECT COUNT(*) FROM trials")
            total_trials = cursor.fetchone()[0]
            
            # Trials by source
            cursor.execute("SELECT source, COUNT(*) FROM trials GROUP BY source")
            by_source = dict(cursor.fetchall())
            
            # Trials by status
            cursor.execute("SELECT status, COUNT(*) FROM trials GROUP BY status")
            by_status = dict(cursor.fetchall())
            
            # Trials by phase
            cursor.execute("SELECT phase, COUNT(*) FROM trials GROUP BY phase")
            by_phase = dict(cursor.fetchall())
            
            conn.close()
            
            stats = {
                'total_trials': total_trials,
                'by_source': by_source,
                'by_status': by_status,
                'by_phase': by_phase,
                'last_updated': datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {}
    
    def close(self):
        """Close database connections"""
        if self.mongodb_client:
            self.mongodb_client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    db = DatabaseManager()
    stats = db.get_statistics()
    print(f"Database statistics: {stats}")
    
    db.close()
