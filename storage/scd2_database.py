"""
SCD Type 2 Database Module for PostgreSQL
Handles Slowly Changing Dimension Type 2 for clinical trials data
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import asdict
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SCD2DatabaseManager:
    """
    Manages SCD Type 2 operations for clinical trials data in PostgreSQL.
    
    SCD Type 2 tracks historical changes by:
    - Creating new records when data changes
    - Maintaining valid_from/valid_to timestamps
    - Keeping is_current flag for easy current-state queries
    """
    
    # Columns tracked for change detection
    TRACKED_COLUMNS = [
        'status', 'phase', 'enrollment', 'completion_date',
        'description', 'eligibility_criteria', 'interventions',
        'normalized_drugs', 'harmonized_drugs', 'adverse_events',
        'meddra_codes'
    ]
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "ct_fda_pipeline",
        user: str = "postgres",
        password: str = "Admin"
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self._init_schema()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _init_schema(self):
        """Initialize PostgreSQL schema with SCD Type 2 tables"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create trials_scd2 table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trials_scd2 (
                        -- Surrogate key
                        surrogate_id SERIAL PRIMARY KEY,
                        
                        -- Natural key (business key)
                        trial_id VARCHAR(100) NOT NULL,
                        
                        -- Source information
                        source VARCHAR(50) NOT NULL,
                        
                        -- Dimension attributes (tracked for changes)
                        title TEXT,
                        condition TEXT,
                        status VARCHAR(100),
                        phase VARCHAR(50),
                        study_type VARCHAR(100),
                        sponsor TEXT,
                        start_date VARCHAR(50),
                        completion_date VARCHAR(50),
                        enrollment INTEGER,
                        url TEXT,
                        description TEXT,
                        eligibility_criteria TEXT,
                        countries TEXT,
                        interventions JSONB,
                        normalized_drugs JSONB,
                        harmonized_drugs JSONB,
                        adverse_events JSONB,
                        meddra_codes JSONB,
                        raw_source_record JSONB,
                        
                        -- SCD Type 2 metadata
                        valid_from TIMESTAMP NOT NULL,
                        valid_to TIMESTAMP,
                        is_current BOOLEAN DEFAULT TRUE,
                        version INTEGER DEFAULT 1,
                        
                        -- Change tracking
                        record_hash VARCHAR(64),
                        change_type VARCHAR(20),
                        changed_columns TEXT[],
                        
                        -- Audit columns
                        data_collection_date DATE,
                        processing_timestamp TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes for performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trials_scd2_trial_id 
                    ON trials_scd2(trial_id)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trials_scd2_current 
                    ON trials_scd2(trial_id, is_current) WHERE is_current = TRUE
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trials_scd2_valid_dates 
                    ON trials_scd2(valid_from, valid_to)
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trials_scd2_status 
                    ON trials_scd2(status) WHERE is_current = TRUE
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_trials_scd2_source 
                    ON trials_scd2(source) WHERE is_current = TRUE
                """)
                
                # Create view for current records
                cursor.execute("""
                    CREATE OR REPLACE VIEW trials_current AS
                    SELECT * FROM trials_scd2 WHERE is_current = TRUE
                """)
                
                # Create pipeline_runs table for tracking
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id SERIAL PRIMARY KEY,
                        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50),
                        trials_inserted INTEGER DEFAULT 0,
                        trials_updated INTEGER DEFAULT 0,
                        trials_unchanged INTEGER DEFAULT 0,
                        us_trials_fetched INTEGER DEFAULT 0,
                        eu_trials_fetched INTEGER DEFAULT 0,
                        duration_seconds FLOAT,
                        error_message TEXT,
                        metadata JSONB
                    )
                """)
                
                # Create change_log table for audit
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trial_change_log (
                        log_id SERIAL PRIMARY KEY,
                        trial_id VARCHAR(100) NOT NULL,
                        change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        change_type VARCHAR(20),
                        old_version INTEGER,
                        new_version INTEGER,
                        changed_columns TEXT[],
                        old_values JSONB,
                        new_values JSONB,
                        pipeline_run_id INTEGER REFERENCES pipeline_runs(run_id)
                    )
                """)
                
                conn.commit()
                logger.info("PostgreSQL SCD2 schema initialized successfully")
                
        except Exception as e:
            logger.error(f"Error initializing PostgreSQL schema: {e}")
            raise
    
    def _compute_hash(self, record: Dict) -> str:
        """Compute SHA256 hash of tracked columns for change detection"""
        tracked_values = {}
        for col in self.TRACKED_COLUMNS:
            value = record.get(col)
            if isinstance(value, (list, dict)):
                tracked_values[col] = json.dumps(value, sort_keys=True)
            else:
                tracked_values[col] = str(value) if value is not None else None
        
        hash_string = json.dumps(tracked_values, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def _get_changed_columns(self, old_record: Dict, new_record: Dict) -> List[str]:
        """Identify which tracked columns changed"""
        changed = []
        for col in self.TRACKED_COLUMNS:
            old_val = old_record.get(col)
            new_val = new_record.get(col)
            
            # Normalize for comparison
            if isinstance(old_val, str):
                try:
                    old_val = json.loads(old_val)
                except (json.JSONDecodeError, TypeError):
                    pass
            
            if old_val != new_val:
                changed.append(col)
        
        return changed
    
    def _prepare_record(self, trial: Dict) -> Dict:
        """Prepare trial record for database insertion"""
        # Handle dataclass or dict
        if hasattr(trial, '__dict__') and hasattr(trial, '__dataclass_fields__'):
            trial = asdict(trial)
        
        # Serialize complex fields to JSON
        json_fields = ['interventions', 'normalized_drugs', 'harmonized_drugs', 
                       'adverse_events', 'meddra_codes', 'raw_source_record']
        
        prepared = {}
        for key, value in trial.items():
            if key in json_fields:
                if isinstance(value, (list, dict)):
                    prepared[key] = json.dumps(value)
                else:
                    prepared[key] = value
            else:
                prepared[key] = value
        
        return prepared
    
    def upsert_trials_scd2(
        self, 
        trials: List[Dict], 
        pipeline_run_id: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Upsert trials using SCD Type 2 logic.
        
        Args:
            trials: List of trial dictionaries
            pipeline_run_id: Optional run ID for audit trail
            
        Returns:
            Statistics dict with counts of inserted, updated, unchanged
        """
        stats = {'inserted': 0, 'updated': 0, 'unchanged': 0, 'errors': 0}
        now = datetime.utcnow()
        
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            for trial in trials:
                try:
                    prepared = self._prepare_record(trial)
                    trial_id = prepared.get('trial_id')
                    
                    if not trial_id:
                        logger.warning("Skipping trial without trial_id")
                        stats['errors'] += 1
                        continue
                    
                    new_hash = self._compute_hash(prepared)
                    
                    # Get current record
                    cursor.execute("""
                        SELECT surrogate_id, record_hash, version,
                               status, phase, enrollment, completion_date,
                               description, eligibility_criteria, interventions,
                               normalized_drugs, harmonized_drugs, adverse_events,
                               meddra_codes
                        FROM trials_scd2 
                        WHERE trial_id = %s AND is_current = TRUE
                    """, (trial_id,))
                    current = cursor.fetchone()
                    
                    if current is None:
                        # New trial - INSERT
                        self._insert_new_record(
                            cursor, prepared, new_hash, now, 
                            version=1, change_type='INSERT'
                        )
                        stats['inserted'] += 1
                        
                        # Log the change
                        if pipeline_run_id:
                            self._log_change(
                                cursor, trial_id, 'INSERT', 
                                None, 1, [], None, prepared, pipeline_run_id
                            )
                        
                    elif current['record_hash'] != new_hash:
                        # Changed - Close old record, insert new version
                        old_surrogate_id = current['surrogate_id']
                        old_version = current['version']
                        changed_cols = self._get_changed_columns(dict(current), prepared)
                        
                        # Close old record
                        cursor.execute("""
                            UPDATE trials_scd2 
                            SET valid_to = %s, 
                                is_current = FALSE, 
                                updated_at = %s
                            WHERE surrogate_id = %s
                        """, (now, now, old_surrogate_id))
                        
                        # Insert new version
                        self._insert_new_record(
                            cursor, prepared, new_hash, now,
                            version=old_version + 1,
                            change_type='UPDATE',
                            changed_columns=changed_cols
                        )
                        stats['updated'] += 1
                        
                        # Log the change
                        if pipeline_run_id:
                            old_values = {col: current.get(col) for col in changed_cols}
                            new_values = {col: prepared.get(col) for col in changed_cols}
                            self._log_change(
                                cursor, trial_id, 'UPDATE',
                                old_version, old_version + 1,
                                changed_cols, old_values, new_values, pipeline_run_id
                            )
                    else:
                        # No change - update timestamp only
                        stats['unchanged'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing trial {trial.get('trial_id', 'unknown')}: {e}")
                    stats['errors'] += 1
                    continue
            
            conn.commit()
        
        logger.info(f"SCD2 upsert complete: {stats}")
        return stats
    
    def _insert_new_record(
        self, 
        cursor, 
        record: Dict, 
        record_hash: str, 
        valid_from: datetime,
        version: int,
        change_type: str = 'INSERT',
        changed_columns: List[str] = None
    ):
        """Insert a new record into the SCD2 table"""
        cursor.execute("""
            INSERT INTO trials_scd2 (
                trial_id, source, title, condition, status, phase, study_type,
                sponsor, start_date, completion_date, enrollment, url,
                description, eligibility_criteria, countries, interventions,
                normalized_drugs, harmonized_drugs, adverse_events, meddra_codes,
                raw_source_record, valid_from, valid_to, is_current, version,
                record_hash, change_type, changed_columns,
                data_collection_date, processing_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            record.get('trial_id'),
            record.get('source'),
            record.get('title'),
            record.get('condition'),
            record.get('status'),
            record.get('phase'),
            record.get('study_type'),
            record.get('sponsor'),
            record.get('start_date'),
            record.get('completion_date'),
            record.get('enrollment'),
            record.get('url'),
            record.get('description'),
            record.get('eligibility_criteria'),
            record.get('countries'),
            record.get('interventions'),
            record.get('normalized_drugs'),
            record.get('harmonized_drugs'),
            record.get('adverse_events'),
            record.get('meddra_codes'),
            record.get('raw_source_record'),
            valid_from,
            None,  # valid_to
            True,  # is_current
            version,
            record_hash,
            change_type,
            changed_columns,
            record.get('data_collection_date'),
            record.get('processing_timestamp')
        ))
    
    def _log_change(
        self, cursor, trial_id: str, change_type: str,
        old_version: Optional[int], new_version: int,
        changed_columns: List[str], old_values: Optional[Dict],
        new_values: Dict, pipeline_run_id: int
    ):
        """Log change to audit table"""
        cursor.execute("""
            INSERT INTO trial_change_log (
                trial_id, change_type, old_version, new_version,
                changed_columns, old_values, new_values, pipeline_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            trial_id, change_type, old_version, new_version,
            changed_columns,
            json.dumps(old_values) if old_values else None,
            json.dumps(new_values) if new_values else None,
            pipeline_run_id
        ))
    
    def start_pipeline_run(self, metadata: Optional[Dict] = None) -> int:
        """Start a new pipeline run and return run_id"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pipeline_runs (status, metadata)
                VALUES ('RUNNING', %s)
                RETURNING run_id
            """, (json.dumps(metadata) if metadata else None,))
            run_id = cursor.fetchone()[0]
            conn.commit()
            return run_id
    
    def complete_pipeline_run(
        self, 
        run_id: int, 
        status: str,
        stats: Dict,
        duration_seconds: float,
        error_message: Optional[str] = None
    ):
        """Complete a pipeline run with final stats"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_runs
                SET status = %s,
                    trials_inserted = %s,
                    trials_updated = %s,
                    trials_unchanged = %s,
                    us_trials_fetched = %s,
                    eu_trials_fetched = %s,
                    duration_seconds = %s,
                    error_message = %s
                WHERE run_id = %s
            """, (
                status,
                stats.get('inserted', 0),
                stats.get('updated', 0),
                stats.get('unchanged', 0),
                stats.get('us_trials_fetched', 0),
                stats.get('eu_trials_fetched', 0),
                duration_seconds,
                error_message,
                run_id
            ))
            conn.commit()
    
    def get_trial_history(self, trial_id: str) -> List[Dict]:
        """Get complete history of a trial across all versions"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT * FROM trials_scd2 
                WHERE trial_id = %s 
                ORDER BY version DESC
            """, (trial_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_trial_at_date(self, trial_id: str, as_of: datetime) -> Optional[Dict]:
        """Get trial state at a specific point in time"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT * FROM trials_scd2 
                WHERE trial_id = %s 
                  AND valid_from <= %s
                  AND (valid_to IS NULL OR valid_to > %s)
            """, (trial_id, as_of, as_of))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    def get_current_trials(
        self, 
        source: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict]:
        """Get current version of all trials with optional filters"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            query = "SELECT * FROM trials_current WHERE 1=1"
            params = []
            
            if source:
                query += " AND source = %s"
                params.append(source)
            
            if status:
                query += " AND status = %s"
                params.append(status)
            
            query += " ORDER BY trial_id LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_recently_changed(self, days: int = 7) -> List[Dict]:
        """Get trials that changed in the last N days"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT * FROM trials_scd2 
                WHERE valid_from >= NOW() - INTERVAL '%s days'
                  AND change_type = 'UPDATE'
                ORDER BY valid_from DESC
            """, (days,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_pipeline_runs(self, limit: int = 10) -> List[Dict]:
        """Get recent pipeline runs"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT * FROM pipeline_runs 
                ORDER BY run_timestamp DESC 
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_change_summary(self, days: int = 30) -> Dict:
        """Get summary of changes over the last N days"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Count by change type
            cursor.execute("""
                SELECT change_type, COUNT(*) as count
                FROM trials_scd2
                WHERE valid_from >= NOW() - INTERVAL '%s days'
                GROUP BY change_type
            """, (days,))
            by_type = {row['change_type']: row['count'] for row in cursor.fetchall()}
            
            # Most frequently changing trials
            cursor.execute("""
                SELECT trial_id, COUNT(*) as version_count
                FROM trials_scd2
                WHERE valid_from >= NOW() - INTERVAL '%s days'
                GROUP BY trial_id
                HAVING COUNT(*) > 1
                ORDER BY version_count DESC
                LIMIT 10
            """, (days,))
            frequent_changes = [dict(row) for row in cursor.fetchall()]
            
            return {
                'period_days': days,
                'changes_by_type': by_type,
                'frequently_changing_trials': frequent_changes
            }


if __name__ == "__main__":
    # Test connection
    logging.basicConfig(level=logging.INFO)
    
    try:
        db = SCD2DatabaseManager()
        print("✓ PostgreSQL SCD2 connection successful!")
        
        # Test with sample data
        sample_trial = {
            'trial_id': 'TEST-001',
            'source': 'test',
            'title': 'Test Trial',
            'status': 'ACTIVE',
            'phase': 'Phase 1',
            'condition': 'Test Condition'
        }
        
        stats = db.upsert_trials_scd2([sample_trial])
        print(f"✓ Test insert: {stats}")
        
        # Update the trial
        sample_trial['status'] = 'COMPLETED'
        stats = db.upsert_trials_scd2([sample_trial])
        print(f"✓ Test update: {stats}")
        
        # Get history
        history = db.get_trial_history('TEST-001')
        print(f"✓ Trial history: {len(history)} versions")
        
    except Exception as e:
        print(f"✗ Error: {e}")
