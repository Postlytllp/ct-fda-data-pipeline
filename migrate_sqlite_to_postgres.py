"""
Migrate SQLite clinical_trials.db data to PostgreSQL SCD Type 2
"""
import sqlite3
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_sqlite_to_postgres():
    """Migrate all trials from SQLite to PostgreSQL SCD2"""
    
    print("="*70)
    print("Migrating SQLite data to PostgreSQL SCD Type 2")
    print("="*70)
    
    # Connect to SQLite
    sqlite_path = "data/clinical_trials.db"
    print(f"\n1. Connecting to SQLite: {sqlite_path}")
    
    try:
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all trials
        cursor.execute("SELECT * FROM trials")
        rows = cursor.fetchall()
        print(f"   Found {len(rows)} trials in SQLite")
        
        if not rows:
            print("   No trials to migrate!")
            return
        
    except Exception as e:
        print(f"   ✗ Failed to connect to SQLite: {e}")
        return
    
    # Convert SQLite rows to trial dictionaries for SCD2
    print("\n2. Converting trials for PostgreSQL...")
    trials_data = []
    
    for row in rows:
        try:
            # Parse JSON fields safely
            def safe_json_loads(val, default=None):
                if val is None or val == '' or val == 'null':
                    return default
                try:
                    return json.loads(val)
                except:
                    return default
            
            countries = safe_json_loads(row['countries'], [])
            interventions = safe_json_loads(row['interventions'], [])
            normalized_drugs = safe_json_loads(row['normalized_drugs'], {})
            adverse_events = safe_json_loads(row['adverse_events'], [])
            meddra_codes = safe_json_loads(row['meddra_codes'], [])
            
            trial = {
                'trial_id': row['trial_id'],
                'source': row['source'],
                'title': row['title'],
                'condition': row['condition'],
                'status': row['status'],
                'phase': row['phase'],
                'study_type': row['study_type'],
                'sponsor': row['sponsor'],
                'start_date': row['start_date'],
                'completion_date': row['completion_date'],
                'enrollment': row['enrollment'],
                'url': row['url'],
                'description': row['description'],
                'eligibility_criteria': row['eligibility_criteria'],
                'countries': json.dumps(countries) if isinstance(countries, list) else countries,
                'interventions': json.dumps(interventions) if isinstance(interventions, list) else interventions,
                'normalized_drugs': json.dumps(normalized_drugs) if isinstance(normalized_drugs, dict) else normalized_drugs,
                'adverse_events': json.dumps(adverse_events) if isinstance(adverse_events, list) else adverse_events,
                'meddra_codes': json.dumps(meddra_codes) if isinstance(meddra_codes, list) else meddra_codes,
                'data_collection_date': row['data_collection_date'] or datetime.now().date().isoformat(),
                'processing_timestamp': row['processing_timestamp'] or datetime.now().isoformat()
            }
            trials_data.append(trial)
            
        except Exception as e:
            logger.error(f"Error converting trial {row['trial_id']}: {e}")
    
    print(f"   Converted {len(trials_data)} trials")
    
    # Import and use SCD2 database manager
    print("\n3. Loading trials into PostgreSQL with SCD Type 2...")
    
    try:
        from storage.scd2_database import SCD2DatabaseManager
        
        db = SCD2DatabaseManager(
            host="localhost",
            port=5432,
            database="ct_fda_pipeline",
            user="postgres",
            password="Admin"
        )
        
        # Start pipeline run
        run_id = db.start_pipeline_run(metadata={
            'migration_source': 'sqlite',
            'sqlite_path': sqlite_path,
            'migration_date': datetime.now().isoformat()
        })
        
        # Perform SCD2 upsert
        stats = db.upsert_trials_scd2(trials_data, pipeline_run_id=run_id)
        
        # Complete pipeline run
        db.complete_pipeline_run(
            run_id=run_id,
            status='SUCCESS',
            stats=stats
        )
        
        print(f"\n   Migration Results:")
        print(f"   - Inserted (new): {stats['inserted']}")
        print(f"   - Updated: {stats['updated']}")
        print(f"   - Unchanged: {stats['unchanged']}")
        print(f"   - Errors: {stats['errors']}")
        
    except Exception as e:
        print(f"   ✗ Failed to load into PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Close SQLite connection
    conn.close()
    
    print("\n" + "="*70)
    print("Migration complete!")
    print("="*70)
    print("\nYou can now query the data in pgAdmin:")
    print("  SELECT * FROM trials_scd2;")
    print("  SELECT * FROM trials_current;")


if __name__ == "__main__":
    migrate_sqlite_to_postgres()
