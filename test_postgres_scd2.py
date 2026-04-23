"""
Test PostgreSQL SCD Type 2 connection and functionality
"""
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_connection():
    """Test PostgreSQL connection"""
    print("\n" + "="*80)
    print("Testing PostgreSQL Connection")
    print("="*80)
    
    try:
        from storage.scd2_database import SCD2DatabaseManager
        
        db = SCD2DatabaseManager(
            host="localhost",
            port=5432,
            database="ct_fda_pipeline",
            user="postgres",
            password="Admin"
        )
        
        print("✓ PostgreSQL connection successful!")
        print("✓ SCD Type 2 schema initialized!")
        return db
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nMake sure:")
        print("  1. PostgreSQL is running on localhost:5432")
        print("  2. Database 'ct_fda_pipeline' exists")
        print("  3. User 'postgres' with password 'Admin' has access")
        print("\nTo create the database, run:")
        print("  CREATE DATABASE ct_fda_pipeline;")
        return None


def test_scd2_operations(db):
    """Test SCD Type 2 insert and update operations"""
    print("\n" + "="*80)
    print("Testing SCD Type 2 Operations")
    print("="*80)
    
    # Sample trial data
    trial_v1 = {
        'trial_id': 'TEST-SCD2-001',
        'source': 'test',
        'title': 'Test Clinical Trial for SCD2 Validation',
        'condition': 'Test Condition',
        'status': 'RECRUITING',
        'phase': 'Phase 2',
        'study_type': 'INTERVENTIONAL',
        'sponsor': 'Test Sponsor Inc.',
        'start_date': '2026-01-01',
        'completion_date': None,
        'enrollment': 50,
        'url': 'https://example.com/test-trial',
        'description': 'Initial trial description',
        'eligibility_criteria': 'Age 18+',
        'countries': 'United States',
        'interventions': '["Drug A", "Placebo"]',
        'data_collection_date': datetime.now().date().isoformat(),
        'processing_timestamp': datetime.now().isoformat()
    }
    
    # Version 1: Insert
    print("\n1. Inserting new trial (Version 1)...")
    stats = db.upsert_trials_scd2([trial_v1])
    print(f"   Result: {stats}")
    assert stats['inserted'] == 1, "Expected 1 insert"
    print("   ✓ Insert successful!")
    
    # Version 2: Update status
    print("\n2. Updating trial status (Version 2)...")
    trial_v2 = trial_v1.copy()
    trial_v2['status'] = 'ACTIVE'
    trial_v2['enrollment'] = 75
    stats = db.upsert_trials_scd2([trial_v2])
    print(f"   Result: {stats}")
    assert stats['updated'] == 1, "Expected 1 update"
    print("   ✓ Update successful!")
    
    # Version 3: Update to completed
    print("\n3. Completing trial (Version 3)...")
    trial_v3 = trial_v2.copy()
    trial_v3['status'] = 'COMPLETED'
    trial_v3['completion_date'] = '2026-06-30'
    trial_v3['enrollment'] = 100
    stats = db.upsert_trials_scd2([trial_v3])
    print(f"   Result: {stats}")
    assert stats['updated'] == 1, "Expected 1 update"
    print("   ✓ Completion update successful!")
    
    # No change
    print("\n4. Submitting same data (no change expected)...")
    stats = db.upsert_trials_scd2([trial_v3])
    print(f"   Result: {stats}")
    assert stats['unchanged'] == 1, "Expected 1 unchanged"
    print("   ✓ No-change detection working!")
    
    return True


def test_history_queries(db):
    """Test historical query capabilities"""
    print("\n" + "="*80)
    print("Testing Historical Queries")
    print("="*80)
    
    trial_id = 'TEST-SCD2-001'
    
    # Get full history
    print(f"\n1. Getting full history for {trial_id}...")
    history = db.get_trial_history(trial_id)
    print(f"   Found {len(history)} versions")
    
    for record in history:
        print(f"   - Version {record['version']}: {record['status']} "
              f"(enrollment: {record['enrollment']}, current: {record['is_current']})")
    
    assert len(history) >= 3, "Expected at least 3 versions"
    print("   ✓ History query successful!")
    
    # Get current state
    print("\n2. Getting current state...")
    current = db.get_current_trials(limit=10)
    test_current = [t for t in current if t['trial_id'] == trial_id]
    
    if test_current:
        print(f"   Current status: {test_current[0]['status']}")
        print(f"   Current enrollment: {test_current[0]['enrollment']}")
        assert test_current[0]['status'] == 'COMPLETED', "Expected COMPLETED status"
        print("   ✓ Current state query successful!")
    
    return True


def test_pipeline_run_tracking(db):
    """Test pipeline run tracking"""
    print("\n" + "="*80)
    print("Testing Pipeline Run Tracking")
    print("="*80)
    
    # Start a run
    print("\n1. Starting pipeline run...")
    run_id = db.start_pipeline_run(metadata={'test': True})
    print(f"   Run ID: {run_id}")
    
    # Complete the run
    print("\n2. Completing pipeline run...")
    db.complete_pipeline_run(
        run_id=run_id,
        status='SUCCESS',
        stats={'inserted': 10, 'updated': 5, 'unchanged': 85},
        duration_seconds=123.45
    )
    print("   ✓ Run completed!")
    
    # Get runs
    print("\n3. Fetching recent runs...")
    runs = db.get_pipeline_runs(limit=5)
    print(f"   Found {len(runs)} recent runs")
    
    for run in runs:
        print(f"   - Run {run['run_id']}: {run['status']} "
              f"(inserted: {run['trials_inserted']}, updated: {run['trials_updated']})")
    
    return True


def test_change_summary(db):
    """Test change summary"""
    print("\n" + "="*80)
    print("Testing Change Summary")
    print("="*80)
    
    summary = db.get_change_summary(days=30)
    print(f"\nChanges in last {summary['period_days']} days:")
    print(f"  By type: {summary['changes_by_type']}")
    print(f"  Frequently changing: {len(summary['frequently_changing_trials'])} trials")
    
    return True


def main():
    """Run all tests"""
    print("\n" + "#"*80)
    print("# PostgreSQL SCD Type 2 Test Suite")
    print("#"*80)
    
    # Test connection
    db = test_connection()
    if not db:
        return False
    
    try:
        # Test SCD2 operations
        test_scd2_operations(db)
        
        # Test history queries
        test_history_queries(db)
        
        # Test pipeline run tracking
        test_pipeline_run_tracking(db)
        
        # Test change summary
        test_change_summary(db)
        
        print("\n" + "="*80)
        print("ALL TESTS PASSED!")
        print("="*80)
        print("\nYour PostgreSQL SCD Type 2 setup is ready for production.")
        print("\nNext steps:")
        print("  1. Install psycopg2: pip install psycopg2-binary")
        print("  2. Start Airflow: docker-compose up -d")
        print("  3. Access Airflow UI: http://localhost:8080")
        print("  4. Enable the clinical_trials_pipeline DAG")
        return True
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
