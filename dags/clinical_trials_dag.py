"""
Airflow DAG for Clinical Trials Data Pipeline
Collects, processes, and stores clinical trial data with SCD Type 2
"""

from datetime import datetime, timedelta
import json
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


# Default arguments for all tasks
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


def collect_us_trials(**context):
    """Collect trials from ClinicalTrials.gov"""
    from data_sources.clinical_trials_gov import ClinicalTrialsGovClient
    from dataclasses import asdict
    
    limit = context['dag_run'].conf.get('us_limit', 100) if context['dag_run'].conf else 100
    
    client = ClinicalTrialsGovClient()
    trials = client.search_ipf_trials(limit=limit)
    
    # Convert to serializable format
    trials_data = []
    for trial in trials:
        trial_dict = asdict(trial)
        trials_data.append(trial_dict)
    
    # Push to XCom
    context['ti'].xcom_push(key='us_trials', value=trials_data)
    context['ti'].xcom_push(key='us_trials_count', value=len(trials_data))
    
    print(f"Collected {len(trials_data)} US trials")
    return len(trials_data)


def collect_eu_trials(**context):
    """Collect trials from EU Clinical Trials (CTIS)"""
    from data_sources.eu_clinical_trials import EUClinicalTrialsClient
    from dataclasses import asdict
    
    max_pages = context['dag_run'].conf.get('eu_max_pages', 5) if context['dag_run'].conf else 5
    
    client = EUClinicalTrialsClient()
    trials = client.search_ipf_trials(max_pages=max_pages)
    
    # Convert to serializable format
    trials_data = []
    for trial in trials:
        trial_dict = asdict(trial)
        # Handle non-serializable fields
        if 'raw_trial' in trial_dict:
            trial_dict['raw_trial'] = None  # Remove large raw data
        trials_data.append(trial_dict)
    
    # Push to XCom
    context['ti'].xcom_push(key='eu_trials', value=trials_data)
    context['ti'].xcom_push(key='eu_trials_count', value=len(trials_data))
    
    print(f"Collected {len(trials_data)} EU trials")
    return len(trials_data)


def process_trials(**context):
    """Process and enrich trial data"""
    from pipeline.data_processor import DataProcessor
    from dataclasses import asdict
    
    ti = context['ti']
    
    # Pull trials from XCom
    us_trials = ti.xcom_pull(key='us_trials', task_ids='collect_us_trials') or []
    eu_trials = ti.xcom_pull(key='eu_trials', task_ids='collect_eu_trials') or []
    
    print(f"Processing {len(us_trials)} US trials and {len(eu_trials)} EU trials")
    
    processor = DataProcessor()
    enriched_trials = []
    
    # Process US trials
    for trial in us_trials:
        try:
            # Reconstruct dataclass-like object
            from data_sources.clinical_trials_gov import ClinicalTrial
            trial_obj = ClinicalTrial(**{k: v for k, v in trial.items() if k in ClinicalTrial.__dataclass_fields__})
            enriched = processor._enrich_us_trial(trial_obj)
            if enriched:
                enriched_trials.append(asdict(enriched))
        except Exception as e:
            print(f"Error processing US trial {trial.get('nct_id', 'unknown')}: {e}")
    
    # Process EU trials
    for trial in eu_trials:
        try:
            from data_sources.eu_clinical_trials import EUClinicalTrial
            trial_obj = EUClinicalTrial(**{k: v for k, v in trial.items() if k in EUClinicalTrial.__dataclass_fields__})
            enriched = processor._enrich_eu_trial(trial_obj)
            if enriched:
                enriched_trials.append(asdict(enriched))
        except Exception as e:
            print(f"Error processing EU trial {trial.get('eudract_number', 'unknown')}: {e}")
    
    # Push enriched trials
    ti.xcom_push(key='enriched_trials', value=enriched_trials)
    ti.xcom_push(key='enriched_count', value=len(enriched_trials))
    
    print(f"Successfully enriched {len(enriched_trials)} trials")
    return len(enriched_trials)


def load_with_scd2(**context):
    """Load enriched trials using SCD Type 2"""
    from storage.scd2_database import SCD2DatabaseManager
    import time
    
    ti = context['ti']
    start_time = time.time()
    
    # Pull enriched trials
    enriched_trials = ti.xcom_pull(key='enriched_trials', task_ids='process_trials') or []
    us_count = ti.xcom_pull(key='us_trials_count', task_ids='collect_us_trials') or 0
    eu_count = ti.xcom_pull(key='eu_trials_count', task_ids='collect_eu_trials') or 0
    
    print(f"Loading {len(enriched_trials)} trials to PostgreSQL with SCD Type 2")
    
    # Initialize database manager
    db = SCD2DatabaseManager(
        host="localhost",
        port=5432,
        database="ct_fda_pipeline",
        user="postgres",
        password="Admin"
    )
    
    # Start pipeline run tracking
    run_id = db.start_pipeline_run(metadata={
        'dag_id': context['dag'].dag_id,
        'run_id': str(context['run_id']),
        'execution_date': str(context['execution_date'])
    })
    
    try:
        # Perform SCD2 upsert
        stats = db.upsert_trials_scd2(enriched_trials, pipeline_run_id=run_id)
        
        # Add fetch counts to stats
        stats['us_trials_fetched'] = us_count
        stats['eu_trials_fetched'] = eu_count
        
        duration = time.time() - start_time
        
        # Complete pipeline run
        db.complete_pipeline_run(
            run_id=run_id,
            status='SUCCESS',
            stats=stats,
            duration_seconds=duration
        )
        
        # Push stats for reporting
        ti.xcom_push(key='load_stats', value=stats)
        ti.xcom_push(key='pipeline_run_id', value=run_id)
        
        print(f"SCD2 Load complete: {stats}")
        return stats
        
    except Exception as e:
        duration = time.time() - start_time
        db.complete_pipeline_run(
            run_id=run_id,
            status='FAILED',
            stats={'error': str(e)},
            duration_seconds=duration,
            error_message=str(e)
        )
        raise


def generate_report(**context):
    """Generate pipeline execution report"""
    ti = context['ti']
    
    # Pull all stats
    us_count = ti.xcom_pull(key='us_trials_count', task_ids='collect_us_trials') or 0
    eu_count = ti.xcom_pull(key='eu_trials_count', task_ids='collect_eu_trials') or 0
    enriched_count = ti.xcom_pull(key='enriched_count', task_ids='process_trials') or 0
    load_stats = ti.xcom_pull(key='load_stats', task_ids='load_with_scd2') or {}
    run_id = ti.xcom_pull(key='pipeline_run_id', task_ids='load_with_scd2')
    
    report = f"""
    ========================================
    Clinical Trials Pipeline Report
    ========================================
    Execution Date: {context['execution_date']}
    Pipeline Run ID: {run_id}
    
    DATA COLLECTION
    ---------------
    US Trials Fetched: {us_count}
    EU Trials Fetched: {eu_count}
    Total Collected: {us_count + eu_count}
    
    PROCESSING
    ----------
    Trials Enriched: {enriched_count}
    
    SCD TYPE 2 LOAD
    ---------------
    New Trials (Inserted): {load_stats.get('inserted', 0)}
    Updated Trials: {load_stats.get('updated', 0)}
    Unchanged Trials: {load_stats.get('unchanged', 0)}
    Errors: {load_stats.get('errors', 0)}
    
    ========================================
    """
    
    print(report)
    
    # Save report to file
    report_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'reports')
    os.makedirs(report_dir, exist_ok=True)
    
    report_file = os.path.join(
        report_dir, 
        f"pipeline_report_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.txt"
    )
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"Report saved to: {report_file}")
    return report


def check_data_quality(**context):
    """Run data quality checks on loaded data"""
    from storage.scd2_database import SCD2DatabaseManager
    
    db = SCD2DatabaseManager(
        host="localhost",
        port=5432,
        database="ct_fda_pipeline",
        user="postgres",
        password="Admin"
    )
    
    # Get current trials
    current_trials = db.get_current_trials(limit=10000)
    
    quality_issues = []
    
    for trial in current_trials:
        trial_id = trial.get('trial_id')
        
        # Check for missing critical fields
        if not trial.get('title'):
            quality_issues.append(f"{trial_id}: Missing title")
        
        if not trial.get('status'):
            quality_issues.append(f"{trial_id}: Missing status")
        
        if not trial.get('condition'):
            quality_issues.append(f"{trial_id}: Missing condition")
    
    quality_report = {
        'total_current_trials': len(current_trials),
        'issues_found': len(quality_issues),
        'issues': quality_issues[:50]  # Limit to first 50
    }
    
    print(f"Data Quality Check: {len(current_trials)} trials, {len(quality_issues)} issues")
    
    if quality_issues:
        print("Sample issues:")
        for issue in quality_issues[:10]:
            print(f"  - {issue}")
    
    context['ti'].xcom_push(key='quality_report', value=quality_report)
    return quality_report


# Create the DAG
with DAG(
    'clinical_trials_pipeline',
    default_args=default_args,
    description='Collect, process, and store clinical trials data with SCD Type 2',
    schedule_interval='0 6 * * *',  # Daily at 6 AM UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['clinical_trials', 'etl', 'scd2'],
) as dag:
    
    # Task 1: Collect US trials
    collect_us = PythonOperator(
        task_id='collect_us_trials',
        python_callable=collect_us_trials,
        provide_context=True,
    )
    
    # Task 2: Collect EU trials (parallel with US)
    collect_eu = PythonOperator(
        task_id='collect_eu_trials',
        python_callable=collect_eu_trials,
        provide_context=True,
    )
    
    # Task 3: Process and enrich trials
    process = PythonOperator(
        task_id='process_trials',
        python_callable=process_trials,
        provide_context=True,
    )
    
    # Task 4: Load with SCD Type 2
    load = PythonOperator(
        task_id='load_with_scd2',
        python_callable=load_with_scd2,
        provide_context=True,
    )
    
    # Task 5: Data quality check
    quality_check = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Task 6: Generate report
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    # Define task dependencies
    # Collect US and EU in parallel, then process, load, quality check, and report
    [collect_us, collect_eu] >> process >> load >> quality_check >> report


# Additional DAG for weekly full refresh (optional)
with DAG(
    'clinical_trials_weekly_full_refresh',
    default_args=default_args,
    description='Weekly full refresh of clinical trials data',
    schedule_interval='0 2 * * 0',  # Sundays at 2 AM UTC
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['clinical_trials', 'etl', 'full_refresh'],
) as dag_weekly:
    
    def full_refresh_collect(**context):
        """Collect all trials with higher limits"""
        context['dag_run'].conf = context['dag_run'].conf or {}
        context['dag_run'].conf['us_limit'] = 1000
        context['dag_run'].conf['eu_max_pages'] = 20
        
        # Reuse collection functions
        collect_us_trials(**context)
        collect_eu_trials(**context)
    
    full_collect = PythonOperator(
        task_id='full_refresh_collect',
        python_callable=full_refresh_collect,
        provide_context=True,
    )
    
    full_process = PythonOperator(
        task_id='full_refresh_process',
        python_callable=process_trials,
        provide_context=True,
    )
    
    full_load = PythonOperator(
        task_id='full_refresh_load',
        python_callable=load_with_scd2,
        provide_context=True,
    )
    
    full_report = PythonOperator(
        task_id='full_refresh_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    full_collect >> full_process >> full_load >> full_report
