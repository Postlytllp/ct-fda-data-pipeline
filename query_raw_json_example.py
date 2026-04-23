#!/usr/bin/env python3
"""
Example: Query full ClinicalTrials.gov raw JSON from SQLite database
"""

import sqlite3
import json
import sys

def query_raw_json(db_path='data/clinical_trials.db', trial_id=None):
    """Query and display raw ClinicalTrials.gov JSON from database"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if trial_id:
        # Query specific trial
        cursor.execute('''
            SELECT trial_id, source, title, raw_source_record 
            FROM trials 
            WHERE trial_id = ? AND raw_source_record IS NOT NULL
        ''', (trial_id,))
    else:
        # Query all trials with raw JSON
        cursor.execute('''
            SELECT trial_id, source, title, LENGTH(raw_source_record) as json_size
            FROM trials 
            WHERE raw_source_record IS NOT NULL
            ORDER BY id DESC
            LIMIT 10
        ''')
    
    rows = cursor.fetchall()
    
    if not rows:
        print(f"No trials found{' with ID: ' + trial_id if trial_id else ''}")
        conn.close()
        return
    
    if trial_id:
        # Display full JSON for specific trial
        trial_id, source, title, raw_json_str = rows[0]
        print(f"Trial ID: {trial_id}")
        print(f"Source: {source}")
        print(f"Title: {title[:80]}...")
        print("\nFull ClinicalTrials.gov v2 JSON:")
        print("=" * 80)
        
        raw_json = json.loads(raw_json_str)
        print(json.dumps(raw_json, indent=2))
        
        # Show available sections
        print("\n" + "=" * 80)
        print("Available JSON sections:")
        for section in raw_json.keys():
            print(f"  - {section}")
            
    else:
        # List all trials with raw JSON
        print("Trials with full ClinicalTrials.gov raw JSON:\n")
        print(f"{'Trial ID':<15} {'Source':<20} {'JSON Size':<12} Title")
        print("-" * 100)
        for row in rows:
            trial_id, source, title, json_size = row
            print(f"{trial_id:<15} {source:<20} {json_size:>10}B  {title[:50]}")
    
    conn.close()

def query_specific_field(db_path='data/clinical_trials.db', trial_id=None, json_path=None):
    """Query specific field from raw JSON using JSON path
    
    Example: Get derivedSection.conditionBrowseModule
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT trial_id, raw_source_record 
        FROM trials 
        WHERE trial_id = ? AND raw_source_record IS NOT NULL
    ''', (trial_id,))
    
    row = cursor.fetchone()
    if not row:
        print(f"Trial {trial_id} not found or has no raw JSON")
        conn.close()
        return
    
    trial_id, raw_json_str = row
    raw_json = json.loads(raw_json_str)
    
    # Navigate JSON path
    parts = json_path.split('.')
    current = raw_json
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            print(f"Path '{json_path}' not found in JSON")
            conn.close()
            return
    
    print(f"Trial: {trial_id}")
    print(f"Path: {json_path}")
    print(json.dumps(current, indent=2))
    
    conn.close()

if __name__ == "__main__":
    print("=" * 80)
    print("ClinicalTrials.gov Raw JSON Database Query Examples")
    print("=" * 80)
    
    # Example 1: List all trials with raw JSON
    print("\n1. List all trials with raw JSON stored:")
    query_raw_json()
    
    # Example 2: Get specific trial's full JSON
    print("\n\n2. Get full JSON for a specific trial (NCT00052052):")
    query_raw_json(trial_id='NCT00052052')
    
    # Example 3: Query specific field
    print("\n\n3. Query specific field from raw JSON:")
    print("   (e.g., derivedSection from NCT00052052)")
    query_specific_field(trial_id='NCT00052052', json_path='derivedSection')
    
    print("\n" + "=" * 80)
    print("\nUsage examples:")
    print("  python query_raw_json_example.py")
    print("\nFor custom queries, use sqlite3 directly:")
    print("  sqlite3 data/clinical_trials.db")
    print("  SELECT trial_id, json_extract(raw_source_record, '$.protocolSection.identificationModule.nctId') FROM trials;")
