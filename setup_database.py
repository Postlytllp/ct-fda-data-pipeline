"""
Database Setup Script
Creates the PostgreSQL database for clinical trials pipeline
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database():
    """Create the ct_fda_pipeline database if it doesn't exist"""
    print("="*60)
    print("PostgreSQL Database Setup")
    print("="*60)
    
    # Connection parameters for postgres system database
    conn_params = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "Admin",
        "database": "postgres"  # Connect to default postgres database first
    }
    
    try:
        # Connect to PostgreSQL server
        print("\n1. Connecting to PostgreSQL server...")
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print("   ✓ Connected successfully!")
        
        # Check if database exists
        print("\n2. Checking if 'ct_fda_pipeline' database exists...")
        cursor.execute("""
            SELECT 1 FROM pg_catalog.pg_database 
            WHERE datname = 'ct_fda_pipeline'
        """)
        exists = cursor.fetchone()
        
        if exists:
            print("   ✓ Database 'ct_fda_pipeline' already exists!")
        else:
            print("   Database does not exist. Creating...")
            cursor.execute("CREATE DATABASE ct_fda_pipeline")
            print("   ✓ Database 'ct_fda_pipeline' created successfully!")
        
        cursor.close()
        conn.close()
        
        # Now connect to the new database and create Airflow database
        print("\n3. Creating 'airflow' database for Airflow metadata...")
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 1 FROM pg_catalog.pg_database 
            WHERE datname = 'airflow'
        """)
        exists = cursor.fetchone()
        
        if exists:
            print("   ✓ Database 'airflow' already exists!")
        else:
            cursor.execute("CREATE DATABASE airflow")
            print("   ✓ Database 'airflow' created successfully!")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*60)
        print("Database setup complete!")
        print("="*60)
        print("\nYou can now run: python test_postgres_scd2.py")
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"\n✗ Failed to connect to PostgreSQL: {e}")
        print("\nPlease ensure:")
        print("  1. PostgreSQL is running on localhost:5432")
        print("  2. User 'postgres' exists with password 'Admin'")
        print("  3. PostgreSQL is accepting connections")
        return False
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False


if __name__ == "__main__":
    create_database()
