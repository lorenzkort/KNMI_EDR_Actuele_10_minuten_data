import pandas as pd
import os
import hashlib
import psycopg2
from psycopg2 import sql
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from src.column_metadata import column_mapping, column_renaming
from psycopg2 import extras
from src.logging_config import setup_logging

logger = setup_logging(__name__)

def calculate_row_hash(row):
    """Calculate hash of row values to detect duplicates"""
    row_str = ''.join(str(val) for val in row)
    return hashlib.md5(row_str.encode()).hexdigest()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_db_connection():
    """Create database connection with retry logic"""
    try:
        conn = psycopg2.connect(
            dbname="weather_data",  # First connect to default database
            user="postgres", 
            password="secret",
            host="postgres",
            port=5432
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create weather_db if it doesn't exist
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'weather_db'")
        if not cur.fetchone():
            cur.execute("CREATE DATABASE weather_db")
        
        # Close connection to default database
        cur.close()
        conn.close()
        
        # Connect to weather_db
        return psycopg2.connect(
            dbname="weather_db",
            user="postgres", 
            password="secret",
            host="postgres",
            port=5432
        )
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def create_table_if_not_exists(conn):
    """Create weather data table if it doesn't exist with proper column types"""
    with conn.cursor() as cur:
        try:
            # First create the TimescaleDB extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            
            # Drop the table if it exists (during development)
            cur.execute("DROP TABLE IF EXISTS weather_data;")
            
            # Create column definitions based on metadata
            column_definitions = []
            for original_name, data_type in column_mapping.items():
                column_name = column_renaming.get(original_name, original_name)
                # Convert metadata types to PostgreSQL types
                pg_type = 'DOUBLE PRECISION' if data_type == 'Float64' else 'TEXT'
                column_definitions.append(f"{column_name} {pg_type}")

            # Join all column definitions
            columns_sql = ",\n                ".join(column_definitions)
            
            # Create the table without any unique constraints initially
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS weather_data (
                    id SERIAL,
                    row_hash TEXT,
                    ingestion_timestamp TIMESTAMPTZ NOT NULL,
                    {columns_sql}
                );
            """)
            
            # Convert to hypertable first
            cur.execute("""
                SELECT create_hypertable('weather_data', 'ingestion_timestamp', 
                                       if_not_exists => TRUE,
                                       migrate_data => TRUE);
            """)
            
            # Now create the indexes after hypertable conversion
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_weather_data_row_hash 
                ON weather_data (row_hash, ingestion_timestamp);
            """)
            
            conn.commit()
            logger.info("Successfully created or updated weather_data table")
            
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            conn.rollback()
            raise

def ingest_parquet_files(input_dir="./parquet_files_validated/"):
    """
    Ingest validated parquet files into TimescaleDB
    """
    conn = None
    try:
        conn = get_db_connection()
        create_table_if_not_exists(conn)
        
        for filename in os.listdir(input_dir):
            if filename.endswith('.parquet'):
                logger.info(f"Processing file for ingestion: {filename}")
                input_path = os.path.join(input_dir, filename)
                
                try:
                    # Read parquet file
                    df = pd.read_parquet(input_path)
                    # Rename columns according to the mapping
                    df = df.rename(columns=column_renaming)
                    
                    # Process rows in batches for better performance
                    rows_to_insert = []
                    existing_hashes = set()
                    
                    # First get all existing hashes for this batch
                    with conn.cursor() as cur:
                        hashes = [calculate_row_hash(row) for _, row in df.iterrows()]
                        cur.execute(
                            "SELECT row_hash FROM weather_data WHERE row_hash = ANY(%s)",
                            (hashes,)
                        )
                        existing_hashes = {row[0] for row in cur.fetchall()}
                    
                    # Prepare rows for insertion
                    for _, row in df.iterrows():
                        row_dict = row.to_dict()
                        row_hash = calculate_row_hash(row)
                        
                        if row_hash not in existing_hashes:
                            rows_to_insert.append(
                                (row_hash, datetime.now()) + tuple(row_dict.values())
                            )
                    
                    if rows_to_insert:
                        with conn.cursor() as cur:
                            columns = ['row_hash', 'ingestion_timestamp'] + list(row_dict.keys())
                            placeholders = ','.join(['%s'] * len(columns))
                            
                            extras.execute_batch(cur, f"""
                                INSERT INTO weather_data ({', '.join(columns)})
                                VALUES ({placeholders})
                            """, rows_to_insert)
                        
                        conn.commit()
                        logger.info(f"Successfully ingested {len(rows_to_insert)} rows from {filename}")
                    else:
                        logger.info(f"No new rows to insert from {filename}")
                    
                except Exception as e:
                    logger.error(f"Error ingesting {filename}: {str(e)}")
                    if conn:
                        conn.rollback()
                    continue
        
    finally:
        if conn:
            conn.close()

def main():
    ingest_parquet_files()

if __name__ == "__main__":
    main()
