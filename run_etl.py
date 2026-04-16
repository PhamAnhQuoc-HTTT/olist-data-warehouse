"""
Olist Data Warehouse - ETL Pipeline Runner

Usage:
    python run_etl.py

Reads raw CSV files, transforms into star schema, loads into PostgreSQL.
"""
import os
import time
from dotenv import load_dotenv

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all


def main():
    # Load environment variables
    load_dotenv()
    
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "olist_dw")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    
    if not db_password:
        raise ValueError("DB_PASSWORD not set in .env file")
    
    # Path to raw CSV files
    raw_data_path = os.path.join(os.path.dirname(__file__), "data", "raw")
    
    print("=" * 50)
    print("OLIST DATA WAREHOUSE - ETL PIPELINE")
    print("=" * 50)
    
    start_time = time.time()
    
    # ── EXTRACT ──────────────────────────────────
    print("\n[1/3] EXTRACT - Reading CSV files...")
    raw_dfs = extract_all(raw_data_path)
    print(f"  Extracted {len(raw_dfs)} tables")
    
    # ── TRANSFORM ────────────────────────────────
    print("\n[2/3] TRANSFORM - Building star schema...")
    star_schema = transform_all(raw_dfs)
    print(f"  Transformed {len(star_schema)} tables")
    
    # ── LOAD ─────────────────────────────────────
    print("\n[3/3] LOAD - Writing to PostgreSQL...")
    load_all(
        star_schema=star_schema,
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )
    
    elapsed = time.time() - start_time
    print(f"\n{'=' * 50}")
    print(f"ETL COMPLETE in {elapsed:.1f} seconds")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()