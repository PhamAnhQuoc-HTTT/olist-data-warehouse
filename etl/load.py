"""
Load module - Write transformed DataFrames into PostgreSQL.

Load order: dim tables first (FK dependencies), then fact table.
Uses TRUNCATE + INSERT strategy (full refresh).
"""
import pandas as pd
from sqlalchemy import create_engine, text


def get_engine(host: str, port: str, dbname: str, user: str, password: str):
    """Create SQLAlchemy engine for PostgreSQL."""
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


def load_table(
    df: pd.DataFrame, 
    table_name: str, 
    engine,
    truncate: bool = True,
) -> None:
    """
    Load a DataFrame into a PostgreSQL table.
    
    Args:
        df: DataFrame to load.
        table_name: Target table name.
        engine: SQLAlchemy engine.
        truncate: If True, clear table before inserting.
    """
    with engine.connect() as conn:
        if truncate:
            # TRUNCATE CASCADE to handle FK dependencies
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
            conn.commit()
    
    # Use pandas to_sql with 'append' (table already exists from DDL)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",      # batch insert for performance
        chunksize=5000,       # insert 5000 rows at a time
    )
    
    # Verify row count
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
    
    print(f"  [OK] {table_name}: {count:,} rows loaded")


def load_all(
    star_schema: dict[str, pd.DataFrame],
    host: str,
    port: str,
    dbname: str,
    user: str,
    password: str,
) -> None:
    """
    Load all star schema tables into PostgreSQL.
    Order: dimensions first, then fact.
    """
    engine = get_engine(host, port, dbname, user, password)
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("  [OK] Connected to PostgreSQL")
    
    # Load dimensions first (fact has FK references to these)
    dim_tables = ["dim_date", "dim_customer", "dim_product", "dim_seller"]
    for table_name in dim_tables:
        load_table(star_schema[table_name], table_name, engine)
    
    # Load fact table last
    load_table(star_schema["fact_order_items"], "fact_order_items", engine)
    
    engine.dispose()
    print("  [OK] All tables loaded successfully")