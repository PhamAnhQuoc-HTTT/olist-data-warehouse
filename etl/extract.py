"""
Extract module - Read raw CSV files from Kaggle Olist dataset.
"""
import os
import pandas as pd


# Mapping: logical name → CSV filename
CSV_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def extract_all(raw_data_path: str) -> dict[str, pd.DataFrame]:
    """
    Read all CSV files from raw_data_path into a dict of DataFrames.
    
    Args:
        raw_data_path: Path to folder containing raw CSV files.
        
    Returns:
        Dict mapping logical name → DataFrame.
    """
    dataframes = {}
    
    for name, filename in CSV_FILES.items():
        filepath = os.path.join(raw_data_path, filename)
        
        if not os.path.exists(filepath):
            print(f"  [WARNING] File not found: {filepath}, skipping...")
            continue
            
        df = pd.read_csv(filepath)
        print(f"  [OK] {name}: {len(df):,} rows, {len(df.columns)} columns")
        dataframes[name] = df
    
    return dataframes