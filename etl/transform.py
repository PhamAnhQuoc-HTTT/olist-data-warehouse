"""
Transform module - Convert raw DataFrames into Star Schema tables.

Transformations:
  - dim_date:     Generated from date range in orders
  - dim_customer: Cleaned from customers dataset
  - dim_product:  Merged products + category translation
  - dim_seller:   Cleaned from sellers dataset
  - fact_order_items: Joined from order_items + orders + reviews
"""
import pandas as pd
import hashlib


# ─────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────

def _generate_sk(*values) -> str:
    """Generate a surrogate key by hashing concatenated values."""
    raw = "|".join(str(v) for v in values)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _to_date_sk(dt: pd.Timestamp) -> int:
    """Convert timestamp to date_sk format YYYYMMDD."""
    if pd.isna(dt):
        return None
    return int(dt.strftime("%Y%m%d"))


# ─────────────────────────────────────────────
# Dimension transforms
# ─────────────────────────────────────────────

def transform_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    """Generate dim_date from min/max purchase date in orders."""
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"]
    )
    
    min_date = orders["order_purchase_timestamp"].min().normalize()
    max_date = orders["order_purchase_timestamp"].max().normalize()
    
    date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    
    dim_date = pd.DataFrame({
        "date_sk": [int(d.strftime("%Y%m%d")) for d in date_range],
        "full_date": date_range.date,
        "year": date_range.year,
        "quarter": date_range.quarter,
        "month": date_range.month,
        "day_of_week": date_range.dayofweek,  # 0=Mon, 6=Sun
        "month_name": date_range.strftime("%B"),
        "quarter_name": ["Q" + str(q) for q in date_range.quarter],
        "is_weekend": date_range.dayofweek >= 5,
    })
    
    print(f"  [OK] dim_date: {len(dim_date):,} rows ({min_date.date()} → {max_date.date()})")
    return dim_date


def transform_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare dim_customer."""
    dim_customer = customers.copy()
    
    # Generate surrogate key
    dim_customer["customer_sk"] = dim_customer["customer_id"].apply(
        lambda x: _generate_sk("cust", x)
    )
    
    # Select and rename columns
    dim_customer = dim_customer[[
        "customer_sk",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
    ]]
    
    # Clean: strip whitespace, title case city names
    dim_customer["customer_city"] = (
        dim_customer["customer_city"].str.strip().str.title()
    )
    
    print(f"  [OK] dim_customer: {len(dim_customer):,} rows")
    return dim_customer


def transform_dim_product(
    products: pd.DataFrame, 
    category_translation: pd.DataFrame
) -> pd.DataFrame:
    """Merge products with category translation."""
    dim_product = products.merge(
        category_translation,
        on="product_category_name",
        how="left",
    )
    
    # Generate surrogate key
    dim_product["product_sk"] = dim_product["product_id"].apply(
        lambda x: _generate_sk("prod", x)
    )
    
    # Select and rename
    dim_product = dim_product[[
        "product_sk",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_lenght",          # typo in original dataset
        "product_description_lenght",   # typo in original dataset
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]]
    
    # Fix column name typos from original dataset
    dim_product = dim_product.rename(columns={
        "product_category_name_english": "product_category_english",
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    })
    
    print(f"  [OK] dim_product: {len(dim_product):,} rows")
    return dim_product


def transform_dim_seller(sellers: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare dim_seller."""
    dim_seller = sellers.copy()
    
    # Generate surrogate key
    dim_seller["seller_sk"] = dim_seller["seller_id"].apply(
        lambda x: _generate_sk("sell", x)
    )
    
    dim_seller = dim_seller[[
        "seller_sk",
        "seller_id",
        "seller_city",
        "seller_state",
        "seller_zip_code_prefix",
    ]]
    
    # Clean city names
    dim_seller["seller_city"] = (
        dim_seller["seller_city"].str.strip().str.title()
    )
    
    print(f"  [OK] dim_seller: {len(dim_seller):,} rows")
    return dim_seller


# ─────────────────────────────────────────────
# Fact transform
# ─────────────────────────────────────────────

def transform_fact_order_items(
    order_items: pd.DataFrame,
    orders: pd.DataFrame,
    order_reviews: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_seller: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build fact_order_items by joining:
      order_items + orders + reviews
    Then map to surrogate keys from dim tables.
    """
    # --- Parse timestamps ---
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in timestamp_cols:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")
    
    # --- Deduplicate reviews (keep first review per order) ---
    reviews_dedup = (
        order_reviews
        .sort_values("review_creation_date")
        .drop_duplicates(subset="order_id", keep="first")
        [["order_id", "review_score"]]
    )
    
    # --- Join order_items → orders → reviews ---
    fact = (
        order_items
        .merge(orders, on="order_id", how="inner")
        .merge(reviews_dedup, on="order_id", how="left")
    )
    
    # --- Calculate delivery metrics ---
    fact["delivery_days_actual"] = (
        fact["order_delivered_customer_date"] - fact["order_purchase_timestamp"]
    ).dt.days
    
    fact["delivery_days_estimated"] = (
        fact["order_estimated_delivery_date"] - fact["order_purchase_timestamp"]
    ).dt.days
    
    fact["delivery_delay_days"] = (
        fact["delivery_days_actual"] - fact["delivery_days_estimated"]
    )
    
    # --- Map surrogate keys ---
    # date_sk from purchase date
    fact["date_sk"] = fact["order_purchase_timestamp"].apply(_to_date_sk)
    
    # customer_sk: join via customer_id
    customer_sk_map = dim_customer.set_index("customer_id")["customer_sk"]
    fact["customer_sk"] = fact["customer_id"].map(customer_sk_map)
    
    # product_sk: join via product_id
    product_sk_map = dim_product.set_index("product_id")["product_sk"]
    fact["product_sk"] = fact["product_id"].map(product_sk_map)
    
    # seller_sk: join via seller_id
    seller_sk_map = dim_seller.set_index("seller_id")["seller_sk"]
    fact["seller_sk"] = fact["seller_id"].map(seller_sk_map)
    
    # --- Generate fact surrogate key ---
    fact["order_item_sk"] = fact.apply(
        lambda row: _generate_sk(row["order_id"], row["order_item_id"]),
        axis=1,
    )
    
    # --- Select final columns ---
    fact_final = fact[[
        "order_item_sk",
        "date_sk",
        "customer_sk",
        "product_sk",
        "seller_sk",
        "order_id",
        "order_item_id",
        "price",
        "freight_value",
        "review_score",
        "delivery_days_actual",
        "delivery_days_estimated",
        "delivery_delay_days",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]].rename(columns={
        "order_purchase_timestamp": "purchase_timestamp",
        "order_delivered_customer_date": "delivered_timestamp",
        "order_estimated_delivery_date": "estimated_delivery",
    })
    
    print(f"  [OK] fact_order_items: {len(fact_final):,} rows")
    return fact_final


# ─────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────

def transform_all(raw_dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Run all transformations and return dict of star schema DataFrames.
    """
    # Dimensions
    dim_date = transform_dim_date(raw_dfs["orders"])
    dim_customer = transform_dim_customer(raw_dfs["customers"])
    dim_product = transform_dim_product(
        raw_dfs["products"], raw_dfs["category_translation"]
    )
    dim_seller = transform_dim_seller(raw_dfs["sellers"])
    
    # Fact (needs dim tables for SK mapping)
    fact_order_items = transform_fact_order_items(
        order_items=raw_dfs["order_items"],
        orders=raw_dfs["orders"],
        order_reviews=raw_dfs["order_reviews"],
        dim_customer=dim_customer,
        dim_product=dim_product,
        dim_seller=dim_seller,
    )
    
    return {
        "dim_date": dim_date,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "fact_order_items": fact_order_items,
    }