-- =============================================
-- OLIST DATA WAREHOUSE - DDL (Star Schema)
-- Database: olist_dw
-- =============================================

-- Dimension: Date
CREATE TABLE dim_date (
    date_sk         INT PRIMARY KEY,            -- YYYYMMDD format
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    day_of_week     INT NOT NULL,               -- 0=Mon, 6=Sun
    month_name      VARCHAR(20) NOT NULL,
    quarter_name    VARCHAR(5) NOT NULL,         -- Q1, Q2, Q3, Q4
    is_weekend      BOOLEAN NOT NULL
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_sk             VARCHAR(50) PRIMARY KEY,
    customer_id             VARCHAR(50) NOT NULL,
    customer_unique_id      VARCHAR(50) NOT NULL,
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(5),
    customer_zip_code_prefix VARCHAR(10)
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_sk                  VARCHAR(50) PRIMARY KEY,
    product_id                  VARCHAR(50) NOT NULL,
    product_category_name       VARCHAR(100),
    product_category_english    VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            FLOAT,
    product_length_cm           FLOAT,
    product_height_cm           FLOAT,
    product_width_cm            FLOAT
);

-- Dimension: Seller
CREATE TABLE dim_seller (
    seller_sk               VARCHAR(50) PRIMARY KEY,
    seller_id               VARCHAR(50) NOT NULL,
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5),
    seller_zip_code_prefix  VARCHAR(10)
);

-- Fact: Order Items
CREATE TABLE fact_order_items (
    order_item_sk           VARCHAR(100) PRIMARY KEY,
    date_sk                 INT REFERENCES dim_date(date_sk),
    customer_sk             VARCHAR(50) REFERENCES dim_customer(customer_sk),
    product_sk              VARCHAR(50) REFERENCES dim_product(product_sk),
    seller_sk               VARCHAR(50) REFERENCES dim_seller(seller_sk),
    order_id                VARCHAR(50) NOT NULL,
    order_item_id           INT NOT NULL,

    -- Measures
    price                   FLOAT,
    freight_value           FLOAT,
    review_score            INT,

    -- Delivery metrics
    delivery_days_actual    INT,
    delivery_days_estimated INT,
    delivery_delay_days     INT,

    -- Status & timestamps
    order_status            VARCHAR(20),
    purchase_timestamp      TIMESTAMP,
    delivered_timestamp     TIMESTAMP,
    estimated_delivery      TIMESTAMP
);

-- =============================================
-- Indexes for common query patterns
-- =============================================
CREATE INDEX idx_fact_date ON fact_order_items(date_sk);
CREATE INDEX idx_fact_customer ON fact_order_items(customer_sk);
CREATE INDEX idx_fact_product ON fact_order_items(product_sk);
CREATE INDEX idx_fact_seller ON fact_order_items(seller_sk);
CREATE INDEX idx_fact_status ON fact_order_items(order_status);
CREATE INDEX idx_fact_order_id ON fact_order_items(order_id);