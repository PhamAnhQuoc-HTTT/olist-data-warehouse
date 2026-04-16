-- =============================================
-- REVENUE ANALYSIS
-- Database: olist_dw (Star Schema)
-- =============================================

-- 1. Monthly revenue trend
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS revenue,
    SUM(f.freight_value) AS total_freight,
    ROUND(AVG(f.price)::NUMERIC, 2) AS avg_order_value
FROM fact_order_items f
JOIN dim_date d ON f.date_sk = d.date_sk
WHERE f.order_status = 'delivered'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 2. Quarterly revenue with growth rate
WITH quarterly AS (
    SELECT
        d.year,
        d.quarter_name,
        SUM(f.price) AS revenue
    FROM fact_order_items f
    JOIN dim_date d ON f.date_sk = d.date_sk
    WHERE f.order_status = 'delivered'
    GROUP BY d.year, d.quarter_name
    ORDER BY d.year, d.quarter_name
)
SELECT
    year,
    quarter_name,
    ROUND(revenue::NUMERIC, 2) AS revenue,
    ROUND(
        ((revenue - LAG(revenue) OVER (ORDER BY year, quarter_name))
        / LAG(revenue) OVER (ORDER BY year, quarter_name) * 100)::NUMERIC
    , 1) AS growth_pct
FROM quarterly;


-- 3. Revenue by product category (Top 15)
SELECT
    p.product_category_english AS category,
    COUNT(*) AS items_sold,
    ROUND(SUM(f.price)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.price)::NUMERIC, 2) AS avg_price,
    ROUND(AVG(f.review_score)::NUMERIC, 2) AS avg_review
FROM fact_order_items f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.order_status = 'delivered'
    AND p.product_category_english IS NOT NULL
GROUP BY p.product_category_english
ORDER BY revenue DESC
LIMIT 15;


-- 4. Revenue by customer state (Top 10)
SELECT
    c.customer_state AS state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
    ROUND(SUM(f.price)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.price)::NUMERIC, 2) AS avg_item_price
FROM fact_order_items f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY revenue DESC
LIMIT 10;


-- 5. Weekend vs Weekday revenue
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.price)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.price)::NUMERIC, 2) AS avg_item_price
FROM fact_order_items f
JOIN dim_date d ON f.date_sk = d.date_sk
WHERE f.order_status = 'delivered'
GROUP BY d.is_weekend;