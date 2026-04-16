-- =============================================
-- DELIVERY PERFORMANCE ANALYSIS
-- Database: olist_dw (Star Schema)
-- =============================================

-- 1. Overall delivery stats
SELECT
    COUNT(*) AS total_delivered,
    ROUND(AVG(delivery_days_actual)::NUMERIC, 1) AS avg_delivery_days,
    ROUND(AVG(delivery_days_estimated)::NUMERIC, 1) AS avg_estimated_days,
    ROUND(AVG(delivery_delay_days)::NUMERIC, 1) AS avg_delay_days,
    SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_count,
    ROUND(
        SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*)::NUMERIC * 100
    , 1) AS late_rate_pct
FROM fact_order_items
WHERE order_status = 'delivered'
    AND delivery_days_actual IS NOT NULL;


-- 2. Late delivery rate by month
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late_orders,
    ROUND(
        SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*)::NUMERIC * 100
    , 1) AS late_rate_pct
FROM fact_order_items f
JOIN dim_date d ON f.date_sk = d.date_sk
WHERE f.order_status = 'delivered'
    AND f.delivery_days_actual IS NOT NULL
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 3. Delivery performance by seller state
SELECT
    s.seller_state,
    COUNT(*) AS items_shipped,
    ROUND(AVG(f.delivery_days_actual)::NUMERIC, 1) AS avg_delivery_days,
    ROUND(
        SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*)::NUMERIC * 100
    , 1) AS late_rate_pct,
    ROUND(AVG(f.review_score)::NUMERIC, 2) AS avg_review
FROM fact_order_items f
JOIN dim_seller s ON f.seller_sk = s.seller_sk
WHERE f.order_status = 'delivered'
    AND f.delivery_days_actual IS NOT NULL
GROUP BY s.seller_state
HAVING COUNT(*) >= 100
ORDER BY late_rate_pct DESC;


-- 4. Impact of late delivery on review score
SELECT
    CASE
        WHEN delivery_delay_days <= -10 THEN '10+ days early'
        WHEN delivery_delay_days <= -5  THEN '5-10 days early'
        WHEN delivery_delay_days <= 0   THEN '0-5 days early'
        WHEN delivery_delay_days <= 5   THEN '1-5 days late'
        WHEN delivery_delay_days <= 10  THEN '6-10 days late'
        ELSE '10+ days late'
    END AS delivery_group,
    COUNT(*) AS order_count,
    ROUND(AVG(review_score)::NUMERIC, 2) AS avg_review_score
FROM fact_order_items
WHERE order_status = 'delivered'
    AND delivery_days_actual IS NOT NULL
    AND review_score IS NOT NULL
GROUP BY
    CASE
        WHEN delivery_delay_days <= -10 THEN '10+ days early'
        WHEN delivery_delay_days <= -5  THEN '5-10 days early'
        WHEN delivery_delay_days <= 0   THEN '0-5 days early'
        WHEN delivery_delay_days <= 5   THEN '1-5 days late'
        WHEN delivery_delay_days <= 10  THEN '6-10 days late'
        ELSE '10+ days late'
    END
ORDER BY avg_review_score DESC;


-- 5. Product categories with worst delivery
SELECT
    p.product_category_english AS category,
    COUNT(*) AS items_shipped,
    ROUND(AVG(f.delivery_days_actual)::NUMERIC, 1) AS avg_delivery_days,
    ROUND(
        SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END)::NUMERIC
        / COUNT(*)::NUMERIC * 100
    , 1) AS late_rate_pct
FROM fact_order_items f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.order_status = 'delivered'
    AND f.delivery_days_actual IS NOT NULL
    AND p.product_category_english IS NOT NULL
GROUP BY p.product_category_english
HAVING COUNT(*) >= 50
ORDER BY late_rate_pct DESC
LIMIT 10;