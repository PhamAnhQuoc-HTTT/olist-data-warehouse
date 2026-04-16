-- =============================================
-- SELLER PERFORMANCE ANALYSIS
-- Database: olist_dw (Star Schema)
-- =============================================

-- 1. Top 20 sellers by revenue
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS items_sold,
    ROUND(SUM(f.price)::NUMERIC, 2) AS revenue,
    ROUND(AVG(f.review_score)::NUMERIC, 2) AS avg_review,
    ROUND(
        SUM(CASE WHEN f.delivery_delay_days > 0 THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(SUM(CASE WHEN f.delivery_days_actual IS NOT NULL THEN 1 ELSE 0 END), 0)::NUMERIC * 100
    , 1) AS late_rate_pct
FROM fact_order_items f
JOIN dim_seller s ON f.seller_sk = s.seller_sk
WHERE f.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY revenue DESC
LIMIT 20;


-- 2. Seller concentration: top sellers vs rest
WITH seller_revenue AS (
    SELECT
        s.seller_id,
        SUM(f.price) AS revenue
    FROM fact_order_items f
    JOIN dim_seller s ON f.seller_sk = s.seller_sk
    WHERE f.order_status = 'delivered'
    GROUP BY s.seller_id
),
ranked AS (
    SELECT
        seller_id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rank,
        COUNT(*) OVER () AS total_sellers
    FROM seller_revenue
)
SELECT
    CASE
        WHEN rank <= 10 THEN 'Top 10'
        WHEN rank <= 50 THEN 'Top 11-50'
        WHEN rank <= 100 THEN 'Top 51-100'
        ELSE 'Rest'
    END AS seller_tier,
    COUNT(*) AS seller_count,
    ROUND(SUM(revenue)::NUMERIC, 2) AS tier_revenue,
    ROUND(
        SUM(revenue)::NUMERIC / (SELECT SUM(revenue) FROM seller_revenue)::NUMERIC * 100
    , 1) AS revenue_share_pct
FROM ranked
GROUP BY
    CASE
        WHEN rank <= 10 THEN 'Top 10'
        WHEN rank <= 50 THEN 'Top 11-50'
        WHEN rank <= 100 THEN 'Top 51-100'
        ELSE 'Rest'
    END
ORDER BY MIN(rank);


-- 3. Seller state performance comparison
SELECT
    s.seller_state,
    COUNT(DISTINCT s.seller_id) AS seller_count,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(SUM(f.price)::NUMERIC, 2) AS revenue,
    ROUND(SUM(f.price)::NUMERIC / COUNT(DISTINCT s.seller_id)::NUMERIC, 2) AS revenue_per_seller,
    ROUND(AVG(f.review_score)::NUMERIC, 2) AS avg_review
FROM fact_order_items f
JOIN dim_seller s ON f.seller_sk = s.seller_sk
WHERE f.order_status = 'delivered'
GROUP BY s.seller_state
HAVING COUNT(DISTINCT s.seller_id) >= 5
ORDER BY revenue DESC;