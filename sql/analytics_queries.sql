-- =============================================================================
-- SMART PRICE ANALYTICS - BUSINESS INTELLIGENCE QUERIES
-- =============================================================================
-- Purpose: Sample analytical queries for stakeholders and recruiters
-- These queries address real business questions from the problem statement
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================================


-- =============================================================================
-- QUERY 1: PRICE TRENDS OVER TIME
-- =============================================================================
-- Shows how prices have evolved for specific products over their lifecycle
-- Use Case: Category managers forecasting price erosion

SELECT
    dp.brand,
    dp.product_name,
    fph.price_date,
    ROUND(AVG(fph.current_price)::NUMERIC, 2) as avg_price,
    ROUND(AVG(fph.mrp)::NUMERIC, 2) as avg_mrp,
    ROUND(AVG(fph.discount_percentage)::NUMERIC, 2) as avg_discount,
    COUNT(DISTINCT fph.source_marketplace) as marketplace_count
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp
    ON fph.product_id = dp.product_id
WHERE
    dp.brand IN ('Apple', 'Samsung', 'Xiaomi')  -- Filter by brands
    AND fph.price_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY
    dp.brand,
    dp.product_name,
    fph.price_date
ORDER BY
    dp.brand,
    dp.product_name,
    fph.price_date DESC
LIMIT 100;


-- =============================================================================
-- QUERY 2: COMPETITIVE PRICE POSITIONING
-- =============================================================================
-- Compares your price vs. competitors on a specific product
-- Use Case: Pricing teams checking real-time competitive gaps

WITH latest_prices AS (
    SELECT
        dp.product_name,
        fph.source_marketplace,
        ROUND(AVG(fph.current_price)::NUMERIC, 2) as current_price,
        ROUND(AVG(fph.customer_rating)::NUMERIC, 2) as avg_rating,
        ROW_NUMBER() OVER (PARTITION BY dp.product_name ORDER BY fph.price_date DESC) as rn
    FROM analytics.fact_price_history fph
    INNER JOIN analytics.dim_product dp
        ON fph.product_id = dp.product_id
    WHERE fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY
        dp.product_name,
        fph.source_marketplace,
        fph.price_date
)
SELECT
    product_name,
    source_marketplace,
    current_price,
    ROUND(MIN(current_price) OVER (PARTITION BY product_name)::NUMERIC, 2) as lowest_price,
    ROUND(AVG(current_price) OVER (PARTITION BY product_name)::NUMERIC, 2) as avg_price,
    ROUND(MAX(current_price) OVER (PARTITION BY product_name)::NUMERIC, 2) as highest_price,
    ROUND((current_price - MIN(current_price) OVER (PARTITION BY product_name))::NUMERIC, 2) as price_above_min,
    avg_rating
FROM latest_prices
WHERE rn = 1
ORDER BY product_name, current_price DESC;


-- =============================================================================
-- QUERY 3: BRAND-LEVEL PRICING ANALYSIS
-- =============================================================================
-- Shows brand pricing patterns and discount behaviors
-- Use Case: Brand managers and marketing for promotional planning

SELECT
    brand,
    stats_date,
    product_count,
    ROUND(avg_price::NUMERIC, 2) as avg_price,
    ROUND(min_price::NUMERIC, 2) as min_price,
    ROUND(max_price::NUMERIC, 2) as max_price,
    ROUND((max_price - min_price)::NUMERIC, 2) as price_range,
    ROUND(avg_discount::NUMERIC, 2) as avg_discount_pct,
    ROUND(avg_rating::NUMERIC, 2) as avg_rating
FROM analytics.agg_brand_daily_stats
WHERE
    stats_date >= CURRENT_DATE - INTERVAL '30 days'
    AND brand IS NOT NULL
ORDER BY
    brand,
    stats_date DESC;


-- =============================================================================
-- QUERY 4: AVAILABILITY & STOCK ANALYSIS
-- =============================================================================
-- Tracks which products are in stock across platforms
-- Use Case: Inventory managers and fulfillment planning

SELECT
    dp.brand,
    dp.product_name,
    fph.source_marketplace,
    fph.availability_status,
    COUNT(*) as count,
    ROUND(AVG(fph.current_price)::NUMERIC, 2) as avg_price,
    MAX(fph.price_date) as latest_date
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp
    ON fph.product_id = dp.product_id
WHERE
    fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
    AND dp.brand IS NOT NULL
GROUP BY
    dp.brand,
    dp.product_name,
    fph.source_marketplace,
    fph.availability_status
ORDER BY
    dp.brand,
    fph.availability_status,
    count DESC;


-- =============================================================================
-- QUERY 5: PRICE VOLATILITY ANALYSIS
-- =============================================================================
-- Identifies products with highest price fluctuations (volatile market)
-- Use Case: Risk management and dynamic repricing strategy

WITH price_stats AS (
    SELECT
        dp.product_id,
        dp.brand,
        dp.product_name,
        ROUND(AVG(fph.current_price)::NUMERIC, 2) as avg_price,
        ROUND(STDDEV_POP(fph.current_price)::NUMERIC, 2) as price_stddev,
        ROUND(MAX(fph.current_price) - MIN(fph.current_price)::NUMERIC, 2) as price_range,
        COUNT(*) as observation_count,
        MAX(fph.price_date) as latest_date
    FROM analytics.fact_price_history fph
    INNER JOIN analytics.dim_product dp
        ON fph.product_id = dp.product_id
    WHERE fph.price_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY
        dp.product_id,
        dp.brand,
        dp.product_name
)
SELECT
    brand,
    product_name,
    avg_price,
    price_stddev,
    price_range,
    ROUND((price_stddev / NULLIF(avg_price, 0) * 100)::NUMERIC, 2) as volatility_pct,
    observation_count,
    latest_date
FROM price_stats
WHERE observation_count >= 5  -- Require minimum observations
ORDER BY volatility_pct DESC
LIMIT 50;


-- =============================================================================
-- QUERY 6: DISCOUNT DEPTH & FREQUENCY
-- =============================================================================
-- Analyzes how aggressive discounting strategies are by brand
-- Use Case: Marketing and promotion planning

SELECT
    dp.brand,
    DATE_TRUNC('week', fph.price_date)::DATE as week_start,
    COUNT(DISTINCT fph.product_id) as products_on_discount,
    ROUND(AVG(fph.discount_percentage)::NUMERIC, 2) as avg_discount,
    ROUND(MAX(fph.discount_percentage)::NUMERIC, 2) as max_discount,
    ROUND(MIN(fph.discount_percentage)::NUMERIC, 2) as min_discount,
    COUNT(DISTINCT CASE WHEN fph.discount_percentage > 20 THEN fph.product_id END) as aggressive_discount_count
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp
    ON fph.product_id = dp.product_id
WHERE
    fph.price_date >= CURRENT_DATE - INTERVAL '90 days'
    AND fph.discount_percentage IS NOT NULL
    AND dp.brand IS NOT NULL
GROUP BY
    dp.brand,
    DATE_TRUNC('week', fph.price_date)
ORDER BY
    dp.brand,
    week_start DESC;


-- =============================================================================
-- QUERY 7: PRODUCT LIFECYCLE ANALYSIS
-- =============================================================================
-- Tracks new product launches and price erosion over time
-- Use Case: Product managers and strategic pricing

WITH price_evolution AS (
    SELECT
        dp.product_id,
        dp.brand,
        dp.product_name,
        MIN(fph.price_date) as first_seen_date,
        MAX(fph.price_date) as last_seen_date,
        (MAX(fph.price_date) - MIN(fph.price_date)) as days_in_market,
        (SELECT current_price FROM analytics.fact_price_history
         WHERE product_id = dp.product_id
         ORDER BY price_date ASC LIMIT 1) as launch_price,
        (SELECT current_price FROM analytics.fact_price_history
         WHERE product_id = dp.product_id
         ORDER BY price_date DESC LIMIT 1) as current_price,
        ROUND(AVG(fph.customer_rating)::NUMERIC, 2) as avg_rating
    FROM analytics.fact_price_history fph
    INNER JOIN analytics.dim_product dp
        ON fph.product_id = dp.product_id
    GROUP BY
        dp.product_id,
        dp.brand,
        dp.product_name
)
SELECT
    brand,
    product_name,
    first_seen_date,
    last_seen_date,
    days_in_market,
    ROUND(launch_price::NUMERIC, 2) as launch_price,
    ROUND(current_price::NUMERIC, 2) as current_price,
    ROUND((1 - current_price / NULLIF(launch_price, 0)) * 100::NUMERIC, 2) as price_erosion_pct,
    avg_rating
FROM price_evolution
WHERE days_in_market > 30  -- Products in market for at least 30 days
ORDER BY first_seen_date DESC, brand;


-- =============================================================================
-- QUERY 8: DATA QUALITY & FRESHNESS DASHBOARD
-- =============================================================================
-- Monitors ETL pipeline health and data freshness

SELECT
    'Staging Raw Data' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT source_marketplace) as source_count,
    MAX(scrape_timestamp_utc)::DATE as latest_scrape_date,
    ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*)::NUMERIC, 2) as data_quality_pct
FROM staging.stg_product_prices
WHERE scrape_timestamp_utc >= CURRENT_TIMESTAMP - INTERVAL '7 days'

UNION ALL

SELECT
    'Analytics Dimension' as layer,
    COUNT(*) as total_records,
    NULL as source_count,
    MAX(updated_at)::DATE as latest_date,
    100.0 as data_quality_pct
FROM analytics.dim_product

UNION ALL

SELECT
    'Analytics Facts' as layer,
    COUNT(*) as total_records,
    COUNT(DISTINCT source_marketplace) as source_count,
    MAX(price_date) as latest_date,
    100.0 as data_quality_pct
FROM analytics.fact_price_history
WHERE price_date >= CURRENT_DATE - INTERVAL '7 days';


-- =============================================================================
-- END OF BUSINESS QUERIES
-- =============================================================================
