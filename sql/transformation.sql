-- =============================================================================
-- SMART PRICE ANALYTICS - TRANSFORMATION LAYER (ELT)
-- =============================================================================
-- Purpose: Transform staging data into analytics-ready fact and dimension tables
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================================

-- =============================================================================
-- STEP 1: POPULATE DIM_PRODUCT (Slowly Changing Dimension Type 1)
-- =============================================================================
-- Deduplicates products and creates a dimension table with product history

INSERT INTO analytics.dim_product (product_name, brand, category, product_key, created_at, updated_at)
SELECT DISTINCT
    TRIM(stg.product_name) as product_name,
    TRIM(stg.brand) as brand,
    'Smartphone' as category,
    MD5(LOWER(TRIM(stg.product_name)) || '|' || COALESCE(LOWER(TRIM(stg.brand)), 'unknown')) as product_key,
    NOW() as created_at,
    NOW() as updated_at
FROM staging.stg_product_prices stg
WHERE stg.product_name IS NOT NULL
  AND stg.current_price > 0
  AND NOT EXISTS (
    SELECT 1 FROM analytics.dim_product dim
    WHERE MD5(LOWER(TRIM(stg.product_name)) || '|' || COALESCE(LOWER(TRIM(stg.brand)), 'unknown'))
        = dim.product_key
  )
ON CONFLICT (product_key) DO UPDATE
SET updated_at = NOW();


-- =============================================================================
-- STEP 2: POPULATE FACT_PRICE_HISTORY (Aggregated Daily Snapshots)
-- =============================================================================
-- Creates daily snapshots of product prices across sources

INSERT INTO analytics.fact_price_history (
    product_id,
    price_date,
    current_price,
    mrp,
    discount_percentage,
    customer_rating,
    review_count,
    availability_status,
    source_marketplace,
    scrape_count_daily,
    loaded_at
)
SELECT
    dim.product_id,
    DATE(stg.scrape_timestamp_utc) as price_date,
    AVG(stg.current_price) as current_price,           -- Take average if multiple daily scrapes
    AVG(stg.mrp) as mrp,
    AVG(stg.discount_percentage) as discount_percentage,
    MAX(stg.customer_rating) as customer_rating,       -- Take latest/highest rating
    MAX(stg.review_count) as review_count,             -- Take latest review count
    stg.availability_status,
    stg.source_marketplace,
    COUNT(DISTINCT stg.record_id) as scrape_count_daily,
    NOW() as loaded_at
FROM staging.stg_product_prices stg
INNER JOIN analytics.dim_product dim
    ON MD5(LOWER(TRIM(stg.product_name)) || '|' || COALESCE(LOWER(TRIM(stg.brand)), 'unknown'))
       = dim.product_key
WHERE stg.is_valid = TRUE
  AND stg.current_price > 0
  AND stg.scrape_timestamp_utc IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM analytics.fact_price_history fact
    WHERE fact.product_id = dim.product_id
      AND fact.price_date = DATE(stg.scrape_timestamp_utc)
      AND fact.source_marketplace = stg.source_marketplace
  )
GROUP BY
    dim.product_id,
    DATE(stg.scrape_timestamp_utc),
    stg.availability_status,
    stg.source_marketplace
ON CONFLICT (product_id, price_date, source_marketplace) DO UPDATE
SET
    current_price = EXCLUDED.current_price,
    mrp = EXCLUDED.mrp,
    discount_percentage = EXCLUDED.discount_percentage,
    customer_rating = EXCLUDED.customer_rating,
    review_count = EXCLUDED.review_count,
    scrape_count_daily = EXCLUDED.scrape_count_daily,
    loaded_at = NOW();


-- =============================================================================
-- STEP 3: REFRESH AGG_BRAND_DAILY_STATS (Materialized Aggregation)
-- =============================================================================
-- Aggregates daily statistics by brand for easy reporting

DELETE FROM analytics.agg_brand_daily_stats
WHERE stats_date >= CURRENT_DATE - INTERVAL '7 days';

INSERT INTO analytics.agg_brand_daily_stats (
    brand,
    stats_date,
    avg_price,
    min_price,
    max_price,
    avg_discount,
    avg_rating,
    product_count,
    loaded_at
)
SELECT
    dim.brand,
    fact.price_date,
    ROUND(AVG(fact.current_price)::NUMERIC, 2) as avg_price,
    ROUND(MIN(fact.current_price)::NUMERIC, 2) as min_price,
    ROUND(MAX(fact.current_price)::NUMERIC, 2) as max_price,
    ROUND(AVG(fact.discount_percentage)::NUMERIC, 2) as avg_discount,
    ROUND(AVG(fact.customer_rating)::NUMERIC, 2) as avg_rating,
    COUNT(DISTINCT fact.product_id) as product_count,
    NOW() as loaded_at
FROM analytics.fact_price_history fact
INNER JOIN analytics.dim_product dim
    ON fact.product_id = dim.product_id
WHERE dim.brand IS NOT NULL
GROUP BY
    dim.brand,
    fact.price_date
ON CONFLICT (brand, stats_date) DO UPDATE
SET
    avg_price = EXCLUDED.avg_price,
    min_price = EXCLUDED.min_price,
    max_price = EXCLUDED.max_price,
    avg_discount = EXCLUDED.avg_discount,
    avg_rating = EXCLUDED.avg_rating,
    product_count = EXCLUDED.product_count,
    loaded_at = NOW();


-- =============================================================================
-- DATA QUALITY CHECKS (Informational - log results)
-- =============================================================================

-- Check for recent data
SELECT
    'Data Freshness Check' as check_name,
    COUNT(*) as records_count,
    MAX(price_date) as latest_date,
    CURRENT_DATE - MAX(price_date) as days_old
FROM analytics.fact_price_history
;

-- Check for missing products in dimension
SELECT
    'Orphaned Staging Records' as check_name,
    COUNT(*) as count
FROM staging.stg_product_prices stg
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.dim_product dim
    WHERE MD5(LOWER(TRIM(stg.product_name)) || '|' || COALESCE(LOWER(TRIM(stg.brand)), 'unknown'))
        = dim.product_key
)
;

-- Check for data anomalies
SELECT
    'Price Anomalies (MRP < Price)' as check_name,
    COUNT(*) as count
FROM analytics.fact_price_history
WHERE mrp IS NOT NULL
  AND mrp < current_price
;

-- =============================================================================
-- END OF TRANSFORMATION LAYER
-- =============================================================================
