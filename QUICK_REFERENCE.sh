#!/usr/bin/env bash

# SMARTPRICE ANALYTICS - QUICK REFERENCE GUIDE
# =============================================
# Common commands for daily operations

# ============================================================================
# SETUP (One-time)
# ============================================================================

# 1. Copy environment template
cp .env.example .env

# 2. Edit with your PostgreSQL credentials
# Windows: notepad .env
# macOS/Linux: nano .env

# 3. Create database (optional if PostgreSQL is running)
python setup_db.py

# 4. Install dependencies
pip install -r requirements.txt


# ============================================================================
# DEVELOPMENT & TESTING
# ============================================================================

# Test scraper alone
python scraper.py

# Test database connection
python db.py

# Test validation
python validation.py

# Test full pipeline (dry-run, no database writes)
python pipeline.py --dry-run --pages 1

# Inspect generated data
head staging_preview.csv


# ============================================================================
# PRODUCTION RUNS
# ============================================================================

# Scrape single product type (1 page)
python pipeline.py --queries smartphone --pages 1

# Scrape multiple product types, 2 pages each
python pipeline.py --queries "iphone samsung xiaomi" --pages 2

# Custom rate limiting (edit config.yaml)
# Then run:
python pipeline.py

# View execution logs
tail -f logs/pipeline.log

# View validation report
cat logs/validation_report_*.txt


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

# Connect to database
psql -U postgres -d smart_price_analytics

# List tables
psql -d smart_price_analytics -c "\dt staging.*"
psql -d smart_price_analytics -c "\dt analytics.*"

# Check raw data
psql -d smart_price_analytics -c "SELECT COUNT(*), MAX(scrape_timestamp_utc) FROM staging.stg_product_prices;"

# Check products loaded
psql -d smart_price_analytics -c "SELECT COUNT(*) as products FROM analytics.dim_product;"

# Check price history
psql -d smart_price_analytics -c "SELECT COUNT(*) FROM analytics.fact_price_history;"

# View recent prices
psql -d smart_price_analytics << EOF
SELECT 
    dp.brand, 
    dp.product_name,
    fph.price_date,
    ROUND(AVG(fph.current_price)::NUMERIC, 2) as price,
    ROUND(AVG(fph.discount_percentage)::NUMERIC, 2) as discount
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp ON fph.product_id = dp.product_id
WHERE fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dp.brand, dp.product_name, fph.price_date
ORDER BY fph.price_date DESC
LIMIT 20;
EOF

# Run analytics queries
psql -d smart_price_analytics -f sql/analytics_queries.sql

# Export results to CSV
psql -d smart_price_analytics -c "
  COPY (
    SELECT brand, stats_date, avg_price, product_count
    FROM analytics.agg_brand_daily_stats
    WHERE stats_date >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY stats_date DESC
  ) TO STDOUT WITH CSV HEADER
" > brand_stats.csv

# Cleanup old data (>2 years)
psql -d smart_price_analytics -c "
  DELETE FROM staging.stg_product_prices
  WHERE scrape_timestamp_utc < CURRENT_TIMESTAMP - INTERVAL '730 days';
"


# ============================================================================
# MONITORING & TROUBLESHOOTING
# ============================================================================

# Check pipeline health
psql -d smart_price_analytics << EOF
SELECT
    'Staging' as layer,
    COUNT(*) as records,
    MAX(scrape_timestamp_utc)::DATE as latest,
    ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*)::NUMERIC, 2) as quality_pct
FROM staging.stg_product_prices
WHERE scrape_timestamp_utc >= CURRENT_TIMESTAMP - INTERVAL '7 days'
UNION ALL
SELECT
    'Analytics Facts',
    COUNT(*),
    MAX(price_date),
    100.0
FROM analytics.fact_price_history
WHERE price_date >= CURRENT_DATE - INTERVAL '7 days';
EOF

# Check for data anomalies
psql -d smart_price_analytics << EOF
-- Products with MRP < Price (should be ~0)
SELECT COUNT(*) as anomalies FROM analytics.fact_price_history
WHERE mrp < current_price;

-- Check rating consistency
SELECT COUNT(*) as invalid_ratings FROM analytics.fact_price_history
WHERE customer_rating NOT BETWEEN 0 AND 5;
EOF

# View Python logs
grep ERROR logs/pipeline.log
grep WARNING logs/pipeline.log

# Clear logs (careful!)
rm logs/*.log logs/*.txt


# ============================================================================
# SCHEDULED RUNS (Cron)
# ============================================================================

# Daily run (add to crontab via: crontab -e)
# 0 2 * * * cd /path/to/smart_price_analytics && python pipeline.py >> logs/daily_run.log 2>&1

# Weekly full refresh every Sunday at 3 AM
# 0 3 * * 0 cd /path/to/smart_price_analytics && python pipeline.py --pages 3 >> logs/weekly_run.log 2>&1

# Real-world: Use Airflow/Prefect for enterprise scheduling


# ============================================================================
# DEPLOYMENT (Production)
# ============================================================================

# Backup database
pg_dump -U postgres smart_price_analytics > backup_$(date +%Y%m%d).sql

# Restore from backup
psql -U postgres smart_price_analytics < backup_20260323.sql

# Create read-only user for BI tools
psql -U postgres << EOF
CREATE ROLE analytics_reader WITH LOGIN PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE smart_price_analytics TO analytics_reader;
GRANT USAGE ON SCHEMA analytics TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analytics_reader;
EOF

# View database size
psql -d smart_price_analytics -c "
  SELECT 
    schemaname,
    SUM(heap_blks_read) as heap_read,
    SUM(heap_blks_hit) as heap_hit
  FROM pg_statio_user_tables
  GROUP BY schemaname;
"


# ============================================================================
# USEFUL QUERIES FOR ANALYTICS
# ============================================================================

# Top 10 most expensive products (last 7 days)
psql -d smart_price_analytics << EOF
SELECT 
    dp.brand,
    dp.product_name,
    ROUND(AVG(fph.current_price)::NUMERIC, 2) as avg_price
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp ON fph.product_id = dp.product_id
WHERE fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dp.brand, dp.product_name
ORDER BY avg_price DESC
LIMIT 10;
EOF

# Price volatility (products with biggest swings)
psql -d smart_price_analytics << EOF
SELECT 
    dp.brand,
    dp.product_name,
    MIN(fph.current_price) as min_price,
    MAX(fph.current_price) as max_price,
    ROUND(MAX(fph.current_price) - MIN(fph.current_price)::NUMERIC, 2) as price_range
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp ON fph.product_id = dp.product_id
WHERE fph.price_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dp.brand, dp.product_name
ORDER BY price_range DESC
LIMIT 10;
EOF

# Best deals (highest discounts)
psql -d smart_price_analytics << EOF
SELECT 
    dp.brand,
    dp.product_name,
    ROUND(AVG(fph.discount_percentage)::NUMERIC, 2) as avg_discount,
    ROUND(AVG(fph.current_price)::NUMERIC, 2) as price
FROM analytics.fact_price_history fph
INNER JOIN analytics.dim_product dp ON fph.product_id = dp.product_id
WHERE fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
  AND fph.discount_percentage > 0
GROUP BY dp.brand, dp.product_name
ORDER BY avg_discount DESC
LIMIT 10;
EOF


# ============================================================================
# SYSTEM INFO
# ============================================================================

# Python version
python --version

# PostgreSQL version
psql --version

# Check PostgreSQL service status
# macOS: brew services list | grep postgres
# Linux: sudo systemctl status postgresql
# Windows: Services app (search for "PostgreSQL")

# List installed Python packages
pip list

# Check virtual environment
which python


# ============================================================================
# HELP & DOCUMENTATION
# ============================================================================

# View main README
less README.md

# View implementation details
less IMPLEMENTATION_SUMMARY.md

# View configuration
cat config.yaml

# Pipeline help
python pipeline.py --help
