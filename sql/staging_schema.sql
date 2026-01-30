-- =============================================================================
-- SMART PRICE ANALYTICS - STAGING LAYER SCHEMA
-- =============================================================================
-- Purpose: Store raw, unprocessed e-commerce product data from multiple sources
-- Layer:   Staging (Bronze)
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================================

-- Drop table if exists (for development/testing only)
-- DROP TABLE IF EXISTS staging.stg_product_prices;

-- =============================================================================
-- CREATE SCHEMA (if not exists)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

COMMENT ON SCHEMA staging IS 'Staging layer for raw, unprocessed source data';

-- =============================================================================
-- STAGING TABLE: Raw Product Price Data
-- =============================================================================

CREATE TABLE staging.stg_product_prices (
    
    -- -------------------------------------------------------------------------
    -- SURROGATE KEY
    -- -------------------------------------------------------------------------
    record_id               BIGSERIAL       PRIMARY KEY,
    
    -- -------------------------------------------------------------------------
    -- PRODUCT IDENTIFICATION
    -- -------------------------------------------------------------------------
    product_name            TEXT            NULL,           -- Raw product title from source
    product_id_source       VARCHAR(100)    NULL,           -- Product ID as provided by source (ASIN, SKU, etc.)
    brand                   VARCHAR(255)    NULL,           -- Brand name (if available)
    category                VARCHAR(255)    NULL,           -- Product category from source
    model                   VARCHAR(255)    NULL,           -- Model name/number
    
    -- -------------------------------------------------------------------------
    -- PRICING INFORMATION
    -- -------------------------------------------------------------------------
    current_price           NUMERIC(12, 2)  NULL,           -- Current selling price
    mrp                     NUMERIC(12, 2)  NULL,           -- Maximum Retail Price / Original price
    discount_value          NUMERIC(12, 2)  NULL,           -- Absolute discount amount
    discount_percentage     NUMERIC(5, 2)   NULL,           -- Discount as percentage (0.00 - 100.00)
    currency_code           CHAR(3)         DEFAULT 'INR',  -- ISO 4217 currency code
    
    -- -------------------------------------------------------------------------
    -- RATINGS & REVIEWS
    -- -------------------------------------------------------------------------
    customer_rating         NUMERIC(2, 1)   NULL,           -- Average rating (0.0 - 5.0 scale)
    review_count            INTEGER         NULL,           -- Total number of customer reviews
    rating_count            INTEGER         NULL,           -- Total number of ratings (may differ from reviews)
    
    -- -------------------------------------------------------------------------
    -- AVAILABILITY & STOCK
    -- -------------------------------------------------------------------------
    availability_status     VARCHAR(50)     NULL,           -- Raw status: 'In Stock', 'Out of Stock', etc.
    stock_quantity          INTEGER         NULL,           -- Quantity available (if provided)
    seller_name             VARCHAR(255)    NULL,           -- Seller/merchant name
    seller_id               VARCHAR(100)    NULL,           -- Seller identifier from source
    fulfillment_type        VARCHAR(50)     NULL,           -- e.g., 'FBA', 'FBF', 'Seller Fulfilled'
    
    -- -------------------------------------------------------------------------
    -- SOURCE METADATA
    -- -------------------------------------------------------------------------
    source_marketplace      VARCHAR(50)     NOT NULL,       -- e.g., 'amazon', 'flipkart', 'croma'
    source_url              TEXT            NULL,           -- Full URL of the product page
    source_region           VARCHAR(10)     NULL,           -- Region code: 'IN', 'US', etc.
    
    -- -------------------------------------------------------------------------
    -- SCRAPE METADATA
    -- -------------------------------------------------------------------------
    scrape_timestamp_utc    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),  -- When data was scraped
    scrape_batch_id         VARCHAR(50)     NULL,           -- Batch identifier for tracking scrape runs
    scrape_job_id           VARCHAR(100)    NULL,           -- Unique identifier for the scrape job
    
    -- -------------------------------------------------------------------------
    -- DATA QUALITY & LINEAGE
    -- -------------------------------------------------------------------------
    raw_html_hash           VARCHAR(64)     NULL,           -- SHA-256 hash of source HTML (for deduplication)
    raw_payload             JSONB           NULL,           -- Original raw data as JSON (optional)
    is_valid                BOOLEAN         DEFAULT TRUE,   -- Flag for basic validation status
    validation_errors       TEXT[]          NULL,           -- Array of validation error messages
    
    -- -------------------------------------------------------------------------
    -- RECORD METADATA
    -- -------------------------------------------------------------------------
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- TABLE COMMENTS
-- =============================================================================

COMMENT ON TABLE staging.stg_product_prices IS 
    'Staging table for raw e-commerce product pricing data from multiple marketplaces. 
     This is the landing zone for scraped data before transformation and loading into the warehouse.';

-- Column comments for documentation
COMMENT ON COLUMN staging.stg_product_prices.record_id IS 'Auto-generated surrogate key';
COMMENT ON COLUMN staging.stg_product_prices.product_name IS 'Raw product title as scraped from source';
COMMENT ON COLUMN staging.stg_product_prices.product_id_source IS 'Native product identifier from source (ASIN, SKU, etc.)';
COMMENT ON COLUMN staging.stg_product_prices.current_price IS 'Current selling price in source currency';
COMMENT ON COLUMN staging.stg_product_prices.mrp IS 'Maximum Retail Price or original list price';
COMMENT ON COLUMN staging.stg_product_prices.customer_rating IS 'Average customer rating on 0-5 scale';
COMMENT ON COLUMN staging.stg_product_prices.availability_status IS 'Stock availability status from source';
COMMENT ON COLUMN staging.stg_product_prices.source_marketplace IS 'Identifier for the source e-commerce platform';
COMMENT ON COLUMN staging.stg_product_prices.scrape_timestamp_utc IS 'UTC timestamp when the data was scraped';
COMMENT ON COLUMN staging.stg_product_prices.raw_payload IS 'Complete raw data preserved as JSONB for debugging';

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Index on scrape timestamp for time-based queries and partitioning lookups
CREATE INDEX idx_stg_product_prices_scrape_ts 
    ON staging.stg_product_prices (scrape_timestamp_utc DESC);

-- Index on source marketplace for filtering by platform
CREATE INDEX idx_stg_product_prices_marketplace 
    ON staging.stg_product_prices (source_marketplace);

-- Index on source product ID for lookups and deduplication
CREATE INDEX idx_stg_product_prices_source_id 
    ON staging.stg_product_prices (product_id_source) 
    WHERE product_id_source IS NOT NULL;

-- Composite index for common query patterns (marketplace + timestamp)
CREATE INDEX idx_stg_product_prices_marketplace_ts 
    ON staging.stg_product_prices (source_marketplace, scrape_timestamp_utc DESC);

-- Index on batch ID for tracking and reprocessing scrape batches
CREATE INDEX idx_stg_product_prices_batch 
    ON staging.stg_product_prices (scrape_batch_id) 
    WHERE scrape_batch_id IS NOT NULL;

-- Index for deduplication using HTML hash
CREATE INDEX idx_stg_product_prices_html_hash 
    ON staging.stg_product_prices (raw_html_hash) 
    WHERE raw_html_hash IS NOT NULL;

-- =============================================================================
-- TRIGGER: Auto-update 'updated_at' timestamp
-- =============================================================================

CREATE OR REPLACE FUNCTION staging.update_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_stg_product_prices_updated
    BEFORE UPDATE ON staging.stg_product_prices
    FOR EACH ROW
    EXECUTE FUNCTION staging.update_modified_timestamp();

-- =============================================================================
-- SAMPLE DATA VALIDATION CONSTRAINT (Optional)
-- =============================================================================

-- Ensure rating is within valid range if provided
ALTER TABLE staging.stg_product_prices
    ADD CONSTRAINT chk_rating_range 
    CHECK (customer_rating IS NULL OR (customer_rating >= 0 AND customer_rating <= 5));

-- Ensure discount percentage is within valid range if provided
ALTER TABLE staging.stg_product_prices
    ADD CONSTRAINT chk_discount_percentage_range 
    CHECK (discount_percentage IS NULL OR (discount_percentage >= 0 AND discount_percentage <= 100));

-- Ensure prices are non-negative if provided
ALTER TABLE staging.stg_product_prices
    ADD CONSTRAINT chk_prices_non_negative 
    CHECK (
        (current_price IS NULL OR current_price >= 0) AND
        (mrp IS NULL OR mrp >= 0)
    );

-- =============================================================================
-- GRANT PERMISSIONS (Adjust roles as per your organization)
-- =============================================================================

-- Example permission grants (uncomment and modify as needed)
-- GRANT USAGE ON SCHEMA staging TO etl_role;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON staging.stg_product_prices TO etl_role;
-- GRANT USAGE, SELECT ON SEQUENCE staging.stg_product_prices_record_id_seq TO etl_role;
-- GRANT SELECT ON staging.stg_product_prices TO analyst_role;

-- =============================================================================
-- END OF SCHEMA DEFINITION
-- =============================================================================
