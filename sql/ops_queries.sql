-- =============================================================================
-- SMART PRICE ANALYTICS - OPERATIONAL AUDIT QUERIES
-- =============================================================================
-- Purpose: Operational visibility for ETL run status, reliability, and throughput
-- Author:  Data Engineering Team
-- Version: 1.0.0
-- =============================================================================


-- =============================================================================
-- QUERY 1: RECENT PIPELINE RUNS
-- =============================================================================
-- Use Case: Quickly inspect latest runs and outcomes

SELECT
    run_id,
    started_at,
    ended_at,
    status,
    dry_run,
    scraped_records,
    valid_records,
    deduplicated_records,
    loaded_records,
    error_message
FROM ops.pipeline_run_audit
ORDER BY started_at DESC
LIMIT 20;


-- =============================================================================
-- QUERY 2: SUCCESS RATE (LAST 30 DAYS)
-- =============================================================================
-- Use Case: Monitor reliability trend

SELECT
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded_runs,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE status = 'succeeded') / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_pct
FROM ops.pipeline_run_audit
WHERE started_at >= NOW() - INTERVAL '30 days';


-- =============================================================================
-- QUERY 3: AVERAGE LATENCY BY DAY
-- =============================================================================
-- Use Case: Detect slowdowns over time

SELECT
    DATE(started_at) AS run_date,
    COUNT(*) AS runs,
    ROUND(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::NUMERIC, 2) AS avg_duration_sec,
    ROUND(MAX(EXTRACT(EPOCH FROM (ended_at - started_at)))::NUMERIC, 2) AS max_duration_sec
FROM ops.pipeline_run_audit
WHERE ended_at IS NOT NULL
  AND started_at >= NOW() - INTERVAL '14 days'
GROUP BY DATE(started_at)
ORDER BY run_date DESC;


-- =============================================================================
-- QUERY 4: FAILURE DIAGNOSTICS
-- =============================================================================
-- Use Case: Identify dominant failure modes

SELECT
    COALESCE(NULLIF(error_message, ''), 'Unknown Error') AS error_group,
    COUNT(*) AS occurrence_count,
    MAX(started_at) AS last_seen
FROM ops.pipeline_run_audit
WHERE status = 'failed'
GROUP BY COALESCE(NULLIF(error_message, ''), 'Unknown Error')
ORDER BY occurrence_count DESC, last_seen DESC;


-- =============================================================================
-- QUERY 5: THROUGHPUT SNAPSHOT (LAST 7 DAYS)
-- =============================================================================
-- Use Case: Measure ingestion effectiveness

SELECT
    DATE(started_at) AS run_date,
    SUM(scraped_records) AS scraped_total,
    SUM(valid_records) AS valid_total,
    SUM(deduplicated_records) AS deduplicated_total,
    SUM(loaded_records) AS loaded_total
FROM ops.pipeline_run_audit
WHERE started_at >= NOW() - INTERVAL '7 days'
GROUP BY DATE(started_at)
ORDER BY run_date DESC;

-- =============================================================================
-- END OF OPERATIONAL QUERIES
-- =============================================================================
