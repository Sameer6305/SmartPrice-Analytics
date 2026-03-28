"""
SmartPrice Analytics Pipeline Orchestrator
============================================
Coordinates the complete ETL workflow: Scrape → Load → Validate → Transform

Author: Smart Price Analytics Team
Version: 1.0.0
"""

import logging
import os
import sys
import hashlib
from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import yaml

# Local imports
from scraper import SmartphoneScraper
from db import DatabaseManager, setup_staging_schema, setup_analytics_schema, deduplicate_products
from validation import DataValidator, generate_validation_report

# Configuration
load_dotenv()

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =============================================================================
# PIPELINE ORCHESTRATOR
# =============================================================================

class PipelineOrchestrator:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, db_manager: DatabaseManager = None):
        """
        Initialize pipeline.
        
        Args:
            db_manager: DatabaseManager instance (creates new if None)
        """
        self.db_manager = db_manager or DatabaseManager()
        self.validator = DataValidator()
        self.pipeline_start = datetime.now(timezone.utc)
        self.run_id = uuid4().hex
        self.batch_id = uuid4().hex
        self.config = self._load_config()
        self.execution_summary = {}

    def _record_run_start(self, search_queries: list, pages_per_query: int, dry_run: bool) -> None:
        """Record pipeline run start in audit table."""
        self.db_manager.execute_query(
            """
            INSERT INTO ops.pipeline_run_audit (
                run_id, started_at, status, dry_run, search_queries, pages_per_query
            ) VALUES (%s, NOW(), %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
            """,
            (
                self.run_id,
                "running",
                bool(dry_run),
                ",".join(search_queries),
                int(pages_per_query),
            ),
        )

    def _record_run_end(self, status: str, error_message: str = None) -> None:
        """Record pipeline run completion in audit table."""
        self.db_manager.execute_query(
            """
            UPDATE ops.pipeline_run_audit
            SET
                ended_at = NOW(),
                status = %s,
                scraped_records = %s,
                valid_records = %s,
                deduplicated_records = %s,
                loaded_records = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                status,
                int(self.execution_summary.get("scraped_records", 0)),
                int(self.execution_summary.get("valid_records", 0)),
                int(self.execution_summary.get("deduplicated_records", 0)),
                int(self.execution_summary.get("loaded_records", 0)),
                error_message,
                self.run_id,
            ),
        )

    def _load_config(self) -> dict:
        """Load optional pipeline config from config.yaml."""
        cfg_file = Path(__file__).parent / "config.yaml"
        if not cfg_file.exists():
            return {}

        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Failed to read config.yaml, using defaults: {e}")
            return {}

    def _get_active_source_config(self) -> dict:
        """Get the first enabled source config, defaulting to amazon values."""
        sources = self.config.get("sources", {}) if isinstance(self.config, dict) else {}
        if isinstance(sources, dict):
            for source_name, source_cfg in sources.items():
                if isinstance(source_cfg, dict) and source_cfg.get("enabled") is True:
                    return {
                        "source": source_name,
                        "base_url": source_cfg.get("base_url", "https://www.amazon.in/s"),
                        "rate_limit": int(source_cfg.get("rate_limit", 2)),
                    }

        return {
            "source": "amazon",
            "base_url": "https://www.amazon.in/s",
            "rate_limit": 2,
        }

    def _add_lineage_and_hash(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add lineage columns and a deterministic row hash for idempotent loads."""
        out = df.copy()

        for col in ["product_name", "source_marketplace", "source_url", "current_price", "mrp"]:
            if col not in out.columns:
                out[col] = None

        def build_hash(row: pd.Series) -> str:
            raw_key = "|".join([
                str(row.get("source_marketplace") or "").strip().lower(),
                str(row.get("source_url") or "").strip().lower(),
                str(row.get("product_name") or "").strip().lower(),
                str(row.get("current_price") or ""),
                str(row.get("mrp") or ""),
            ])
            return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

        out["raw_html_hash"] = out.apply(build_hash, axis=1)
        out["scrape_batch_id"] = self.batch_id
        out["scrape_job_id"] = self.batch_id
        return out

    def _filter_existing_hashes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows already present in staging by raw_html_hash."""
        if "raw_html_hash" not in df.columns or df.empty:
            return df

        hashes = [h for h in df["raw_html_hash"].dropna().unique().tolist() if h]
        if not hashes:
            return df

        try:
            rows = self.db_manager.fetch_query(
                """
                SELECT raw_html_hash
                FROM staging.stg_product_prices
                WHERE raw_html_hash = ANY(%s)
                """,
                (hashes,),
            )
            existing = {r[0] for r in rows or []}
            if not existing:
                return df

            filtered = df[~df["raw_html_hash"].isin(existing)].copy()
            logger.info(f"Idempotency filter removed {len(df) - len(filtered)} existing records")
            return filtered
        except Exception as e:
            logger.warning(f"Idempotency filter skipped due to query error: {e}")
            return df
    
    def run_full_pipeline(
        self,
        search_queries: list = None,
        pages_per_query: int = 1,
        dry_run: bool = False
    ) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Args:
            search_queries: List of search terms
            pages_per_query: Pages to scrape per query
            dry_run: If True, don't load to database
        
        Returns:
            True if successful.
        """
        logger.info("=" * 80)
        logger.info("SMARTPRICE ANALYTICS - PIPELINE START")
        logger.info("=" * 80)
        
        search_queries = search_queries or ["smartphone"]
        run_started = False
        
        try:
            # Step 1: Setup Database Schema
            logger.info("\n[STEP 1] Setting up database schemas...")
            if not self._setup_schemas():
                logger.error("Schema setup failed")
                return False

            self._record_run_start(search_queries, pages_per_query, dry_run)
            run_started = True
            
            # Step 2: Scrape Data
            logger.info("\n[STEP 2] Scraping product data...")
            scraped_df = self._scrape_data(search_queries, pages_per_query)
            if scraped_df is None or len(scraped_df) == 0:
                logger.error("No data scraped")
                if run_started:
                    self._record_run_end("failed", "No data scraped")
                return False
            
            self.execution_summary["scraped_records"] = len(scraped_df)
            
            # Step 3: Validate Data
            logger.info("\n[STEP 3] Validating data quality...")
            valid_df = self._validate_data(scraped_df)
            if valid_df is None or len(valid_df) == 0:
                logger.error("No valid data after validation")
                if run_started:
                    self._record_run_end("failed", "No valid data after validation")
                return False
            
            self.execution_summary["valid_records"] = len(valid_df)
            
            # Step 4: Deduplicate
            logger.info("\n[STEP 4] Deduplicating products...")
            valid_df = deduplicate_products(valid_df, self.db_manager)
            self.execution_summary["deduplicated_records"] = len(valid_df)
            
            # Step 5: Load to Staging
            logger.info("\n[STEP 5] Loading data to staging layer...")
            if not dry_run:
                if not self._load_to_staging(valid_df):
                    logger.error("Failed to load to staging")
                    if run_started:
                        self._record_run_end("failed", "Failed to load to staging")
                    return False
            else:
                logger.info("[DRY RUN] Skipping database load")
                valid_df.to_csv("staging_preview.csv", index=False)
                logger.info("Data saved to staging_preview.csv for review")
            
            # Step 6: Transform Data
            logger.info("\n[STEP 6] Transforming data to analytics layer...")
            if not dry_run:
                if not self._transform_data():
                    logger.error("Failed to transform data")
                    if run_started:
                        self._record_run_end("failed", "Failed to transform data")
                    return False
            else:
                logger.info("[DRY RUN] Skipping transformation")
            
            # Step 7: Generate Summary
            logger.info("\n[STEP 7] Generating pipeline summary...")
            self._generate_summary()
            
            logger.info("\n" + "=" * 80)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            if run_started:
                self._record_run_end("succeeded")
            return True
        
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            if run_started:
                self._record_run_end("failed", str(e)[:1000])
            return False
        finally:
            self.db_manager.close()
    
    def _setup_schemas(self) -> bool:
        """Setup database schemas."""
        try:
            if not setup_staging_schema(self.db_manager):
                logger.error("Staging schema setup failed")
                return False
            
            if not setup_analytics_schema(self.db_manager):
                logger.error("Analytics schema setup failed")
                return False
            
            logger.info("✓ Database schemas ready")
            return True
        
        except Exception as e:
            logger.error(f"Schema setup error: {e}")
            return False
    
    def _scrape_data(self, search_queries: list, pages_per_query: int) -> pd.DataFrame:
        """
        Scrape product data from e-commerce sources.
        
        Args:
            search_queries: List of search terms
            pages_per_query: Pages to scrape per query
        
        Returns:
            DataFrame with scraped data or None.
        """
        try:
            all_products = []
            src_cfg = self._get_active_source_config()
            
            # Initialize scraper from config.yaml (falls back to defaults).
            scraper = SmartphoneScraper(
                source=src_cfg["source"],
                base_url=src_cfg["base_url"],
                rate_limit=src_cfg["rate_limit"]
            )
            
            # Scrape for each query
            for query in search_queries:
                logger.info(f"Scraping for: {query}")
                products = scraper.scrape_products(
                    search_query=query,
                    num_pages=pages_per_query
                )
                all_products.extend(products)
                logger.info(f"  → Scraped {len(products)} products")
            
            if not all_products:
                logger.warning("No products scraped")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(all_products)
            logger.info(f"✓ Total scraped: {len(df)} records")
            logger.info(f"  Columns: {list(df.columns)}")
            
            return df
        
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return None
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate scraped data.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Valid DataFrame or None.
        """
        try:
            # Run validation
            result = self.validator.validate(df)
            
            # Generate report
            report = generate_validation_report(
                df, result,
                output_file=log_dir / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )
            logger.info(f"\n{result.summary()}")
            
            # Filter to valid records
            valid_df = self.validator.filter_valid_records(df)
            
            if len(valid_df) == 0:
                logger.error("No records passed validation")
                return None
            
            logger.info(f"✓ Validation passed: {len(valid_df)} valid records")
            return valid_df
        
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return None
    
    def _load_to_staging(self, df: pd.DataFrame) -> bool:
        """
        Load data to staging layer in PostgreSQL.
        
        Args:
            df: DataFrame to load
        
        Returns:
            True if successful.
        """
        try:
            # Add metadata columns
            df["is_valid"] = True
            df["created_at"] = datetime.now(timezone.utc)
            df["updated_at"] = datetime.now(timezone.utc)

            # Add lineage/idempotency metadata and filter existing records.
            df = self._add_lineage_and_hash(df)
            df = self._filter_existing_hashes(df)

            if df.empty:
                logger.info("No new records to load after idempotency filter")
                self.execution_summary["loaded_records"] = 0
                return True
            
            # Load to database
            success = self.db_manager.load_dataframe(
                df,
                table_name="stg_product_prices",
                schema="staging",
                if_exists="append"
            )
            
            if success:
                self.execution_summary["loaded_records"] = len(df)
                logger.info(f"✓ Loaded {len(df)} records to staging.stg_product_prices")
                return True
            else:
                logger.error("Failed to load data to database")
                return False
        
        except Exception as e:
            logger.error(f"Load error: {e}")
            return False
    
    def _transform_data(self) -> bool:
        """
        Execute transformation queries to populate analytics layer.
        
        Returns:
            True if successful.
        """
        try:
            sql_file = Path(__file__).parent / "sql" / "transformation.sql"
            
            if not sql_file.exists():
                logger.error(f"Transformation SQL file not found: {sql_file}")
                return False
            
            with open(sql_file, "r") as f:
                transform_sql = f.read()

            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("Failed to get database connection for transformation")
                return False

            try:
                with conn.cursor() as cur:
                    # Execute the full script in one transaction for deterministic behavior.
                    cur.execute(transform_sql)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Transformation failed and was rolled back: {e}")
                return False
            finally:
                self.db_manager.return_connection(conn)

            logger.info("✓ Transformation completed successfully")
            return True
        
        except Exception as e:
            logger.error(f"Transformation error: {e}")
            return False
    
    def _generate_summary(self):
        """Generate and log pipeline summary."""
        elapsed = (datetime.now(timezone.utc) - self.pipeline_start).total_seconds()
        
        summary = f"""
================================================================================
                        PIPELINE EXECUTION SUMMARY
================================================================================
Execution Time: {datetime.now(timezone.utc).isoformat()}
Elapsed Time: {elapsed:.2f} seconds

Records Processed:
  • Scraped: {self.execution_summary.get('scraped_records', 0)}
  • Valid: {self.execution_summary.get('valid_records', 0)}
  • Deduplicated: {self.execution_summary.get('deduplicated_records', 0)}
  • Loaded to Staging: {self.execution_summary.get('deduplicated_records', 0)}

Next Steps:
  • Check staging.stg_product_prices for raw data
  • Verify analytics.fact_price_history for transformed data
  • Run analytics_queries.sql for business insights

Log Files:
  • Pipeline Log: logs/pipeline.log
  • Validation Report: logs/validation_report_*.txt

================================================================================
"""
        logger.info(summary)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point."""
    
    # Check for command-line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description="SmartPrice Analytics Pipeline")
    parser.add_argument(
        "--queries",
        nargs="+",
        default=["smartphone"],
        help="Search queries (default: smartphone)"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Pages to scrape per query (default: 1)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no database writes)"
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    orchestrator = PipelineOrchestrator()
    success = orchestrator.run_full_pipeline(
        search_queries=args.queries,
        pages_per_query=args.pages,
        dry_run=args.dry_run
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
