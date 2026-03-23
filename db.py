"""
Database Operations Module
===========================
Handles PostgreSQL connections, schema creation, and data loading.

Author: Smart Price Analytics Team
Version: 1.0.0
"""

import logging
import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import psycopg2
from psycopg2 import sql, pool, Error
from psycopg2.extensions import connection, cursor
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


# =============================================================================
# DATABASE CONNECTION MANAGER
# =============================================================================

class DatabaseManager:
    """Manages PostgreSQL connections and operations."""
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None
    ):
        """
        Initialize database manager.
        
        Args:
            host: Database host (default from .env)
            port: Database port (default from .env)
            database: Database name (default from .env)
            user: Database user (default from .env)
            password: Database password (default from .env)
        """
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = int(port or os.getenv("DB_PORT", 5432))
        self.database = database or os.getenv("DB_NAME", "smart_price_analytics")
        self.user = user or os.getenv("DB_USER", "postgres")
        self.password = password or os.getenv("DB_PASSWORD", "")
        
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Create connection pool."""
        try:
            self.connection_pool = pool.SimpleConnectionPool(
                1, 5,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Database connection pool initialized for {self.database}")
        except Error as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def get_connection(self) -> Optional[connection]:
        """Get a connection from the pool."""
        try:
            return self.connection_pool.getconn()
        except Error as e:
            logger.error(f"Failed to get connection: {e}")
            return None
    
    def return_connection(self, conn: connection):
        """Return a connection to the pool."""
        try:
            self.connection_pool.putconn(conn)
        except Error as e:
            logger.error(f"Failed to return connection: {e}")
    
    def execute_query(self, query: str, params: tuple = None) -> bool:
        """
        Execute a query without returning results.
        
        Args:
            query: SQL query
            params: Query parameters
        
        Returns:
            True if successful, False otherwise.
        """
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            logger.debug(f"Query executed successfully")
            return True
        except Error as e:
            logger.error(f"Query execution failed: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
            self.return_connection(conn)
    
    def fetch_query(self, query: str, params: tuple = None) -> Optional[List[tuple]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query
            params: Query parameters
        
        Returns:
            List of tuples or None if failed.
        """
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            results = cur.fetchall()
            return results
        except Error as e:
            logger.error(f"Query fetch failed: {e}")
            return None
        finally:
            cur.close()
            self.return_connection(conn)
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "staging",
        if_exists: str = "append"
    ) -> bool:
        """
        Load a pandas DataFrame into PostgreSQL.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Schema name
            if_exists: 'append', 'replace', or 'fail'
        
        Returns:
            True if successful, False otherwise.
        """
        from sqlalchemy import create_engine
        
        try:
            # Create SQLAlchemy engine
            engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
            
            # Convert timestamp columns
            for col in df.columns:
                if isinstance(df[col].dtype, object) and "timestamp" in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass
            
            # Load data
            df.to_sql(
                table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(
                f"Loaded {len(df)} rows into "
                f"{schema}.{table_name}"
            )
            return True
        
        except Exception as e:
            logger.error(f"Failed to load DataFrame: {e}")
            return False
    
    def close(self):
        """Close all connections in the pool."""
        try:
            self.connection_pool.closeall()
            logger.info("Connection pool closed")
        except Error as e:
            logger.error(f"Error closing connection pool: {e}")


# =============================================================================
# SCHEMA SETUP
# =============================================================================

def setup_staging_schema(db_manager: DatabaseManager) -> bool:
    """
    Create staging tables if they don't exist.
    
    Args:
        db_manager: DatabaseManager instance
    
    Returns:
        True if successful.
    """
    # Read schema from SQL file
    sql_file = os.path.join(
        os.path.dirname(__file__),
        "sql",
        "staging_schema.sql"
    )
    
    if not os.path.exists(sql_file):
        logger.error(f"Schema file not found: {sql_file}")
        return False
    
    try:
        with open(sql_file, "r") as f:
            schema_sql = f.read()
        
        # Execute schema creation
        conn = db_manager.get_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute(schema_sql)
        conn.commit()
        logger.info("Staging schema created successfully")
        
        cur.close()
        db_manager.return_connection(conn)
        return True
    
    except Exception as e:
        logger.error(f"Failed to setup staging schema: {e}")
        return False


def setup_analytics_schema(db_manager: DatabaseManager) -> bool:
    """
    Create analytics (transformed) tables.
    
    Args:
        db_manager: DatabaseManager instance
    
    Returns:
        True if successful.
    """
    analytics_sql = """
    -- =============================================================================
    -- SMART PRICE ANALYTICS - ANALYTICS LAYER SCHEMA
    -- =============================================================================
    
    CREATE SCHEMA IF NOT EXISTS analytics;
    COMMENT ON SCHEMA analytics IS 'Analytics layer with transformed, ready-to-query data';
    
    -- =============================================================================
    -- DIMENSION TABLE: Products
    -- =============================================================================
    
    CREATE TABLE IF NOT EXISTS analytics.dim_product (
        product_id              BIGSERIAL       PRIMARY KEY,
        product_name            TEXT            NOT NULL,
        brand                   VARCHAR(255)    NULL,
        category                VARCHAR(255)    DEFAULT 'Smartphone',
        product_key             VARCHAR(255)    UNIQUE,  -- Natural key for deduplication
        created_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_dim_product_brand ON analytics.dim_product(brand);
    CREATE INDEX IF NOT EXISTS idx_dim_product_name ON analytics.dim_product(product_name);
    
    COMMENT ON TABLE analytics.dim_product IS 'Product dimension table with unique product information';
    
    -- =============================================================================
    -- FACT TABLE: Price History
    -- =============================================================================
    
    CREATE TABLE IF NOT EXISTS analytics.fact_price_history (
        price_id                BIGSERIAL       PRIMARY KEY,
        product_id              BIGINT          NOT NULL REFERENCES analytics.dim_product(product_id),
        price_date              DATE            NOT NULL,
        current_price           NUMERIC(12, 2)  NOT NULL,
        mrp                     NUMERIC(12, 2)  NULL,
        discount_percentage     NUMERIC(5, 2)   NULL,
        customer_rating         NUMERIC(2, 1)   NULL,
        review_count            INTEGER         NULL,
        availability_status     VARCHAR(50)     NULL,
        source_marketplace      VARCHAR(50)     NULL,
        scrape_count_daily      INTEGER         DEFAULT 1,
        loaded_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_fact_price_date ON analytics.fact_price_history(price_date);
    CREATE INDEX IF NOT EXISTS idx_fact_product_date ON analytics.fact_price_history(product_id, price_date);
    CREATE INDEX IF NOT EXISTS idx_fact_price_marketplace ON analytics.fact_price_history(source_marketplace);
    
    COMMENT ON TABLE analytics.fact_price_history IS 'Time-series fact table for price tracking and analysis';
    
    -- =============================================================================
    -- AGGREGATION TABLE: Daily Brand Statistics
    -- =============================================================================
    
    CREATE TABLE IF NOT EXISTS analytics.agg_brand_daily_stats (
        stats_id                BIGSERIAL       PRIMARY KEY,
        brand                   VARCHAR(255)    NOT NULL,
        stats_date              DATE            NOT NULL,
        avg_price               NUMERIC(12, 2)  NULL,
        min_price               NUMERIC(12, 2)  NULL,
        max_price               NUMERIC(12, 2)  NULL,
        avg_discount            NUMERIC(5, 2)   NULL,
        avg_rating              NUMERIC(2, 1)   NULL,
        product_count           INTEGER         NULL,
        loaded_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE (brand, stats_date)
    );
    
    CREATE INDEX IF NOT EXISTS idx_agg_brand_date ON analytics.agg_brand_daily_stats(brand, stats_date);
    
    COMMENT ON TABLE analytics.agg_brand_daily_stats IS 'Daily aggregated statistics by brand for trend analysis';
    """
    
    try:
        conn = db_manager.get_connection()
        if not conn:
            return False
        
        cur = conn.cursor()
        cur.execute(analytics_sql)
        conn.commit()
        logger.info("Analytics schema created successfully")
        
        cur.close()
        db_manager.return_connection(conn)
        return True
    
    except Exception as e:
        logger.error(f"Failed to setup analytics schema: {e}")
        return False


# =============================================================================
# DEDUPLICATION & DATA QUALITY
# =============================================================================

def deduplicate_products(
    df: pd.DataFrame,
    db_manager: DatabaseManager
) -> pd.DataFrame:
    """
    Remove duplicate products within the batch and against database.
    
    Args:
        df: DataFrame with new products
        db_manager: DatabaseManager instance
    
    Returns:
        Deduplicated DataFrame.
    """
    # Remove duplicates within batch
    initial_count = len(df)
    df = df.drop_duplicates(subset=["product_name", "brand", "source_marketplace"])
    logger.info(f"Deduplicated batch: {initial_count} → {len(df)} rows")
    
    # Optional: Check against database
    # (Can be implemented based on product_key matching)
    
    return df


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def test_connection():
    """Test database connection and schema setup."""
    db = DatabaseManager()
    
    # Test simple query
    result = db.fetch_query("SELECT 1")
    if result:
        logger.info("✓ Database connection successful")
        logger.info(f"  Result: {result}")
    else:
        logger.error("✗ Database connection failed")
        return False
    
    # Setup schemas
    if setup_staging_schema(db):
        logger.info("✓ Staging schema created")
    else:
        logger.error("✗ Staging schema creation failed")
    
    if setup_analytics_schema(db):
        logger.info("✓ Analytics schema created")
    else:
        logger.error("✗ Analytics schema creation failed")
    
    db.close()
    return True


if __name__ == "__main__":
    test_connection()
