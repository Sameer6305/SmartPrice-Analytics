"""
Database Setup Helper
=====================
Simplifies PostgreSQL database initialization.

Usage:
  python setup_db.py
"""

import os
import subprocess
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()


def create_database():
    """Create PostgreSQL database."""
    
    db_name = os.getenv("DB_NAME", "smart_price_analytics")
    db_user = os.getenv("DB_USER", "postgres")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    logger.info(f"Creating database: {db_name}")
    logger.info(f"  Host: {db_host}:{db_port}")
    logger.info(f"  User: {db_user}")
    
    # Try psql command
    try:
        cmd = [
            "psql",
            "-h", db_host,
            "-U", db_user,
            "-p", db_port,
            "-c", f"CREATE DATABASE {db_name};"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✓ Database '{db_name}' created successfully")
            return True
        elif "already exists" in result.stderr:
            logger.info(f"✓ Database '{db_name}' already exists")
            return True
        else:
            logger.error(f"✗ Error creating database:")
            logger.error(result.stderr)
            return False
    
    except FileNotFoundError:
        logger.error("✗ psql command not found. Install PostgreSQL client tools.")
        return False


def run_schema_script():
    """Run staging schema creation script."""
    
    schema_file = Path(__file__).parent / "sql" / "staging_schema.sql"
    
    if not schema_file.exists():
        logger.error(f"✗ Schema file not found: {schema_file}")
        return False
    
    db_name = os.getenv("DB_NAME", "smart_price_analytics")
    db_user = os.getenv("DB_USER", "postgres")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    
    logger.info(f"Running schema script: {schema_file}")
    
    try:
        with open(schema_file, "r") as f:
            schema_sql = f.read()
        
        cmd = [
            "psql",
            "-h", db_host,
            "-U", db_user,
            "-p", db_port,
            "-d", db_name,
            "-f", str(schema_file)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✓ Staging schema created successfully")
            return True
        else:
            logger.error(f"✗ Error running schema script:")
            logger.error(result.stderr)
            return False
    
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        return False


def main():
    """Execute setup."""
    
    logger.info("=" * 60)
    logger.info("SmartPrice Analytics - Database Setup")
    logger.info("=" * 60)
    
    # Check for .env file
    if not Path(".env").exists():
        logger.warning("⚠ .env file not found. Using defaults or creating from .env.example...")
        if Path(".env.example").exists():
            import shutil
            shutil.copy(".env.example", ".env")
            logger.info("✓ Created .env from .env.example (please edit with your credentials)")
        else:
            logger.error("✗ .env.example not found")
            return 1
    
    # Create database
    if not create_database():
        logger.error("Failed to create database")
        return 1
    
    # Run schema
    if not run_schema_script():
        logger.error("Failed to run schema script")
        return 1
    
    logger.info("\n" + "=" * 60)
    logger.info("✓ Database setup completed successfully!")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. pip install -r requirements-etl.txt")
    logger.info("2. python pipeline.py --dry-run")
    logger.info("3. python pipeline.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
