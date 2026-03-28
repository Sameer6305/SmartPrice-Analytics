"""
Data Validation Module
======================
Implements data quality checks for scraped product data.

Author: Smart Price Analytics Team
Version: 1.0.0
"""

import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera import Column, Check, Index

logger = logging.getLogger(__name__)


# =============================================================================
# VALIDATION SCHEMA
# =============================================================================

# Define the expected schema for raw product data
PRODUCT_SCHEMA = pa.DataFrameSchema(
    columns={
        "product_name": Column(str, checks=[
            Check.str_length(min_value=3, max_value=500),
        ]),
        "brand": Column(str, checks=[Check.str_length(min_value=1)], nullable=True, required=False),
        "current_price": Column(float, checks=[
            Check.greater_than(0),
            Check.less_than(500000),  # Reasonable max for smartphones
        ]),
        "mrp": Column(float, checks=[
            Check.greater_than(0),
            Check.less_than(500000),
        ], nullable=True, required=False),
        "discount_percentage": Column(float, checks=[
            Check.greater_than_or_equal_to(0),
            Check.less_than_or_equal_to(100),
        ], nullable=True, required=False),
        "customer_rating": Column(float, checks=[
            Check.greater_than_or_equal_to(0),
            Check.less_than_or_equal_to(5),
        ], nullable=True, required=False),
        "review_count": Column(int, checks=[Check.greater_than_or_equal_to(0)], nullable=True, required=False),
        "availability_status": Column(str, nullable=True, required=False),
        "source_marketplace": Column(str),
        "source_url": Column(str, nullable=True, required=False),
        "source_region": Column(str, nullable=True, required=False),
        "scrape_timestamp_utc": Column(object, nullable=True, required=False),
    },
    strict=False,  # Allow extra columns
)


# =============================================================================
# VALIDATION RESULT
# =============================================================================

@dataclass
class ValidationResult:
    """Validation result container."""
    is_valid: bool
    total_records: int
    valid_records: int
    invalid_records: int
    errors: List[str]
    warnings: List[str]
    
    @property
    def valid_percentage(self) -> float:
        """Percentage of valid records."""
        return (self.valid_records / self.total_records * 100) if self.total_records > 0 else 0
    
    def summary(self) -> str:
        """Return summary string."""
        return (
            f"Validation Summary:\n"
            f"  Total: {self.total_records}\n"
            f"  Valid: {self.valid_records} ({self.valid_percentage:.1f}%)\n"
            f"  Invalid: {self.invalid_records}\n"
            f"  Status: {'✓ PASS' if self.is_valid else '✗ FAIL'}"
        )


# =============================================================================
# DATA VALIDATOR
# =============================================================================

class DataValidator:
    """Validates product data against business rules."""
    
    def __init__(self, schema: pa.DataFrameSchema = None):
        """
        Initialize validator.
        
        Args:
            schema: Pandera DataFrameSchema (uses default if None)
        """
        self.schema = schema or PRODUCT_SCHEMA
        self.errors = []
        self.warnings = []
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate a DataFrame.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            ValidationResult object.
        """
        self.errors = []
        self.warnings = []
        
        total_records = len(df)
        valid_records = 0
        invalid_records = 0
        
        # Schema validation
        try:
            self.schema.validate(df)
            logger.info("✓ Data schema validation passed")
            valid_records = total_records
        except pa.errors.SchemaError as e:
            logger.warning(f"Schema validation failed: {e}")
            self.errors.append(f"Schema validation: {str(e)[:200]}")
            
            # Try row-by-row validation for more granular errors
            valid_records, invalid_records = self._validate_row_by_row(df)
        
        # Business rule validation
        self._validate_business_rules(df)
        
        # Determine overall status
        is_valid = invalid_records == 0 and len(self.errors) == 0
        
        result = ValidationResult(
            is_valid=is_valid,
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            errors=self.errors,
            warnings=self.warnings,
        )
        
        logger.info(result.summary())
        return result
    
    def _validate_row_by_row(self, df: pd.DataFrame) -> tuple[int, int]:
        """
        Validate each row individually.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            Tuple of (valid_count, invalid_count)
        """
        valid_count = 0
        invalid_count = 0
        
        for idx, row in df.iterrows():
            is_valid = self._validate_row(row)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        
        return valid_count, invalid_count
    
    def _validate_row(self, row: pd.Series) -> bool:
        """
        Validate a single row.
        
        Args:
            row: Series representing a product
        
        Returns:
            True if valid, False otherwise.
        """
        # Check required fields
        if pd.isna(row.get("product_name")) or len(str(row["product_name"])) < 3:
            return False
        
        if pd.isna(row.get("current_price")) or row["current_price"] <= 0:
            return False
        
        if pd.isna(row.get("source_marketplace")):
            return False
        
        # Check value ranges
        if not pd.isna(row.get("customer_rating")):
            if not (0 <= row["customer_rating"] <= 5):
                return False
        
        # MRP >= Price
        if not pd.isna(row.get("mrp")) and not pd.isna(row.get("current_price")):
            if row["mrp"] < row["current_price"]:
                self.warnings.append(
                    f"Row {row.name}: MRP < Price ({row['mrp']} < {row['current_price']})"
                )
        
        return True
    
    def _validate_business_rules(self, df: pd.DataFrame):
        """
        Apply business-specific validation rules.
        
        Args:
            df: DataFrame to validate
        """
        if "current_price" not in df.columns:
            self.warnings.append("Business rule checks skipped: current_price column missing")
            logger.info(f"Business rule validation: {len(self.warnings)} warnings found")
            return

        # Rule 1: Price within reasonable range
        invalid_price = df[(df["current_price"] < 5000) | (df["current_price"] > 300000)]
        if len(invalid_price) > 0:
            self.warnings.append(
                f"{len(invalid_price)} records have prices outside 5000-300000 range"
            )
        
        # Rule 2: MRP >= Price
        if "mrp" in df.columns:
            invalid_mrp = df[(df["mrp"].notna()) & (df["mrp"] < df["current_price"])]
            if len(invalid_mrp) > 0:
                self.warnings.append(
                    f"{len(invalid_mrp)} records have MRP < Current Price"
                )
        
        # Rule 3: Rating consistency
        if "customer_rating" in df.columns:
            invalid_rating = df[
                (df["customer_rating"].notna()) & 
                ((df["customer_rating"] < 0) | (df["customer_rating"] > 5))
            ]
            if len(invalid_rating) > 0:
                self.warnings.append(
                    f"{len(invalid_rating)} records have invalid ratings (outside 0-5)"
                )
        
        # Rule 4: Discount consistency
        if "discount_percentage" in df.columns:
            invalid_discount = df[
                (df["discount_percentage"].notna()) & 
                ((df["discount_percentage"] < 0) | (df["discount_percentage"] > 100))
            ]
            if len(invalid_discount) > 0:
                self.warnings.append(
                    f"{len(invalid_discount)} records have invalid discount percentages"
                )
        
        logger.info(f"Business rule validation: {len(self.warnings)} warnings found")
    
    def filter_valid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return only valid records from DataFrame.
        
        Args:
            df: DataFrame to filter
        
        Returns:
            Filtered DataFrame with only valid records.
        """
        valid_rows = []
        
        for idx, row in df.iterrows():
            if self._validate_row(row):
                valid_rows.append(row)
        
        if valid_rows:
            return pd.DataFrame(valid_rows)
        else:
            logger.warning("No valid rows after filtering")
            return pd.DataFrame()


# =============================================================================
# VALIDATION REPORT
# =============================================================================

def generate_validation_report(
    df: pd.DataFrame,
    result: ValidationResult,
    output_file: str = None
) -> str:
    """
    Generate a validation report.
    
    Args:
        df: Original DataFrame
        result: ValidationResult
        output_file: Optional file path to save report
    
    Returns:
        Report as string.
    """
    report = f"""
================================================================================
                    DATA VALIDATION REPORT
================================================================================
Generated: {datetime.now().isoformat()}

{result.summary()}

================================================================================
COLUMN STATISTICS
================================================================================
"""
    
    # Column-level stats
    for col in df.columns:
        report += f"\n{col}:\n"
        if df[col].dtype in ['float64', 'int64']:
            report += f"  Min: {df[col].min()}, Max: {df[col].max()}, Mean: {df[col].mean():.2f}\n"
            report += f"  Null Count: {df[col].isna().sum()}\n"
        else:
            report += f"  Unique Values: {df[col].nunique()}\n"
            report += f"  Null Count: {df[col].isna().sum()}\n"
    
    # Errors
    if result.errors:
        report += f"\n================================================================================\nERRORS\n"
        for error in result.errors:
            report += f"  • {error}\n"
    
    # Warnings
    if result.warnings:
        report += f"\n================================================================================\nWARNINGS\n"
        for warning in result.warnings:
            report += f"  • {warning}\n"
    
    report += "\n================================================================================\n"
    
    if output_file:
        with open(output_file, "w") as f:
            f.write(report)
        logger.info(f"Validation report saved to {output_file}")
    
    return report


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Example validation usage."""
    
    # Create sample data
    sample_data = {
        "product_name": ["iPhone 15 Pro", "Samsung Galaxy S24", "Invalid"],
        "brand": ["Apple", "Samsung", None],
        "current_price": [79999.0, 64999.0, -100.0],
        "mrp": [99999.0, 79999.0, 50000.0],
        "discount_percentage": [19.0, 18.5, 200.0],
        "customer_rating": [4.8, 4.5, 6.0],
        "review_count": [1500, 2000, 100],
        "availability_status": ["In Stock", "In Stock", "Out of Stock"],
        "source_marketplace": ["amazon", "flipkart", "amazon"],
        "source_url": ["https://...", "https://...", "https://..."],
        "source_region": ["IN", "IN", "IN"],
        "scrape_timestamp_utc": [datetime.now(), datetime.now(), datetime.now()],
    }
    
    df = pd.DataFrame(sample_data)
    
    # Validate
    validator = DataValidator()
    result = validator.validate(df)
    
    # Generate report
    report = generate_validation_report(df, result, "validation_report.txt")
    print(report)
    
    # Filter valid records
    valid_df = validator.filter_valid_records(df)
    print(f"\nValid records after filtering: {len(valid_df)}")


if __name__ == "__main__":
    main()
