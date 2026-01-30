"""
E-Commerce Product Listing Scraper
==================================
A clean, production-ready web scraper for extracting smartphone product data
from e-commerce listing pages using requests and BeautifulSoup.

Author: Smart Price Analytics Team
Version: 1.0.0
"""

import re
import logging
from datetime import datetime, timezone
from typing import Optional, Any

import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# HTTP Headers to mimic a real browser and avoid basic blocking
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

# Request timeout in seconds
REQUEST_TIMEOUT = 30


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def extract_price(text: Optional[str]) -> Optional[float]:
    """
    Extract numeric price from a string containing currency symbols.
    
    Args:
        text: Raw price string (e.g., "₹12,999", "$999.99", "Rs. 15,000")
    
    Returns:
        Extracted price as float, or None if extraction fails.
    """
    if not text:
        return None
    
    # Remove currency symbols and whitespace, keep digits, commas, and decimals
    cleaned = re.sub(r"[^\d.,]", "", text.strip())
    
    # Remove thousand separators (commas)
    cleaned = cleaned.replace(",", "")
    
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def extract_discount(text: Optional[str]) -> Optional[str]:
    """
    Extract discount percentage or amount from text.
    
    Args:
        text: Raw discount string (e.g., "20% off", "Save ₹2,000")
    
    Returns:
        Normalized discount string, or None if not found.
    """
    if not text:
        return None
    
    # Match percentage discounts (e.g., "20%", "20% off")
    percent_match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if percent_match:
        return f"{percent_match.group(1)}%"
    
    # Match absolute discounts (e.g., "Save ₹2000")
    amount_match = re.search(r"[\d,]+(?:\.\d+)?", text)
    if amount_match:
        amount = amount_match.group().replace(",", "")
        return f"₹{amount}"
    
    return text.strip() if text.strip() else None


def extract_rating(text: Optional[str]) -> Optional[float]:
    """
    Extract numeric rating from text.
    
    Args:
        text: Raw rating string (e.g., "4.5 out of 5", "4.5", "4.5 stars")
    
    Returns:
        Rating as float (0-5 scale), or None if extraction fails.
    """
    if not text:
        return None
    
    # Match rating patterns like "4.5", "4.5/5", "4.5 out of 5"
    match = re.search(r"(\d(?:\.\d)?)", text)
    if match:
        rating = float(match.group(1))
        return rating if 0 <= rating <= 5 else None
    
    return None


def determine_availability(element: Optional[Tag], text: Optional[str]) -> str:
    """
    Determine product availability status from element or text.
    
    Args:
        element: BeautifulSoup element that may contain availability info
        text: Raw availability text
    
    Returns:
        "In Stock", "Out of Stock", or "Unknown"
    """
    if not text and not element:
        return "Unknown"
    
    search_text = text.lower() if text else ""
    
    # Check element attributes for availability indicators
    if element:
        element_str = str(element).lower()
        search_text = f"{search_text} {element_str}"
    
    # Out of stock patterns
    out_of_stock_patterns = [
        "out of stock", "sold out", "unavailable", "not available",
        "currently unavailable", "out-of-stock", "soldout"
    ]
    
    for pattern in out_of_stock_patterns:
        if pattern in search_text:
            return "Out of Stock"
    
    # In stock patterns
    in_stock_patterns = [
        "in stock", "available", "add to cart", "buy now",
        "in-stock", "instock"
    ]
    
    for pattern in in_stock_patterns:
        if pattern in search_text:
            return "In Stock"
    
    return "Unknown"


def safe_get_text(element: Optional[Tag], default: str = "") -> str:
    """
    Safely extract text from a BeautifulSoup element.
    
    Args:
        element: BeautifulSoup Tag element
        default: Default value if element is None
    
    Returns:
        Cleaned text content or default value.
    """
    if element is None:
        return default
    return element.get_text(strip=True)


def find_element_by_patterns(
    container: Tag,
    tag_patterns: list[dict[str, Any]]
) -> Optional[Tag]:
    """
    Find an element using multiple fallback patterns.
    
    Args:
        container: Parent element to search within
        tag_patterns: List of dicts with 'tag' and 'attrs' for find()
    
    Returns:
        First matching element, or None if no pattern matches.
    """
    for pattern in tag_patterns:
        element = container.find(pattern.get("tag"), attrs=pattern.get("attrs", {}))
        if element:
            return element
    return None


# =============================================================================
# CORE SCRAPER CLASS
# =============================================================================

class EcommerceProductScraper:
    """
    A flexible scraper for extracting product data from e-commerce listing pages.
    
    This scraper uses generalized patterns to locate product information,
    making it adaptable to various e-commerce platforms with minimal changes.
    """
    
    def __init__(self, headers: Optional[dict] = None, timeout: int = REQUEST_TIMEOUT):
        """
        Initialize the scraper with custom headers and timeout.
        
        Args:
            headers: Custom HTTP headers (uses defaults if None)
            timeout: Request timeout in seconds
        """
        self.headers = headers or DEFAULT_HEADERS
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch and parse an HTML page.
        
        Args:
            url: URL of the page to fetch
        
        Returns:
            BeautifulSoup object, or None if request fails.
        """
        try:
            logger.info(f"Fetching URL: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            logger.info(f"Successfully fetched page (Status: {response.status_code})")
            return BeautifulSoup(response.content, "html.parser")
        
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out for URL: {url}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for URL: {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for URL: {url} - {str(e)}")
        
        return None
    
    def extract_product_data(self, product_element: Tag) -> dict[str, Any]:
        """
        Extract product data from a single product container element.
        
        Args:
            product_element: BeautifulSoup element containing product info
        
        Returns:
            Dictionary with extracted product fields.
        """
        # Define search patterns for each field (generalized, not site-specific)
        name_patterns = [
            {"tag": "h2", "attrs": {}},
            {"tag": "h3", "attrs": {}},
            {"tag": "a", "attrs": {"class": re.compile(r"title|name|product", re.I)}},
            {"tag": "span", "attrs": {"class": re.compile(r"title|name|product", re.I)}},
            {"tag": "div", "attrs": {"class": re.compile(r"title|name|product", re.I)}},
        ]
        
        current_price_patterns = [
            {"tag": "span", "attrs": {"class": re.compile(r"price|cost|sale|final", re.I)}},
            {"tag": "div", "attrs": {"class": re.compile(r"price|cost|sale|final", re.I)}},
            {"tag": None, "attrs": {"data-price": True}},
        ]
        
        mrp_patterns = [
            {"tag": "span", "attrs": {"class": re.compile(r"mrp|original|strike|was|old", re.I)}},
            {"tag": "del", "attrs": {}},
            {"tag": "s", "attrs": {}},
            {"tag": "strike", "attrs": {}},
        ]
        
        discount_patterns = [
            {"tag": "span", "attrs": {"class": re.compile(r"discount|off|save|percent", re.I)}},
            {"tag": "div", "attrs": {"class": re.compile(r"discount|off|save|percent", re.I)}},
        ]
        
        rating_patterns = [
            {"tag": "span", "attrs": {"class": re.compile(r"rating|star|review", re.I)}},
            {"tag": "div", "attrs": {"class": re.compile(r"rating|star|review", re.I)}},
            {"tag": None, "attrs": {"data-rating": True}},
        ]
        
        availability_patterns = [
            {"tag": "span", "attrs": {"class": re.compile(r"stock|avail|inventory", re.I)}},
            {"tag": "div", "attrs": {"class": re.compile(r"stock|avail|inventory", re.I)}},
        ]
        
        # Extract each field using patterns with graceful fallbacks
        name_elem = find_element_by_patterns(product_element, name_patterns)
        price_elem = find_element_by_patterns(product_element, current_price_patterns)
        mrp_elem = find_element_by_patterns(product_element, mrp_patterns)
        discount_elem = find_element_by_patterns(product_element, discount_patterns)
        rating_elem = find_element_by_patterns(product_element, rating_patterns)
        availability_elem = find_element_by_patterns(product_element, availability_patterns)
        
        # Build product data dictionary
        product_data = {
            "product_name": safe_get_text(name_elem) or None,
            "current_price": extract_price(safe_get_text(price_elem)),
            "mrp": extract_price(safe_get_text(mrp_elem)),
            "discount": extract_discount(safe_get_text(discount_elem)),
            "rating": extract_rating(safe_get_text(rating_elem)),
            "availability": determine_availability(
                availability_elem, 
                safe_get_text(availability_elem)
            ),
            "scrape_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        
        # Calculate discount if MRP and current price are available but discount is not
        if (
            product_data["discount"] is None 
            and product_data["mrp"] 
            and product_data["current_price"]
            and product_data["mrp"] > product_data["current_price"]
        ):
            discount_pct = (
                (product_data["mrp"] - product_data["current_price"]) 
                / product_data["mrp"] * 100
            )
            product_data["discount"] = f"{discount_pct:.1f}%"
        
        return product_data
    
    def find_product_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """
        Locate all product container elements on the page.
        
        Args:
            soup: Parsed HTML page
        
        Returns:
            List of product container elements.
        """
        # Generalized patterns for product containers
        container_patterns = [
            {"attrs": {"class": re.compile(r"product[-_]?card", re.I)}},
            {"attrs": {"class": re.compile(r"product[-_]?item", re.I)}},
            {"attrs": {"class": re.compile(r"product[-_]?tile", re.I)}},
            {"attrs": {"class": re.compile(r"search[-_]?result", re.I)}},
            {"attrs": {"data-component-type": "s-search-result"}},  # Amazon pattern
            {"attrs": {"class": re.compile(r"_1AtVbE", re.I)}},  # Flipkart pattern
        ]
        
        containers = []
        for pattern in container_patterns:
            found = soup.find_all(attrs=pattern.get("attrs"))
            if found:
                containers.extend(found)
                logger.info(f"Found {len(found)} products using pattern: {pattern}")
                break  # Use first matching pattern
        
        return containers
    
    def scrape_listing_page(self, url: str) -> list[dict[str, Any]]:
        """
        Scrape all products from a listing page URL.
        
        Args:
            url: URL of the product listing page
        
        Returns:
            List of dictionaries containing product data.
        """
        soup = self.fetch_page(url)
        if soup is None:
            logger.warning("Failed to fetch page, returning empty result")
            return []
        
        containers = self.find_product_containers(soup)
        logger.info(f"Found {len(containers)} product containers")
        
        products = []
        for idx, container in enumerate(containers, start=1):
            try:
                product_data = self.extract_product_data(container)
                
                # Skip entries without a product name (likely invalid containers)
                if product_data["product_name"]:
                    product_data["source_url"] = url
                    products.append(product_data)
                    logger.debug(f"Extracted product {idx}: {product_data['product_name']}")
            except Exception as e:
                logger.warning(f"Failed to extract product {idx}: {str(e)}")
                continue
        
        logger.info(f"Successfully extracted {len(products)} products")
        return products
    
    def scrape_to_dataframe(self, url: str) -> pd.DataFrame:
        """
        Scrape products and return as a pandas DataFrame.
        
        Args:
            url: URL of the product listing page
        
        Returns:
            DataFrame with product data, ready for analytics pipeline.
        """
        products = self.scrape_listing_page(url)
        
        if not products:
            # Return empty DataFrame with expected schema
            return pd.DataFrame(columns=[
                "product_name", "current_price", "mrp", "discount",
                "rating", "availability", "scrape_timestamp_utc", "source_url"
            ])
        
        df = pd.DataFrame(products)
        
        # Ensure consistent column ordering
        column_order = [
            "product_name", "current_price", "mrp", "discount",
            "rating", "availability", "scrape_timestamp_utc", "source_url"
        ]
        df = df.reindex(columns=column_order)
        
        return df
    
    def close(self):
        """Close the HTTP session."""
        self.session.close()
        logger.info("Scraper session closed")


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def scrape_products(url: str) -> list[dict[str, Any]]:
    """
    Convenience function to scrape products from a URL.
    
    Args:
        url: URL of the product listing page
    
    Returns:
        List of product dictionaries.
    """
    scraper = EcommerceProductScraper()
    try:
        return scraper.scrape_listing_page(url)
    finally:
        scraper.close()


def scrape_products_to_df(url: str) -> pd.DataFrame:
    """
    Convenience function to scrape products and return as DataFrame.
    
    Args:
        url: URL of the product listing page
    
    Returns:
        DataFrame with product data.
    """
    scraper = EcommerceProductScraper()
    try:
        return scraper.scrape_to_dataframe(url)
    finally:
        scraper.close()


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Example: Scrape a product listing page
    # Replace with an actual e-commerce URL for testing
    
    SAMPLE_URL = "https://www.example-ecommerce.com/smartphones"
    
    print("=" * 60)
    print("E-Commerce Product Scraper - Demo Run")
    print("=" * 60)
    
    # Initialize scraper
    scraper = EcommerceProductScraper()
    
    # Scrape products to DataFrame
    df = scraper.scrape_to_dataframe(SAMPLE_URL)
    
    # Display results
    if not df.empty:
        print(f"\nScraped {len(df)} products:\n")
        print(df.to_string(index=False))
        
        # Export to CSV for data warehouse ingestion
        output_file = "scraped_products.csv"
        df.to_csv(output_file, index=False)
        print(f"\nData exported to: {output_file}")
    else:
        print("\nNo products found. Please check the URL and try again.")
    
    # Cleanup
    scraper.close()
    
    print("\n" + "=" * 60)
    print("Scraping complete!")
    print("=" * 60)
