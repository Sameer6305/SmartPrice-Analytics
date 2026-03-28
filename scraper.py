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
        First matching element or None if no pattern matches.
    """
    for pattern in tag_patterns:
        element = container.find(pattern.get("tag"), pattern.get("attrs"))
        if element:
            return element
    return None


# =============================================================================
# MAIN SCRAPER CLASS
# =============================================================================

class SmartphoneScraper:
    """
    Production-ready web scraper for smartphone product data.
    
    Handles:
    - HTTP requests with retry logic
    - HTML parsing and data extraction
    - Error handling and logging
    - Rate limiting
    """
    
    def __init__(
        self,
        source: str = "amazon",
        base_url: str = "https://www.amazon.in/s",
        headers: dict = None,
        timeout: int = REQUEST_TIMEOUT,
        rate_limit: int = 2
    ):
        """
        Initialize scraper.
        
        Args:
            source: Data source identifier (e.g., 'amazon', 'flipkart')
            base_url: Base URL for the marketplace
            headers: Custom HTTP headers
            timeout: Request timeout in seconds
            rate_limit: Delay between requests in seconds
        """
        self.source = source
        self.base_url = base_url
        self.headers = headers or DEFAULT_HEADERS
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        logger.info(f"Initialized scraper for source: {source}")
    
    def fetch_page(self, url: str, params: dict = None) -> Optional[str]:
        """
        Fetch HTML content from a URL.
        
        Args:
            url: URL to fetch
            params: Query parameters
        
        Returns:
            HTML content as string, or None if request fails.
        """
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout,
                allow_redirects=True
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch {url}: {str(e)}")
            return None
    
    def parse_product_cards(self, html: str) -> list[dict[str, Any]]:
        """
        Parse HTML and extract product data.
        
        Args:
            html: HTML content
        
        Returns:
            List of product dictionaries.
        """
        soup = BeautifulSoup(html, "html.parser")
        products = []
        
        # Source-specific selectors (adaptable for different marketplaces)
        # For demo purposes, using generic patterns
        product_containers = soup.find_all("div", class_=re.compile(r"product|item", re.I))
        
        if not product_containers:
            logger.warning(f"No products found on page")
            return products
        
        logger.info(f"Found {len(product_containers)} product containers")
        
        for container in product_containers:
            try:
                product = self._extract_product_data(container)
                if product:
                    products.append(product)
            except Exception as e:
                logger.debug(f"Error parsing product container: {str(e)}")
                continue
        
        return products
    
    def _extract_product_data(self, container: Tag) -> Optional[dict]:
        """
        Extract product data from a container element.
        
        Args:
            container: BeautifulSoup element with product data
        
        Returns:
            Product dictionary or None if extraction fails.
        """
        # Extract product name
        name_elem = container.find("h2") or container.find("a")
        product_name = safe_get_text(name_elem)
        if not product_name or len(product_name) < 3:
            return None
        
        # Extract price
        price_candidates = container.find_all(re.compile(r"span|div"), class_=re.compile(r"price", re.I))
        price = None
        mrp = None
        
        for elem in price_candidates:
            text = safe_get_text(elem)
            extracted = extract_price(text)
            if extracted:
                if price is None:
                    price = extracted
                elif extracted > price:  # Assume MRP is higher
                    mrp = extracted
                else:
                    mrp = price
                    price = extracted
        
        if not price or price <= 0:
            return None
        
        # Extract rating
        rating_elem = container.find(re.compile(r"span|div"), class_=re.compile(r"rating|stars", re.I))
        rating = extract_rating(safe_get_text(rating_elem)) if rating_elem else None
        
        # Extract review count
        review_text = safe_get_text(container.find(re.compile(r"span"), class_=re.compile(r"reviews?|count", re.I)))
        review_count = None
        if review_text:
            match = re.search(r"(\d+(?:,\d+)*)", review_text)
            if match:
                review_count = int(match.group(1).replace(",", ""))
        
        # Extract discount
        discount_elem = container.find(re.compile(r"span|div"), class_=re.compile(r"discount|off", re.I))
        discount = extract_discount(safe_get_text(discount_elem)) if discount_elem else None
        
        # Extract availability
        availability = determine_availability(
            container.find(re.compile(r"span|div"), class_=re.compile(r"availability|stock", re.I)),
            safe_get_text(container)
        )
        
        # Extract product URL
        url_elem = container.find("a", href=True)
        product_url = url_elem["href"] if url_elem else None
        
        # Extract brand (heuristic: often in product name)
        brand = self._extract_brand(product_name)
        
        product_data = {
            "product_name": product_name.strip(),
            "brand": brand,
            "current_price": price,
            "mrp": mrp or price,
            "discount_percentage": self._calculate_discount_pct(price, mrp) if mrp else None,
            "customer_rating": rating,
            "review_count": review_count,
            "availability_status": availability,
            "source_marketplace": self.source,
            "source_url": product_url,
            "source_region": "IN",
            "scrape_timestamp_utc": datetime.now(timezone.utc),
        }
        
        return product_data
    
    def _extract_brand(self, product_name: str) -> Optional[str]:
        """Extract brand from product name."""
        # Common smartphone brands
        brands = ["Apple", "Samsung", "Xiaomi", "Redmi", "Poco", "OnePlus", "Realme", "Oppo", "Vivo", "Motorola", "Nokia", "Asus"]
        for brand in brands:
            if brand.lower() in product_name.lower():
                return brand
        return None
    
    def _calculate_discount_pct(self, price: float, mrp: float) -> Optional[float]:
        """Calculate discount percentage."""
        if not mrp or mrp <= 0 or mrp <= price:
            return None
        return round((1 - price / mrp) * 100, 2)
    
    def scrape_products(self, search_query: str, num_pages: int = 1) -> list[dict]:
        """
        Scrape multiple pages of products.
        
        Args:
            search_query: Search term for products
            num_pages: Number of pages to scrape
        
        Returns:
            List of all scraped products.
        """
        all_products = []
        
        for page in range(1, num_pages + 1):
            logger.info(f"Scraping page {page}...")
            
            params = {
                "k": search_query,
                "page": page,
            }
            
            html = self.fetch_page(self.base_url, params=params)
            if not html:
                logger.warning(f"Failed to fetch page {page}")
                break
            
            products = self.parse_product_cards(html)
            all_products.extend(products)
            logger.info(f"Page {page}: Extracted {len(products)} products")
            
            # Rate limiting
            if page < num_pages:
                import time
                time.sleep(self.rate_limit)
        
        return all_products


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Example usage of the scraper."""
    
    # Initialize scraper
    scraper = SmartphoneScraper(
        source="amazon",
        base_url="https://www.amazon.in/s",
        rate_limit=2
    )
    
    # Scrape products
    products = scraper.scrape_products(
        search_query="smartphone",
        num_pages=1  # Start with 1 page for testing
    )
    
    if products:
        # Convert to DataFrame
        df = pd.DataFrame(products)
        logger.info(f"\nScraped {len(df)} products")
        logger.info(f"\nSample data:\n{df.head()}")
        
        # Save to CSV for inspection
        df.to_csv("raw_products.csv", index=False)
        logger.info("Data saved to raw_products.csv")
        
        return df
    else:
        logger.warning("No products scraped")
        return None


if __name__ == "__main__":
    main()
