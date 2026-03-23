import unittest
from scraper import SmartphoneScraper

class TestSmartphoneScraper(unittest.TestCase):

    def setUp(self):
        self.scraper = SmartphoneScraper()

    def test_fetch_page(self):
        result = self.scraper.fetch_page("https://example.com")
        self.assertIsNotNone(result)
        self.assertIn("html", result.lower())

    def test_parse_product_cards(self):
        html = "<div class='product-card'>...</div>"
        products = self.scraper.parse_product_cards(html)
        self.assertIsInstance(products, list)

if __name__ == "__main__":
    unittest.main()