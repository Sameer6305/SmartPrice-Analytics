import unittest
from unittest.mock import Mock, patch
from scraper import SmartphoneScraper

class TestSmartphoneScraper(unittest.TestCase):

    def setUp(self):
        self.scraper = SmartphoneScraper()

    @patch("scraper.requests.Session.get")
    def test_fetch_page(self, mock_get):
        mock_response = Mock()
        mock_response.text = "<html><body>ok</body></html>"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.scraper.fetch_page("https://example.com")
        self.assertIsNotNone(result)
        self.assertIn("html", result.lower())

    def test_parse_product_cards(self):
        html = """
        <div class='product-card'>
            <h2>Samsung Galaxy S24</h2>
            <span class='price'>₹65,000</span>
            <span class='rating'>4.5</span>
        </div>
        """
        products = self.scraper.parse_product_cards(html)
        self.assertIsInstance(products, list)
        self.assertEqual(len(products), 1)
        self.assertEqual(products[0]["product_name"], "Samsung Galaxy S24")

if __name__ == "__main__":
    unittest.main()