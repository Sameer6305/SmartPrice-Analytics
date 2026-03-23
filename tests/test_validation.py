import unittest
import pandas as pd
from validation import DataValidator

class TestDataValidator(unittest.TestCase):

    def setUp(self):
        self.validator = DataValidator()
        self.sample_data = pd.DataFrame({
            "product_name": ["iPhone 15 Pro"],
            "current_price": [79999.0],
            "source_marketplace": ["amazon"]
        })

    def test_validate(self):
        result = self.validator.validate(self.sample_data)
        self.assertTrue(result.is_valid)

if __name__ == "__main__":
    unittest.main()