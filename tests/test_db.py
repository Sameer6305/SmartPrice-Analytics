import unittest
from db import DatabaseManager

class TestDatabaseManager(unittest.TestCase):

    def setUp(self):
        self.db_manager = DatabaseManager()

    def test_connection(self):
        conn = self.db_manager.get_connection()
        self.assertIsNotNone(conn)
        conn.close()

if __name__ == "__main__":
    unittest.main()