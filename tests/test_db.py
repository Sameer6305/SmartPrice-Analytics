import unittest
from unittest.mock import Mock, patch
from db import DatabaseManager

class TestDatabaseManager(unittest.TestCase):

    def setUp(self):
        pool_mock = Mock()
        pool_mock.getconn.return_value = Mock()
        self.patcher = patch("db.pool.SimpleConnectionPool", return_value=pool_mock)
        self.patcher.start()
        self.db_manager = DatabaseManager()

    def tearDown(self):
        self.patcher.stop()

    def test_connection(self):
        conn = self.db_manager.get_connection()
        self.assertIsNotNone(conn)

if __name__ == "__main__":
    unittest.main()