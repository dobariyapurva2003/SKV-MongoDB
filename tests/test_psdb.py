"""
Unit and Scale Tests for PSDB
"""
import unittest
from core.database import Database

class TestPSDB(unittest.TestCase):
    def setUp(self):
        self.db = Database("test_data.json")
        self.db.insert("test1", {"name": "Test User", "email": "test@example.com"})

    def test_insert(self):
        self.db.insert("test2", {"name": "New User", "email": "new@example.com"})
        result = self.db.find("test2")
        self.assertEqual(result["name"], "New User")

    def test_find(self):
        result = self.db.find("test1")
        self.assertEqual(result["name"], "Test User")

    def test_update(self):
        self.db.update("test1", {"name": "Updated User", "email": "updated@example.com"})
        result = self.db.find("test1")
        self.assertEqual(result["name"], "Updated User")

    def test_delete(self):
        self.db.delete("test1")
        result = self.db.find("test1")
        self.assertEqual(result, "Key not found")

if __name__ == "__main__":
    unittest.main()
