import unittest
import os
from .store import Store


class TestStore(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_tuck.db"
        self.store = Store(db_path=self.db_path, max_cache_size=2)
        self.store.clear("test_ns")

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_set_and_check(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        self.assertTrue(self.store.check("id1", namespace="test_ns"))
        self.assertFalse(self.store.check("id2", namespace="test_ns"))

    def test_get(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        result = self.store.get("id1", namespace="test_ns")
        self.assertEqual(result, {"data": "value1"})
        self.assertIsNone(self.store.get("id2", namespace="test_ns"))

    def test_cache_hit(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        first_get = self.store.get("id1", namespace="test_ns")
        second_get = self.store.get("id1", namespace="test_ns")
        self.assertEqual(first_get, second_get)

    def test_cache_size_limit(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        self.store.set("id2", {"data": "value2"}, namespace="test_ns")
        self.store.set("id3", {"data": "value3"}, namespace="test_ns")
        self.assertTrue(len(self.store._cache["test_ns"]) <= 2)
        self.assertNotIn("id1", self.store._cache["test_ns"])
        self.assertIn("id2", self.store._cache["test_ns"])
        self.assertIn("id3", self.store._cache["test_ns"])

    def test_delete(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        self.store.delete("id1", namespace="test_ns")
        self.assertFalse(self.store.check("id1", namespace="test_ns"))
        self.assertNotIn("id1", self.store._cache.get("test_ns", {}))

    def test_clear(self):
        self.store.set("id1", {"data": "value1"}, namespace="test_ns")
        self.store.set("id2", {"data": "value2"}, namespace="test_ns")
        self.store.clear("test_ns")
        self.assertFalse(self.store.check("id1", namespace="test_ns"))
        self.assertFalse(self.store.check("id2", namespace="test_ns"))
        self.assertEqual(self.store._cache["test_ns"], {})


if __name__ == "__main__":
    unittest.main()