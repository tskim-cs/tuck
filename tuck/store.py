import sqlite3
import json
from typing import Optional, Dict, Any
from collections import OrderedDict

MAX_CACHE_SIZE = 1000
_CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS {namespace} (id TEXT PRIMARY KEY, content TEXT)"


class Store:
    def __init__(self, db_path: str = "tuck.db", max_cache_size: int = MAX_CACHE_SIZE):
        self.db_path = db_path
        self.max_cache_size = max_cache_size
        self._cache: Dict[str, OrderedDict] = {}

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _add_to_cache(self, namespace: str, id: str, content: Any) -> None:
        if namespace not in self._cache:
            self._cache[namespace] = OrderedDict()

        cache = self._cache[namespace]
        if id in cache:
            cache.move_to_end(id)
        else:
            if len(cache) >= self.max_cache_size:
                cache.popitem(last=False)
            cache[id] = content

    def check(self, id: str, namespace: str = "default") -> bool:
        """Check if a key exists in the specified namespace."""
        if namespace in self._cache and id in self._cache[namespace]:
            self._cache[namespace].move_to_end(id)
            return True
        
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(_CREATE_TABLE_SQL.format(namespace=namespace))
            cursor.execute(f"SELECT 1 FROM {namespace} WHERE id = ?", (id,))
            return cursor.fetchone() is not None

    def get(self, id: str, namespace: str = "default") -> Optional[Dict[str, Any]]:
        """Retrieve the value for a key in the specified namespace, or None if not found."""
        if namespace in self._cache and id in self._cache[namespace]:
            self._cache[namespace].move_to_end(id)
            return self._cache[namespace][id]
        
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(_CREATE_TABLE_SQL.format(namespace=namespace))
            cursor.execute(f"SELECT content FROM {namespace} WHERE id = ?", (id,))
            result = cursor.fetchone()
            if result:
                content = json.loads(result[0])
                self._add_to_cache(namespace, id, content)
                return content
            return None

    def set(self, id: str, content: Any, namespace: str = "default") -> None:
        """Set a key-value pair in the specified namespace."""
        content_json = json.dumps(content)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(_CREATE_TABLE_SQL.format(namespace=namespace))
            cursor.execute(f"INSERT OR REPLACE INTO {namespace} (id, content) VALUES (?, ?)", (id, content_json))
            conn.commit()
        self._add_to_cache(namespace, id, content)

    def delete(self, id: str, namespace: str = "default") -> None:
        """Delete a key from the specified namespace."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(_CREATE_TABLE_SQL.format(namespace=namespace))
            cursor.execute(f"DELETE FROM {namespace} WHERE id = ?", (id,))
            conn.commit()
        if namespace in self._cache and id in self._cache[namespace]:
            del self._cache[namespace][id]

    def clear(self, namespace: str = "default") -> None:
        """Clear all key-value pairs in the specified namespace."""
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(_CREATE_TABLE_SQL.format(namespace=namespace))
            cursor.execute(f"DELETE FROM {namespace}")
            conn.commit()
        if namespace in self._cache:
            self._cache[namespace] = OrderedDict()
