import os
import json
from datetime import datetime, timedelta
from typing import Any, Optional

class FileCache:
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _get_cache_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def get(self, key: str) -> Optional[Any]:
        cache_path = self._get_cache_path(key)
        if not os.path.exists(cache_path):
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if datetime.fromisoformat(data['timestamp']) + timedelta(seconds=data['ttl']) < datetime.now():
                    os.remove(cache_path)
                    return None
                return data['value']
        except Exception:
            return None

    def set(self, key: str, value: Any, ttl: int = 86400) -> None:
        cache_path = self._get_cache_path(key)
        data = {
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'ttl': ttl
        }
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)