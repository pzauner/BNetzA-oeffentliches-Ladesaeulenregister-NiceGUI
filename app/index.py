from __future__ import annotations
from typing import Dict, Any
import os
import json
from .config import INDEX_DIR, STATION_INDEX_PATH


def load_station_index() -> Dict[str, Any]:
    os.makedirs(INDEX_DIR, exist_ok=True)
    if os.path.isfile(STATION_INDEX_PATH):
        try:
            with open(STATION_INDEX_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_station_index(index: Dict[str, Any]) -> None:
    os.makedirs(INDEX_DIR, exist_ok=True)
    with open(STATION_INDEX_PATH, 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

