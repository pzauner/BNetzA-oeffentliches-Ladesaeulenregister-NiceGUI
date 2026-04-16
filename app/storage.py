from __future__ import annotations
from typing import Dict, Any, List
import os
import glob
import json
import re
from .config import CONTEXT_DIR


def sanitize_id(station_id: str) -> str:
    return re.sub(r'[^A-Za-z0-9._\-]', '_', str(station_id))


def get_station_dir(station_id: str) -> str:
    safe_id = sanitize_id(station_id)
    return os.path.join(CONTEXT_DIR, safe_id)


def ensure_station_dir(station_id: str) -> str:
    path = get_station_dir(station_id)
    os.makedirs(path, exist_ok=True)
    return path


def load_notes_html(station_id: str) -> str:
    notes_path = os.path.join(get_station_dir(station_id), 'notes.html')
    if os.path.exists(notes_path):
        try:
            with open(notes_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception:
            return ""
    return ""


def save_notes_html(station_id: str, html_content: str) -> None:
    station_dir = ensure_station_dir(station_id)
    notes_path = os.path.join(station_dir, 'notes.html')
    with open(notes_path, 'w', encoding='utf-8') as f:
        f.write(html_content or "")


def list_station_files(station_id: str) -> List[str]:
    station_dir = get_station_dir(station_id)
    if not os.path.isdir(station_dir):
        return []
    files = [os.path.basename(p) for p in glob.glob(os.path.join(station_dir, '*'))]
    return sorted([f for f in files if f not in ('notes.html', 'meta.json')])


def load_meta(station_id: str) -> Dict[str, Any]:
    path = os.path.join(ensure_station_dir(station_id), 'meta.json')
    if os.path.isfile(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_meta(station_id: str, meta: Dict[str, Any]) -> None:
    path = os.path.join(ensure_station_dir(station_id), 'meta.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def load_public_access_status_map(station_ids: List[str]) -> Dict[str, str]:
    status_map: Dict[str, str] = {}
    for station_id in station_ids:
        safe_id = sanitize_id(station_id)
        meta_path = os.path.join(CONTEXT_DIR, safe_id, 'meta.json')
        if not os.path.isfile(meta_path):
            continue
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                meta = json.load(f)
            status = meta.get('public_access_status')
            if isinstance(status, str) and status:
                status_map[station_id] = status
        except Exception:
            continue
    return status_map


def load_afir_qr_check_map(station_ids: List[str]) -> Dict[str, bool]:
    afir_map: Dict[str, bool] = {}
    for station_id in station_ids:
        safe_id = sanitize_id(station_id)
        meta_path = os.path.join(CONTEXT_DIR, safe_id, 'meta.json')
        if not os.path.isfile(meta_path):
            continue
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                meta = json.load(f)
            afir_value = meta.get('afir_qr_check')
            if isinstance(afir_value, bool):
                afir_map[station_id] = afir_value
        except Exception:
            continue
    return afir_map
