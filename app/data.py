from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, List
import threading
import requests
from bs4 import BeautifulSoup
import pandas as pd
import glob
import os
import logging
import re
import unicodedata
from .config import DOWNLOAD_DIR, BNETZA_PAGE_URL


@dataclass
class DownloadState:
    is_running: bool = False
    progress: float = 0.0
    total_mb: float = 0.0
    downloaded_mb: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def update(self, downloaded: int, total: int):
        with self.lock:
            self.progress = downloaded / total if total > 0 else 0
            self.downloaded_mb = downloaded / 1024 / 1024
            self.total_mb = total / 1024 / 1024

    def start(self):
        with self.lock:
            self.is_running = True
            self.progress = 0.0
            self.total_mb = 0.0
            self.downloaded_mb = 0.0

    def finish(self):
        with self.lock:
            self.is_running = False


def find_csv_download_url(page_url: str) -> Optional[str]:
    try:
        logging.info(f"Fetching page to find download link: {page_url}")
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        link_element = soup.find('a', class_='downloadLink Publication FTcsv', href=lambda href: href and 'Ladesaeulenregister' in href and href.endswith('.csv'))
        if link_element and link_element['href']:
            url = link_element['href']
            logging.info(f"Found download link: {url}")
            return url
    except requests.RequestException as e:
        logging.error(f"Error fetching page {page_url}: {e}")
    except Exception as e:
        logging.error(f"An error occurred during parsing: {e}")
    return None


def download_csv(url: str, dest_folder: str, state: DownloadState) -> bool:
    os.makedirs(dest_folder, exist_ok=True)
    filename = os.path.join(dest_folder, url.split('/')[-1])
    logging.info(f"Starting download of {url} to {filename}")
    state.start()
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            downloaded_size = 0
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    state.update(downloaded_size, total_size)
        logging.info(f"Successfully downloaded {filename}")
        return True
    except Exception as e:
        logging.error(f"Failed to download {url}: {e}")
        return False
    finally:
        state.finish()


def get_available_csvs() -> List[str]:
    if not os.path.isdir(DOWNLOAD_DIR):
        return []
    return sorted([os.path.basename(f) for f in glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv'))], reverse=True)


def _normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize('NFKD', name)
    ascii_only = ''.join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r'[^a-z0-9]+', '', ascii_only.lower())


def _resolve_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    normalized_index = {_normalize_column_name(col): col for col in columns}
    for candidate in candidates:
        resolved = normalized_index.get(_normalize_column_name(candidate))
        if resolved:
            return resolved
    return None


def load_data(csv_filename: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict[str, int]]]:
    if not csv_filename:
        return None, "Keine CSV-Datei ausgewählt.", None
    file_path = os.path.join(DOWNLOAD_DIR, csv_filename)
    if not os.path.exists(file_path):
        return None, f"Datei {csv_filename} nicht gefunden!", None

    try:
        header_row = None
        selected_encoding = None
        for encoding in ('utf-8-sig', 'utf-8', 'latin-1'):
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    for i, line in enumerate(f):
                        if 'Ladeeinrichtungs-ID' in line:
                            header_row = i
                            selected_encoding = encoding
                            break
                if header_row is not None:
                    break
            except UnicodeDecodeError:
                continue

        if header_row is None or selected_encoding is None:
            return None, "Konnte die Kopfzeile in der CSV-Datei nicht finden.", None

        df = pd.read_csv(
            file_path,
            low_memory=False,
            encoding=selected_encoding,
            delimiter=';',
            skiprows=header_row,
            decimal=',',
            dtype={'Postleitzahl': str, 'Ladeeinrichtungs-ID': str},
        )
        raw_rows = len(df)

        canonical_candidates = {
            'Ladeeinrichtungs-ID': ['Ladeeinrichtungs-ID', 'Ladeeinrichtungs ID'],
            'Breitengrad': ['Breitengrad', 'Breitengrad (WGS84)'],
            'Längengrad': ['Längengrad', 'Laengengrad', 'Längengrad (WGS84)'],
            'Betreiber': ['Betreiber'],
            'Nennleistung Ladeeinrichtung [kW]': [
                'Nennleistung Ladeeinrichtung [kW]',
                'Nennleistung Ladeeinrichtung',
            ],
        }

        resolved_map: Dict[str, str] = {}
        for canonical_name, candidates in canonical_candidates.items():
            resolved = _resolve_column(list(df.columns), candidates)
            if not resolved:
                return None, f"Erforderliche Spalte '{canonical_name}' nicht in der CSV-Datei gefunden.", None
            resolved_map[resolved] = canonical_name

        df.rename(columns=resolved_map, inplace=True)

        id_col = 'Ladeeinrichtungs-ID'
        lat_col = 'Breitengrad'
        lon_col = 'Längengrad'
        operator_col = 'Betreiber'
        power_col = 'Nennleistung Ladeeinrichtung [kW]'

        required_cols = [id_col, lat_col, lon_col, operator_col, power_col]
        df.dropna(subset=required_cols, inplace=True)
        df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
        df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
        df.dropna(subset=[lat_col, lon_col], inplace=True)

        cleaned_rows = len(df)
        stats = {'raw': raw_rows, 'cleaned': cleaned_rows}
        return df, None, stats
    except Exception as e:
        logging.error(f"Critical error loading data: {e}", exc_info=True)
        return None, f"Ein kritischer Fehler beim Laden der Daten ist aufgetreten: {e}", None


def get_latest_csv() -> Optional[str]:
    csvs = get_available_csvs()
    return csvs[0] if csvs else None
