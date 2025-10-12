from dataclasses import dataclass, field
from nicegui import ui, app, run
from starlette.requests import Request
from starlette.responses import FileResponse, Response, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
import pandas as pd
import sys
import os
import html
from typing import Dict, List, Any, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import glob
import logging
import threading
import re
import json
import json

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

# --- Constants ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(SCRIPT_DIR, 'register-downloads')
CONTEXT_DIR = os.path.join(SCRIPT_DIR, 'station-context')
BNETZA_PAGE_URL = 'https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html'
MAX_MARKERS_IN_VIEW = 2500
KARLSRUHE_COORDS = (49.0069, 8.4037)
STATION_PAGE_ROUTE = '/station/{station_id}'
INDEX_DIR = os.path.join(CONTEXT_DIR, 'index')
STATION_INDEX_PATH = os.path.join(INDEX_DIR, 'station-index.json')

# --- Data Management & State ---
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

download_state = DownloadState()

# --- Secrets & Auth helpers ---
def parse_secret_file(filepath: str = ".secret") -> Dict[str, str]:
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        if not content:
            return {}
        if '=' not in content and '\n' not in content:
            # legacy format: raw storage secret only
            return { 'STORAGE_SECRET': content }
        values: Dict[str, str] = {}
        for line in content.splitlines():
            if not line.strip() or line.strip().startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                values[k.strip()] = v.strip()
        return values
    except Exception:
        return {}

def load_credentials(filepath: str = ".secret") -> Tuple[str, str]:
    d = parse_secret_file(filepath)
    username = d.get('AUTH_USERNAME', 'admin')
    password = d.get('AUTH_PASSWORD', 'pass1')
    return username, password

@ui.page('/login')
def login(redirect_to: str = '/'):
    expected_user, expected_pass = load_credentials()

    def try_login():
        if username.value == expected_user and password.value == expected_pass:
            app.storage.user.update({'username': username.value, 'authenticated': True})
            ui.navigate.to(redirect_to)
        else:
            ui.notify('Falscher Benutzer oder Passwort', color='negative')

    if app.storage.user.get('authenticated', False):
        ui.navigate.to('/')
        return
    with ui.card().classes('absolute-center'):
        username = ui.input('Username').on('keydown.enter', try_login)
        password = ui.input('Password', password=True, password_toggle_button=True).on('keydown.enter', try_login)
        ui.button('Log in', on_click=try_login)


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware, die Bearbeitungs-Endpunkte für nicht angemeldete Nutzer sperrt."""
    unrestricted = {'/login', '/'}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Allow NiceGUI internal assets and public pages
        if path.startswith('/_nicegui'):
            return await call_next(request)
        if path in self.unrestricted or path.startswith('/station/') or path.startswith('/station-files/'):
            return await call_next(request)
        # For any future API endpoints under /api/edit/ require auth
        if path.startswith('/api/edit/') and not app.storage.user.get('authenticated', False):
            return RedirectResponse(f"/login?redirect_to={path}")
        return await call_next(request)

app.add_middleware(AuthMiddleware)

def find_csv_download_url(page_url: str) -> Optional[str]:
    """Finds the CSV download URL from the BNetzA page."""
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
    """Downloads a CSV file, updating a shared state object."""
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
    """Returns a list of available CSV filenames in the download directory."""
    if not os.path.isdir(DOWNLOAD_DIR):
        return []
    return sorted([os.path.basename(f) for f in glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv'))], reverse=True)

def get_latest_csv() -> Optional[str]:
    csvs = get_available_csvs()
    return csvs[0] if csvs else None

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

def load_data(csv_filename: str) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict[str, int]]]:
    """Loads a CSV file into a pandas DataFrame, skipping the header."""
    if not csv_filename:
        return None, "Keine CSV-Datei ausgewählt.", None
    
    file_path = os.path.join(DOWNLOAD_DIR, csv_filename)
    if not os.path.exists(file_path):
        return None, f"Datei {csv_filename} nicht gefunden!", None

    try:
        # Find the actual header row
        header_row = 0
        with open(file_path, 'r', encoding='latin-1') as f:
            for i, line in enumerate(f):
                if 'Ladeeinrichtungs-ID' in line:
                    header_row = i
                    break
            else:
                return None, "Konnte die Kopfzeile in der CSV-Datei nicht finden.", None

        df = pd.read_csv(
            file_path,
            low_memory=False,
            encoding='latin-1',
            delimiter=';',
            skiprows=header_row,
            decimal=',',
            dtype={'Postleitzahl': str, 'Ladeeinrichtungs-ID': str}
        )
        raw_rows = len(df)
        
        id_col = 'Ladeeinrichtungs-ID'
        lat_col = 'Breitengrad'
        lon_col = 'Längengrad'
        operator_col = 'Betreiber'
        power_col = 'Nennleistung Ladeeinrichtung [kW]'

        required_cols = [id_col, lat_col, lon_col, operator_col, power_col]
        for col in required_cols:
            if col not in df.columns:
                return None, f"Erforderliche Spalte '{col}' nicht in der CSV-Datei gefunden.", None

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

# --- UI Application ---

def sanitize_id(station_id: str) -> str:
    """Sanitize ID for safe filesystem usage while remaining recognizable."""
    return re.sub(r'[^A-Za-z0-9._\-]', '_', str(station_id))

def get_station_dir(station_id: str) -> str:
    safe_id = sanitize_id(station_id)
    return os.path.join(CONTEXT_DIR, safe_id)

def ensure_station_dir(station_id: str) -> str:
    path = get_station_dir(station_id)
    os.makedirs(path, exist_ok=True)
    return path

def load_notes_html(station_id: str) -> str:
    """Load saved HTML notes for a station; returns empty string if not present."""
    notes_path = os.path.join(get_station_dir(station_id), 'notes.html')
    if os.path.exists(notes_path):
        try:
            with open(notes_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception:
            return ""
    return ""

def save_notes_html(station_id: str, html_content: str) -> None:
    """Persist HTML notes for a station."""
    station_dir = ensure_station_dir(station_id)
    notes_path = os.path.join(station_dir, 'notes.html')
    with open(notes_path, 'w', encoding='utf-8') as f:
        f.write(html_content or "")

def list_station_files(station_id: str) -> List[str]:
    """List files stored for the station (excluding notes.html)."""
    station_dir = get_station_dir(station_id)
    if not os.path.isdir(station_dir):
        return []
    files = [os.path.basename(p) for p in glob.glob(os.path.join(station_dir, '*'))]
    return sorted([f for f in files if f != 'notes.html'])

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

def get_station_header_text(row: pd.Series, id_col: str, operator_col: str, power_col: str) -> Dict[str, str]:
    lade_id = str(row[id_col])
    betreiber = html.escape(str(row[operator_col]))
    adresse = html.escape(f"{row.get('Straße', '')} {row.get('Hausnummer', '')}, {row.get('Postleitzahl', '')} {row.get('Ort', '')}")
    leistung = html.escape(f"{row.get(power_col, 'N/A')} kW")
    return {
        'lade_id': lade_id,
        'betreiber': betreiber,
        'adresse': adresse,
        'leistung': leistung,
    }

@app.get('/station-files/{station_id}/{filename}')
async def download_station_file(station_id: str, filename: str):
    safe_id = sanitize_id(station_id)
    safe_name = os.path.basename(filename)
    station_dir = get_station_dir(safe_id)
    file_path = os.path.join(station_dir, safe_name)
    if not os.path.isfile(file_path):
        return Response(status_code=404)
    return FileResponse(file_path, filename=safe_name)

@ui.page(STATION_PAGE_ROUTE)
async def station_page(request: Request, station_id: str):
    id_col = 'Ladeeinrichtungs-ID'
    lat_col = 'Breitengrad'
    lon_col = 'Längengrad'
    operator_col = 'Betreiber'
    power_col = 'Nennleistung Ladeeinrichtung [kW]'

    selected_csv = app.storage.user.get('selected_csv')
    if not selected_csv:
        # auto-select newest available
        selected_csv = get_latest_csv()
        if selected_csv:
            app.storage.user['selected_csv'] = selected_csv
        else:
            ui.notify('Keine lokalen Daten gefunden. Bitte zuerst Daten herunterladen.', type='warning')
            ui.link('Zur Karte', '/').props('color=primary')
            return

    df, error_message, _ = await run.io_bound(load_data, selected_csv)
    if error_message or df is None:
        ui.notify(error_message or 'Konnte Daten nicht laden.', type='negative')
        ui.link('Zur Karte', '/').props('color=primary')
        return

    station_rows = df[df[id_col] == station_id]
    if station_rows.empty:
        # Check station index for last_seen dataset and try to load it
        index = await run.io_bound(load_station_index)
        info = index.get(str(station_id)) if isinstance(index, dict) else None
        last_seen = info.get('last_seen') if isinstance(info, dict) else None
        if last_seen and os.path.isfile(os.path.join(DOWNLOAD_DIR, last_seen)):
            df2, error_message2, _ = await run.io_bound(load_data, last_seen)
            if not error_message2 and df2 is not None:
                station_rows = df2[df2[id_col] == station_id]
                if not station_rows.empty:
                    ui.notify(f"ID nicht in aktuellem Datensatz, aber gefunden in '{last_seen}'.", type='warning')
                    df = df2  # use older dataset for rendering
                else:
                    ui.notify(f"ID {station_id} nicht gefunden.", type='negative')
                    ui.link('Zur Karte', '/').props('color=primary')
                    return
            else:
                ui.notify(f"ID {station_id} nicht im aktuellen Datensatz. Letzter bekannter Datensatz '{last_seen}' nicht ladbar.", type='negative')
                ui.link('Zur Karte', '/').props('color=primary')
                return
        else:
            ui.notify(f"ID {station_id} nicht im aktuellen Datensatz und kein älterer Datensatz vermerkt.", type='negative')
            ui.link('Zur Karte', '/').props('color=primary')
            return

    row = station_rows.iloc[0]
    header = get_station_header_text(row, id_col, operator_col, power_col)
    lat, lon = row[lat_col], row[lon_col]
    google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
    apple_maps_url = f"http://maps.apple.com/?daddr={lat},{lon}"

    with ui.column().classes('w-full max-w-screen-md mx-auto p-4 gap-2'):
        with ui.row().classes('items-center justify-between w-full'):
            ui.label(f"Kontext zu Ladesäule {header['lade_id']}").classes('text-xl font-bold')
            with ui.row().classes('items-center gap-2'):
                if app.storage.user.get('authenticated'):
                    def do_logout():
                        app.storage.user.clear()
                        ui.navigate.to('/login')
                    ui.button('Logout', on_click=do_logout).props('flat dense')
                else:
                    ui.link('Login', f"/login?redirect_to=/station/{station_id}")
                ui.link('Zur Karte', '/').props('color=primary')

        with ui.card().classes('w-full'):
            ui.label(f"ID: {header['lade_id']}")
            ui.label(f"Betreiber: {header['betreiber']}")
            ui.label(f"Adresse: {header['adresse']}")
            ui.label(f"Leistung: {header['leistung']}")
            with ui.row().classes('gap-4 mt-2'):
                ui.link('Route mit Google Maps', google_maps_url, new_tab=True)
                ui.link('Route mit Apple Maps', apple_maps_url, new_tab=True)

        ui.separator()
        if app.storage.user.get('authenticated'):
            ui.label('Notizen').classes('text-lg font-semibold')
            existing_html = await run.io_bound(load_notes_html, station_id)
            editor = ui.editor(value=existing_html, placeholder='Notizen / Kontext erfassen...').classes('w-full h-80')
            with ui.row().classes('justify-end w-full mt-2'):
                async def save_notes():
                    await run.io_bound(save_notes_html, station_id, editor.value)
                    ui.notify('Notizen gespeichert.', type='positive')
                ui.button('Speichern', on_click=save_notes).props('color=primary')
        else:
            ui.label('Bitte einloggen, um Notizen zu bearbeiten.').classes('text-gray-600')

        ui.separator()
        ui.label('Dateien').classes('text-lg font-semibold')
        files_column = ui.column().classes('w-full gap-1')

        async def refresh_files():
            files = await run.io_bound(list_station_files, station_id)
            files_column.clear()
            if files:
                meta = await run.io_bound(load_meta, station_id)
                titles = meta.get('titles', {}) if isinstance(meta, dict) else {}
                for fname in files:
                    with files_column:
                        with ui.row().classes('items-center gap-2 w-full'):
                            title_input = ui.input(value=titles.get(fname, ''), placeholder='Titel')
                            ui.link(fname, f"/station-files/{sanitize_id(station_id)}/{fname}")
                            def rename_factory(name=fname, ti=title_input):
                                async def _rename():
                                    if not app.storage.user.get('authenticated'):
                                        ui.notify('Bitte zuerst einloggen.', type='warning')
                                        return
                                    new_name = await ui.run_javascript('prompt("Neuer Dateiname (inkl. Erweiterung):", arguments[0])', arguments=[name])
                                    if not new_name or new_name == 'null':
                                        return
                                    new_name = os.path.basename(new_name)
                                    old_path = os.path.join(ensure_station_dir(station_id), name)
                                    new_path = os.path.join(ensure_station_dir(station_id), new_name)
                                    if os.path.exists(new_path):
                                        ui.notify('Zieldatei existiert bereits.', type='warning')
                                        return
                                    try:
                                        os.rename(old_path, new_path)
                                    except Exception as e:
                                        ui.notify(f'Umbenennen fehlgeschlagen: {e}', type='negative')
                                        return
                                    m = await run.io_bound(load_meta, station_id)
                                    if not isinstance(m, dict):
                                        m = {}
                                    t = m.get('titles', {}) if isinstance(m.get('titles'), dict) else {}
                                    if name in t:
                                        t[new_name] = t.pop(name)
                                    m['titles'] = t
                                    await run.io_bound(save_meta, station_id, m)
                                    ui.notify('Datei umbenannt.', type='positive')
                                    await refresh_files()
                                return _rename
                            ui.button('Umbenennen', on_click=rename_factory()).props('flat')
                            if fname.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                                def preview_factory(name=fname):
                                    def _open():
                                        with ui.dialog() as d:
                                            with ui.card():
                                                ui.image(f"/station-files/{sanitize_id(station_id)}/{name}").classes('max-w-[80vw] max-h-[80vh]')
                                                ui.button('Schließen', on_click=d.close)
                                        d.open()
                                    return _open
                                ui.button('Vorschau', on_click=preview_factory())
                            def save_title_factory(name=fname, ti=title_input):
                                async def _save():
                                    if not app.storage.user.get('authenticated'):
                                        ui.notify('Bitte zuerst einloggen.', type='warning')
                                        return
                                    m = await run.io_bound(load_meta, station_id)
                                    if not isinstance(m, dict):
                                        m = {}
                                    t = m.get('titles', {}) if isinstance(m.get('titles'), dict) else {}
                                    t[name] = ti.value
                                    m['titles'] = t
                                    await run.io_bound(save_meta, station_id, m)
                                    ui.notify('Titel gespeichert.', type='positive')
                                return _save
                            ui.button('Titel speichern', on_click=save_title_factory())
                            def delete_factory(name=fname):
                                def _delete():
                                    if not app.storage.user.get('authenticated'):
                                        ui.notify('Bitte zuerst einloggen.', type='warning')
                                        return
                                    def do_delete():
                                        try:
                                            os.remove(os.path.join(ensure_station_dir(station_id), name))
                                        except Exception as e:
                                            ui.notify(f'Löschen fehlgeschlagen: {e}', type='negative')
                                            return
                                        ui.notify('Datei gelöscht.', type='positive')
                                        run.async_task(refresh_files())
                                    ui.dialog() \
                                        .classes('p-4')
                                    with ui.dialog() as dlg:
                                        with ui.card():
                                            ui.label(f"'{name}' wirklich löschen?")
                                            with ui.row().classes('justify-end w-full mt-2'):
                                                ui.button('Abbrechen', on_click=dlg.close)
                                                ui.button('Löschen', on_click=lambda: (do_delete(), dlg.close())).props('color=negative')
                                    dlg.open()
                                return _delete
                            ui.button('Löschen', on_click=delete_factory()).props('color=negative flat')
            else:
                with files_column:
                    ui.label('Noch keine Dateien vorhanden.').classes('text-gray-500')

        def on_upload(e):
            dest_dir = ensure_station_dir(station_id)
            try:
                target_path = os.path.join(dest_dir, e.name)
                with open(target_path, 'wb') as f:
                    f.write(e.content.read())
                ui.notify(f'Datei {e.name} gespeichert.', type='positive')
            except Exception as ex:
                ui.notify(f'Upload fehlgeschlagen: {ex}', type='negative')
                return
            ui.timer(0.01, lambda: run.async_task(refresh_files()), once=True)

        if app.storage.user.get('authenticated'):
            ui.upload(multiple=True, auto_upload=True, on_upload=on_upload)
        else:
            ui.label('Bitte einloggen, um Dateien hochzuladen.').classes('text-gray-600')
        await refresh_files()

@ui.page('/')
async def main_page(request: Request):
    df: Optional[pd.DataFrame] = None
    active_markers: Dict[str, Any] = {}
    id_to_open: Optional[str] = None

    id_col = 'Ladeeinrichtungs-ID'
    lat_col = 'Breitengrad'
    lon_col = 'Längengrad'
    operator_col = 'Betreiber'
    power_col = 'Nennleistung Ladeeinrichtung [kW]'

    app.storage.user.setdefault('selected_operators', [])
    app.storage.user.setdefault('selected_powers', [])
    available_csvs = get_available_csvs()
    app.storage.user.setdefault('selected_csv', available_csvs[0] if available_csvs else None)
    app.storage.user.setdefault('panel_is_visible', True)

    async def update_view():
        nonlocal active_markers, id_to_open
        if df is None:
            for marker in active_markers.values():
                m.remove_layer(marker)
            active_markers.clear()
            operator_select.options.clear()
            power_select.options.clear()
            operator_select.update()
            power_select.update()
            return
        
        ui.notify('Aktualisiere Kartenausschnitt...', timeout=1)

        try:
            bounds = await m.run_map_method('getBounds')
            min_lat, min_lon = bounds['_southWest']['lat'], bounds['_southWest']['lng']
            max_lat, max_lon = bounds['_northEast']['lat'], bounds['_northEast']['lng']
        except Exception as e:
            logging.warning(f"Could not get map bounds: {e}")
            return

        df_in_view = df[
            (df[lat_col] >= min_lat) & (df[lat_col] <= max_lat) &
            (df[lon_col] >= min_lon) & (df[lon_col] <= max_lon)
        ]

        # Update filter options based on the current view
        unique_operators = sorted(df_in_view[operator_col].unique())
        unique_powers = sorted(df_in_view[power_col].unique())
        
        if operator_select.options != unique_operators:
            operator_select.options = unique_operators
            operator_select.update()
            
        if power_select.options != unique_powers:
            power_select.options = unique_powers
            power_select.update()

        df_to_display = df_in_view
        selected_ops = app.storage.user.get('selected_operators', [])
        if selected_ops:
            df_to_display = df_to_display[df_to_display[operator_col].isin(selected_ops)]

        selected_powers = app.storage.user.get('selected_powers', [])
        if selected_powers:
            df_to_display = df_to_display[df_to_display[power_col].isin(selected_powers)]
        
        if len(df_to_display) > MAX_MARKERS_IN_VIEW:
            ui.notify(f"Anzeigelimit erreicht. Zeige {MAX_MARKERS_IN_VIEW} von {len(df_to_display)}.", type='warning')
            df_to_display = df_to_display.head(MAX_MARKERS_IN_VIEW)

        # remove previous markers from map
        for marker in active_markers.values():
            m.remove_layer(marker)
        active_markers.clear()

        # group markers by near-identical coordinates (rounded to 5 decimals ~ 1m)
        group_counts: Dict[Tuple[float, float], int] = {}
        for _, row in df_to_display.iterrows():
            key = (round(float(row[lat_col]), 5), round(float(row[lon_col]), 5))
            group_counts[key] = group_counts.get(key, 0) + 1
        group_indices: Dict[Tuple[float, float], int] = {k: 0 for k in group_counts}

        for _, row in df_to_display.iterrows():
            lade_id = str(row[id_col])
            lat, lon = float(row[lat_col]), float(row[lon_col])
            betreiber = html.escape(str(row[operator_col]))
            adresse = html.escape(f"{row.get('Straße', '')} {row.get('Hausnummer', '')}, {row.get('Postleitzahl', '')} {row.get('Ort', '')}")
            leistung = html.escape(f"{row.get(power_col, 'N/A')} kW")
            google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
            apple_maps_url = f"http://maps.apple.com/?daddr={lat},{lon}"
            edit_link = f"/station/{lade_id}"
            popup_html = f"""
                <div>
                    <b>ID:</b> {lade_id}<br>
                    <b>Betreiber:</b> {betreiber}<br>
                    <b>Adresse:</b> {adresse}<br>
                    <b>Leistung:</b> {leistung}<br>
                    <a href=\"{google_maps_url}\" target=\"_blank\">Route mit Google Maps</a><br>
                    <a href=\"{apple_maps_url}\" target=\"_blank\">Route mit Apple Maps</a><br>
                    <a href=\"{edit_link}\" style=\"display:inline-block;margin-top:8px;\">Kontext bearbeiten</a>
                </div>
            """

            key = (round(lat, 5), round(lon, 5))
            count = group_counts.get(key, 1)
            idx = group_indices.get(key, 0)
            group_indices[key] = idx + 1
            # distribute rotation angles evenly
            rotation_angle = (idx * (360.0 / count)) if count > 1 else 0.0

            marker = m.marker(latlng=(lat, lon), options={'rotationAngle': rotation_angle})
            marker.run_method('bindPopup', popup_html)
            active_markers[lade_id] = marker

        if id_to_open and id_to_open in active_markers:
            active_markers[id_to_open].run_method('openPopup')
            id_to_open = None

        ui.notify(f"{len(active_markers)} Ladesäulen angezeigt.", type='positive', timeout=2)

    async def perform_search():
        await search_and_zoom(search_input.value)

    async def search_and_zoom(search_id: str):
        nonlocal id_to_open
        if df is None:
            ui.notify("Bitte zuerst eine Datenquelle auswählen.", type='warning')
            return
        
        search_id = search_id.strip()
        if not search_id:
            return
        
        results = df[df[id_col] == search_id]
        if results.empty:
            ui.notify(f"ID {search_id} nicht gefunden.", type='negative')
            return
        
        station = results.iloc[0]
        lat, lon = station[lat_col], station[lon_col]
        
        id_to_open = search_id
        
        m.run_map_method('setView', [lat, lon], 17)
        if app.storage.user.get('panel_is_visible'):
            m.run_map_method('panBy', [-200, 0])
        
        if id_to_open in active_markers:
            active_markers[id_to_open].run_method('openPopup')

        ui.notify(f"Zoome zu ID {search_id}...", type='info')

    # removed dialog-based editor (replaced by dedicated page)

    async def on_csv_change(e: Any):
        nonlocal df
        app.storage.user['selected_operators'] = []
        app.storage.user['selected_powers'] = []
        new_df, error_message, stats = await run.io_bound(load_data, e.value)
        if error_message:
            ui.notify(error_message, type='negative')
            df = None
        else:
            df = new_df
            if stats:
                removed_count = stats['raw'] - stats['cleaned']
                ui.notify(
                    f"'{os.path.basename(e.value)}' geladen: {stats['cleaned']:,} von {stats['raw']:,} Ladesäulen geladen. "
                    f"({removed_count:,} Einträge wegen fehlender Daten entfernt).",
                    type='positive', multi_line=True, close_button=True
                )
            # Update station index last_seen for current dataset
            try:
                dataset_name = os.path.basename(e.value)
                index = await run.io_bound(load_station_index)
                id_col_local = 'Ladeeinrichtungs-ID'
                if id_col_local in df.columns:
                    for sid in df[id_col_local].astype(str).tolist():
                        entry = index.get(sid) or {}
                        entry['last_seen'] = dataset_name
                        # keep first_seen if present; else set now
                        entry.setdefault('first_seen', dataset_name)
                        index[sid] = entry
                    await run.io_bound(save_station_index, index)
            except Exception:
                pass
        await update_view()

    async def check_and_download_data():
        if not get_available_csvs():
            ui.notify("Keine lokalen Daten gefunden. Starte Download...", type='info')
            await check_for_updates(notify_if_uptodate=False)

    async def check_for_updates(notify_if_uptodate: bool = True):
        if download_state.is_running:
            ui.notify("Ein Download läuft bereits.", type='warning')
            return
            
        ui.notify("Prüfe auf neue Daten...", type='info')
        csv_url = await run.io_bound(find_csv_download_url, BNETZA_PAGE_URL)
        if csv_url:
            latest_filename = os.path.basename(csv_url)
            if latest_filename not in get_available_csvs():
                success = await run.io_bound(download_csv, csv_url, DOWNLOAD_DIR, download_state)
                if success:
                    ui.notify(f"Daten '{latest_filename}' erfolgreich heruntergeladen.", type='positive')
                    new_csvs = get_available_csvs()
                    csv_select.options = new_csvs
                    csv_select.value = latest_filename
                    csv_select.update()
                else:
                    ui.notify("Fehler beim Download. Bitte erneut versuchen.", type='negative')
            elif notify_if_uptodate:
                ui.notify("Daten sind auf dem neuesten Stand.", type='positive')
        else:
            ui.notify("Konnte Download-Link nicht finden.", type='negative')

    def toggle_panel():
        app.storage.user['panel_is_visible'] = not app.storage.user.get('panel_is_visible', True)
        toggle_button.props(f"icon={'menu_open' if app.storage.user['panel_is_visible'] else 'menu'}")

    with ui.row().classes('w-full h-screen p-0 m-0 no-wrap'):
        with ui.column().classes('w-full md:w-1/3 h-full p-4 overflow-auto') \
            .bind_visibility(app.storage.user, 'panel_is_visible') as control_panel:
            
            ui.label('Daten & Filter').classes('text-xl font-bold mb-2')

            with ui.row().classes('w-full items-center'):
                csv_select = ui.select(
                    options=available_csvs,
                    label='Datenquelle auswählen',
                    on_change=on_csv_change
                ).bind_value(app.storage.user, 'selected_csv').classes('grow')
                ui.button(icon='refresh', on_click=check_for_updates).props('flat dense').tooltip('Auf neue Daten prüfen')
                # auth controls visible on main page
                if app.storage.user.get('authenticated'):
                    def do_logout_main():
                        app.storage.user.clear()
                        ui.navigate.to('/login')
                    ui.button('Logout', on_click=do_logout_main).props('flat dense')
                else:
                    ui.link('Login', '/login')


            with ui.column().classes('w-full'):
                with ui.row().classes('w-full no-wrap'):
                    search_input = ui.input(label='Ladeeinrichtungs-ID suchen').props("clearable").classes('grow')
                    ui.button('Suche', on_click=perform_search)
                    async def open_editor_for_search():
                        target_id = (search_input.value or '').strip()
                        if target_id:
                            ui.open(f"/station/{target_id}")
                        else:
                            ui.notify('Bitte eine gültige ID eingeben.', type='warning')
                    ui.button('Editor', on_click=open_editor_for_search).props('outline')
            
            ui.separator().classes('my-4')

            ui.label('Filter').classes('text-lg font-bold')
            with ui.row().classes('w-full no-wrap'):
                with ui.column().classes('w-1/2'):
                    operator_select = ui.select(
                        options=[], label='Betreiber', multiple=True, with_input=True,
                    ).bind_value(app.storage.user, 'selected_operators').classes('w-full')
                with ui.column().classes('w-1/2'):
                    power_select = ui.select(
                        options=[], label='Leistung (kW)', multiple=True, with_input=True,
                    ).props('use-chips').bind_value(app.storage.user, 'selected_powers').classes('w-full')
            
            ui.button('Karte aktualisieren', on_click=update_view).classes('w-full mt-2')

            with ui.column().classes('w-full items-center mt-4') as progress_container:
                progress_bar = ui.linear_progress(value=0).props('instant-feedback').classes('w-full')
                progress_label = ui.label("").classes('text-sm text-gray-500')
            
            def update_progress():
                with download_state.lock:
                    is_running = download_state.is_running
                    progress_container.set_visibility(is_running)
                    if is_running:
                        progress_bar.value = download_state.progress
                        progress_label.text = f"Download: {download_state.downloaded_mb:.2f} / {download_state.total_mb:.2f} MB"
            
            ui.timer(0.1, update_progress, active=True)

        with ui.column().classes('h-full p-0 m-0 grow'):
            m = ui.leaflet(center=KARLSRUHE_COORDS, zoom=13, additional_resources=[
                'https://unpkg.com/leaflet-rotatedmarker@0.2.0/leaflet.rotatedMarker.js',
            ]).classes('h-full')
            m.on('zoomend', update_view, throttle=1.0)
            m.on('dragend', update_view, throttle=1.0)
            
            toggle_button = ui.button(icon='menu_open', on_click=toggle_panel) \
                .props('fab-mini flat color=grey-8').classes('absolute top-2 left-2 z-10')

    await ui.context.client.connected()
    await check_and_download_data()
    selected_csv = app.storage.user.get('selected_csv')
    if selected_csv:
        df, error_message, stats = await run.io_bound(load_data, selected_csv)
        if error_message:
            ui.notify(error_message, type='negative')
        elif stats:
            removed_count = stats['raw'] - stats['cleaned']
            ui.notify(
                f"'{os.path.basename(selected_csv)}' geladen: {stats['cleaned']:,} von {stats['raw']:,} Ladesäulen geladen. "
                f"({removed_count:,} Einträge wegen fehlender Daten entfernt).",
                type='positive', multi_line=True, close_button=True
            )
    await update_view()

    # no dialog open on load anymore

def load_storage_secret(filepath: str = ".secret") -> str:
    """Lädt das storage_secret aus .secret (neu: key=value Format)."""
    try:
        values = parse_secret_file(filepath)
        secret = values.get('STORAGE_SECRET') or values.get('storage_secret')
        if not secret:
            logging.warning(f"Die Secret-Datei '{filepath}' enthält kein STORAGE_SECRET. Es wird ein temporäres Secret verwendet.")
            return "temp_secret_please_run_setup"
        return secret
    except FileNotFoundError:
        logging.error(f"FEHLER: Secret-Datei '{filepath}' nicht gefunden. Führen Sie 'bash setup.sh' aus.")
        logging.error("Verwende ein unsicheres, temporäres Secret. UI-Elemente funktionieren möglicherweise nicht wie erwartet.")
        return "temp_secret_please_run_setup"

ui.run(
    storage_secret=load_storage_secret()
)
