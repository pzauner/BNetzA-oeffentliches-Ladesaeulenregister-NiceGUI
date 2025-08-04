from dataclasses import dataclass, field
from nicegui import ui, app, run
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

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

# --- Constants ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(SCRIPT_DIR, 'register-downloads')
BNETZA_PAGE_URL = 'https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html'
MAX_MARKERS_IN_VIEW = 2500
KARLSRUHE_COORDS = (49.0069, 8.4037)

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

@ui.page('/')
async def main_page():
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
        
        for marker in active_markers.values():
            m.remove_layer(marker)
        active_markers.clear()

        if len(df_to_display) > MAX_MARKERS_IN_VIEW:
            ui.notify(f"Anzeigelimit erreicht. Zeige {MAX_MARKERS_IN_VIEW} von {len(df_to_display)}.", type='warning')
            df_to_display = df_to_display.head(MAX_MARKERS_IN_VIEW)

        for _, row in df_to_display.iterrows():
            lade_id = str(row[id_col])
            lat, lon = row[lat_col], row[lon_col]
            
            betreiber = html.escape(str(row[operator_col]))
            adresse = html.escape(f"{row.get('Straße', '')} {row.get('Hausnummer', '')}, {row.get('Postleitzahl', '')} {row.get('Ort', '')}")
            leistung = html.escape(f"{row.get(power_col, 'N/A')} kW")
            google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
            apple_maps_url = f"http://maps.apple.com/?daddr={lat},{lon}"

            popup_html = f"""
                <div>
                    <b>ID:</b> {lade_id}<br>
                    <b>Betreiber:</b> {betreiber}<br>
                    <b>Adresse:</b> {adresse}<br>
                    <b>Leistung:</b> {leistung}<br>
                    <a href="{google_maps_url}" target="_blank">Route mit Google Maps</a><br>
                    <a href="{apple_maps_url}" target="_blank">Route mit Apple Maps</a>
                </div>
            """
            marker = m.marker(latlng=(lat, lon))
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


            with ui.column().classes('w-full'):
                with ui.row().classes('w-full no-wrap'):
                    search_input = ui.input(label='Ladeeinrichtungs-ID suchen').props("clearable").classes('grow')
                    ui.button('Suche', on_click=perform_search)
            
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
            m = ui.leaflet(center=KARLSRUHE_COORDS, zoom=13).classes('h-full')
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

def load_storage_secret(filepath: str = ".secret") -> str:
    """Lädt das storage_secret aus der angegebenen Datei."""
    try:
        with open(filepath, "r") as f:
            secret = f.read().strip()
        if not secret:
            logging.warning(f"Die Secret-Datei '{filepath}' ist leer. Es wird ein temporäres Secret verwendet.")
            return "temp_secret_please_run_setup"
        return secret
    except FileNotFoundError:
        logging.error(f"FEHLER: Secret-Datei '{filepath}' nicht gefunden. Führen Sie 'bash setup.sh' aus.")
        logging.error("Verwende ein unsicheres, temporäres Secret. UI-Elemente funktionieren möglicherweise nicht wie erwartet.")
        return "temp_secret_please_run_setup"

ui.run(
    storage_secret=load_storage_secret()
)
