from nicegui import ui, app, run
from starlette.requests import Request
from starlette.responses import FileResponse, Response, RedirectResponse
import pandas as pd
import sys
import os
import html
import math
from typing import Dict, List, Any, Optional
import logging
import json
from app.config import DOWNLOAD_DIR, CONTEXT_DIR, BNETZA_PAGE_URL, MAX_MARKERS_IN_VIEW, KARLSRUHE_COORDS, STATION_PAGE_ROUTE
from app.data import DownloadState, find_csv_download_url, download_csv, get_available_csvs, load_data, get_latest_csv
from app.storage import sanitize_id, get_station_dir, ensure_station_dir, load_notes_html, save_notes_html, list_station_files, load_meta, save_meta, load_public_access_status_map, load_afir_qr_check_map
import asyncio
from app.index import load_station_index, save_station_index
from app.auth import login, AuthMiddleware, load_storage_secret

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)

# constants moved to app.config
PUBLIC_ACCESS_OPTIONS: Dict[str, str] = {
    'eindeutig_oeffentlich': 'Eindeutig öffentlich zugänglich',
    'uneindeutig': 'Uneindeutig',
    'eindeutig_nicht_oeffentlich': 'Eindeutig nicht öffentlich zugänglich',
    'ungeprueft': 'Ungeprüft',
}
PUBLIC_ACCESS_COLORS: Dict[str, str] = {
    'eindeutig_oeffentlich': '#2e7d32',       # grün
    'uneindeutig': '#f9a825',                 # gelb
    'eindeutig_nicht_oeffentlich': '#c62828', # rot
    'ungeprueft': '#607d8b',                  # blau-grau
}
REVIEWED_PUBLIC_ACCESS_STATUSES = {
    'eindeutig_oeffentlich',
    'uneindeutig',
    'eindeutig_nicht_oeffentlich',
}
AFIR_QR_COLOR = '#ff00ff'  # magenta
DEFAULT_PUBLIC_ACCESS_STATUS = 'ungeprueft'

# --- Data Management & State ---

download_state = DownloadState()
app.add_middleware(AuthMiddleware)

# data and index helpers are imported from app.data / app.index

# --- UI Application ---

# storage helpers are imported from app.storage

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


def _normalize_text_value(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()


def find_same_location_station_ids(
    dataframe: pd.DataFrame,
    reference_row: pd.Series,
    station_id: str,
    id_col: str,
    lat_col: str,
    lon_col: str,
) -> List[str]:
    coord_mask = pd.Series(False, index=dataframe.index)
    ref_lat = reference_row.get(lat_col)
    ref_lon = reference_row.get(lon_col)
    if pd.notna(ref_lat) and pd.notna(ref_lon):
        coord_mask = (dataframe[lat_col] == ref_lat) & (dataframe[lon_col] == ref_lon)

    address_fields = ['Straße', 'Hausnummer', 'Postleitzahl', 'Ort']
    ref_address = [_normalize_text_value(reference_row.get(col)) for col in address_fields]
    address_mask = pd.Series(False, index=dataframe.index)
    if any(ref_address):
        address_mask = pd.Series(True, index=dataframe.index)
        for col, value in zip(address_fields, ref_address):
            series = dataframe[col].fillna('').astype(str).str.strip() if col in dataframe.columns else pd.Series('', index=dataframe.index)
            address_mask &= (series == value)

    location_mask = coord_mask | address_mask
    same_location_ids = dataframe.loc[location_mask, id_col].dropna().astype(str).unique().tolist()
    if station_id not in same_location_ids:
        same_location_ids.append(station_id)
    return same_location_ids


def save_public_access_status_for_ids(station_ids: List[str], selected_status: str) -> int:
    updated = 0
    for sid in station_ids:
        meta = load_meta(sid)
        if not isinstance(meta, dict):
            meta = {}
        meta['public_access_status'] = selected_status
        save_meta(sid, meta)
        updated += 1
    return updated


def save_afir_qr_check_for_ids(station_ids: List[str], enabled: bool) -> int:
    updated = 0
    for sid in station_ids:
        meta = load_meta(sid)
        if not isinstance(meta, dict):
            meta = {}
        meta['afir_qr_check'] = bool(enabled)
        if enabled and not meta.get('afir_qr_check_note'):
            meta['afir_qr_check_note'] = 'Check dynamischer QR-Code nach AFIR?'
        save_meta(sid, meta)
        updated += 1
    return updated

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
        ui.label('Zugangsstatus').classes('text-lg font-semibold')
        meta = await run.io_bound(load_meta, station_id)
        current_public_access = meta.get('public_access_status', DEFAULT_PUBLIC_ACCESS_STATUS)
        if current_public_access not in PUBLIC_ACCESS_OPTIONS:
            current_public_access = DEFAULT_PUBLIC_ACCESS_STATUS

        if app.storage.user.get('authenticated'):
            async def save_public_access_status(e: Any) -> None:
                selected_status = e.value if e.value in PUBLIC_ACCESS_OPTIONS else DEFAULT_PUBLIC_ACCESS_STATUS
                same_location_ids = await run.io_bound(
                    find_same_location_station_ids,
                    df,
                    row,
                    station_id,
                    id_col,
                    lat_col,
                    lon_col,
                )
                updated_count = await run.io_bound(save_public_access_status_for_ids, same_location_ids, selected_status)
                ui.notify(f'Zugangsstatus für {updated_count} Einträge am Standort gespeichert.', type='positive')

            status_toggle = ui.toggle(
                options=PUBLIC_ACCESS_OPTIONS,
                value=current_public_access,
                on_change=save_public_access_status,
            ).classes('w-full')
        else:
            status_toggle = ui.toggle(
                options=PUBLIC_ACCESS_OPTIONS,
                value=current_public_access,
            ).classes('w-full')
            status_toggle.disable()
            ui.label('Bitte einloggen, um den Zugangsstatus zu ändern.').classes('text-gray-600')

        ui.separator()
        ui.label('AFIR-Check').classes('text-lg font-semibold')
        current_afir_qr_check = bool(meta.get('afir_qr_check', False))
        if app.storage.user.get('authenticated'):
            async def save_afir_qr_check(e: Any) -> None:
                same_location_ids = await run.io_bound(
                    find_same_location_station_ids,
                    df,
                    row,
                    station_id,
                    id_col,
                    lat_col,
                    lon_col,
                )
                enabled = bool(e.value)
                updated_count = await run.io_bound(save_afir_qr_check_for_ids, same_location_ids, enabled)
                ui.notify(
                    f'AFIR-QR-Check für {updated_count} Einträge am Standort {"aktiviert" if enabled else "deaktiviert"}.',
                    type='positive',
                )

            ui.switch(
                text='Check dynamischer QR-Code nach AFIR?',
                value=current_afir_qr_check,
                on_change=save_afir_qr_check,
            )
        else:
            afir_switch = ui.switch(
                text='Check dynamischer QR-Code nach AFIR?',
                value=current_afir_qr_check,
            )
            afir_switch.disable()
            ui.label('Bitte einloggen, um den AFIR-Check zu ändern.').classes('text-gray-600')

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
            # Upload control appears above the list inside the files column
            if app.storage.user.get('authenticated'):
                with files_column:
                    ui.label('Dateien hochladen').classes('text-md font-semibold')
                    ui.upload(multiple=True, auto_upload=True, on_upload=on_upload)
            if files:
                meta = await run.io_bound(load_meta, station_id)
                titles = meta.get('titles', {}) if isinstance(meta, dict) else {}
                for fname in files:
                    with files_column:
                        with ui.row().classes('items-center gap-2 w-full'):
                            with ui.row().classes('items-center gap-2'):
                                title_input = ui.input(value=titles.get(fname, ''), placeholder='Name')
                                async def save_name(name=fname, ti=title_input):
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
                                    ui.notify('Name gespeichert.', type='positive')
                                ui.button(on_click=save_name, icon='check').props('flat color=positive').tooltip('Name speichern')
                            ui.link(fname, f"/station-files/{sanitize_id(station_id)}/{fname}")
                            def rename_factory(name=fname, ti=title_input):
                                def _open_dialog():
                                    with ui.dialog() as dlg:
                                        with ui.card():
                                            ui.label('Datei umbenennen')
                                            new_name_input = ui.input(value=name, label='Neuer Dateiname (inkl. Erweiterung)')
                                            with ui.row().classes('justify-end w-full mt-2'):
                                                ui.button('Abbrechen', on_click=dlg.close)
                                                async def do_rename():
                                                    if not app.storage.user.get('authenticated'):
                                                        ui.notify('Bitte zuerst einloggen.', type='warning')
                                                        return
                                                    new_name = os.path.basename(new_name_input.value or '')
                                                    if not new_name:
                                                        ui.notify('Name darf nicht leer sein.', type='warning')
                                                        return
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
                                                    dlg.close()
                                                    await refresh_files()
                                                ui.button('Speichern', on_click=do_rename).props('color=primary')
                                    dlg.open()
                                return _open_dialog
                            ui.button('Umbenennen', on_click=rename_factory()).props('flat')
                            if fname.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                                # Zeige Bilder direkt inline größer und optional in Dialog
                                ui.image(f"/station-files/{sanitize_id(station_id)}/{fname}").classes('max-w-[60vw] max-h-[40vh] rounded')
                                def preview_factory(name=fname):
                                    def _open():
                                        with ui.dialog() as d:
                                            d.props('maximized')
                                            with ui.card().classes('w-full h-full'):
                                                ui.image(f"/station-files/{sanitize_id(station_id)}/{name}").classes('w-full h-full object-contain rounded')
                                                ui.button('Schließen', on_click=d.close).classes('absolute top-2 right-2')
                                        d.open()
                                    return _open
                                ui.button('Vollbild', on_click=preview_factory())
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
                                        ui.timer(0.01, lambda: asyncio.create_task(refresh_files()), once=True)
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
            # Direkt-Upload: schneller & stabiler auf mobilen Browsern
            dest_dir = ensure_station_dir(station_id)
            try:
                target_path = os.path.join(dest_dir, e.name)
                with open(target_path, 'wb') as f:
                    f.write(e.content.read())
                ui.notify(f'Datei {e.name} gespeichert.', type='positive')
            except Exception as ex:
                ui.notify(f'Upload fehlgeschlagen: {ex}', type='negative')
                return
            ui.timer(0.01, lambda: asyncio.create_task(refresh_files()), once=True)

        if not app.storage.user.get('authenticated'):
            ui.label('Bitte einloggen, um Dateien hochzuladen.').classes('text-gray-600')
        await refresh_files()

@ui.page('/')
async def main_page(request: Request):
    df: Optional[pd.DataFrame] = None
    active_markers: Dict[str, Any] = {}
    marker_render_state: Dict[str, tuple[float, float, str, str]] = {}
    id_to_open: Optional[str] = None
    is_view_updating = False
    last_bounds: Optional[Dict[str, float]] = None

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
    app.storage.user.setdefault('map_center', [KARLSRUHE_COORDS[0], KARLSRUHE_COORDS[1]])
    app.storage.user.setdefault('map_zoom', 13)
    loaded_csv_name: Optional[str] = app.storage.user.get('selected_csv')

    def get_initial_map_view() -> tuple[tuple[float, float], int]:
        center = app.storage.user.get('map_center', KARLSRUHE_COORDS)
        zoom = app.storage.user.get('map_zoom', 13)
        try:
            if isinstance(center, (list, tuple)) and len(center) == 2:
                lat = float(center[0])
                lon = float(center[1])
                center_out = (lat, lon)
            else:
                center_out = KARLSRUHE_COORDS
        except Exception:
            center_out = KARLSRUHE_COORDS
        try:
            zoom_out = int(zoom)
        except Exception:
            zoom_out = 13
        return center_out, zoom_out

    async def update_view():
        nonlocal active_markers, marker_render_state, id_to_open, is_view_updating, last_bounds
        if is_view_updating:
            return
        is_view_updating = True
        view_update_spinner.set_visibility(True)
        try:
            # Persist current map view for "Zur Karte" round-trip.
            try:
                current_center = m.center
                if isinstance(current_center, (list, tuple)) and len(current_center) == 2:
                    app.storage.user['map_center'] = [float(current_center[0]), float(current_center[1])]
                app.storage.user['map_zoom'] = int(m.zoom)
            except Exception:
                pass

            if df is None:
                for marker in active_markers.values():
                    m.remove_layer(marker)
                active_markers.clear()
                marker_render_state.clear()
                operator_select.options.clear()
                power_select.options.clear()
                operator_select.update()
                power_select.update()
                return

            bounds_ready = False
            df_in_view = df
            try:
                bounds = await m.run_map_method('getBounds', timeout=2.5)
                if isinstance(bounds, dict):
                    sw = bounds.get('_southWest')
                    ne = bounds.get('_northEast')
                    if isinstance(sw, dict) and isinstance(ne, dict):
                        min_lat, min_lon = sw.get('lat'), sw.get('lng')
                        max_lat, max_lon = ne.get('lat'), ne.get('lng')
                        if None not in (min_lat, min_lon, max_lat, max_lon):
                            bounds_ready = True
                            last_bounds = {
                                'min_lat': float(min_lat),
                                'min_lon': float(min_lon),
                                'max_lat': float(max_lat),
                                'max_lon': float(max_lon),
                            }
                            df_in_view = df[
                                (df[lat_col] >= float(min_lat)) & (df[lat_col] <= float(max_lat)) &
                                (df[lon_col] >= float(min_lon)) & (df[lon_col] <= float(max_lon))
                            ]
            except Exception as e:
                logging.warning(f"Could not get map bounds: {e}")
            if not bounds_ready:
                if last_bounds is not None:
                    df_in_view = df[
                        (df[lat_col] >= last_bounds['min_lat']) & (df[lat_col] <= last_bounds['max_lat']) &
                        (df[lon_col] >= last_bounds['min_lon']) & (df[lon_col] <= last_bounds['max_lon'])
                    ]
                else:
                    # startup fallback around current center to avoid "empty map" and avoid full dataset
                    center_lat, center_lon = m.center
                    lat_span = 0.20
                    lon_span = 0.30
                    df_in_view = df[
                        (df[lat_col] >= center_lat - lat_span) & (df[lat_col] <= center_lat + lat_span) &
                        (df[lon_col] >= center_lon - lon_span) & (df[lon_col] <= center_lon + lon_span)
                    ]

            # Betreiber global aus gesamtem Datensatz, Leistung weiterhin ausschnittsbasiert.
            # Keep currently selected values in options to avoid implicit reset by the select widget.
            unique_operators = sorted(df[operator_col].unique())
            unique_powers = sorted(df_in_view[power_col].unique())
            selected_powers = app.storage.user.get('selected_powers', [])
            for selected_power in selected_powers:
                if selected_power not in unique_powers:
                    unique_powers.append(selected_power)

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

            # group identical coordinates and place them in small rings around the real point
            grouped_rows: Dict[tuple[float, float], List[pd.Series]] = {}
            for _, row in df_to_display.iterrows():
                key = (round(float(row[lat_col]), 6), round(float(row[lon_col]), 6))
                grouped_rows.setdefault(key, []).append(row)

            visible_station_ids = df_to_display[id_col].astype(str).tolist()
            public_access_status_map = await run.io_bound(load_public_access_status_map, visible_station_ids)
            afir_qr_check_map = await run.io_bound(load_afir_qr_check_map, visible_station_ids)

            new_marker_specs: Dict[str, tuple[float, float, str, str]] = {}

            for _, rows_at_location in grouped_rows.items():
                count_at_location = len(rows_at_location)
                for idx, row in enumerate(rows_at_location):
                    lade_id = str(row[id_col])
                    base_lat, base_lon = float(row[lat_col]), float(row[lon_col])
                    betreiber = html.escape(str(row[operator_col]))
                    adresse = html.escape(f"{row.get('Straße', '')} {row.get('Hausnummer', '')}, {row.get('Postleitzahl', '')} {row.get('Ort', '')}")
                    leistung = html.escape(f"{row.get(power_col, 'N/A')} kW")
                    status_key = public_access_status_map.get(lade_id, DEFAULT_PUBLIC_ACCESS_STATUS)
                    if status_key not in PUBLIC_ACCESS_OPTIONS:
                        status_key = DEFAULT_PUBLIC_ACCESS_STATUS
                    status_label = PUBLIC_ACCESS_OPTIONS[status_key]
                    has_afir_qr_check = afir_qr_check_map.get(lade_id, False)
                    afir_qr_check_label = 'Ja' if has_afir_qr_check else 'Nein'

                    if count_at_location > 1:
                        layer = idx // 8
                        pos = idx % 8
                        slots_in_layer = min(8, count_at_location - (layer * 8))
                        angle = (2 * math.pi * pos) / max(slots_in_layer, 1)
                        radius_m = 7.0 + (layer * 7.0)
                        lat_offset = (radius_m / 111_320.0) * math.sin(angle)
                        lon_scale = 111_320.0 * max(math.cos(math.radians(base_lat)), 0.2)
                        lon_offset = (radius_m / lon_scale) * math.cos(angle)
                        marker_lat = base_lat + lat_offset
                        marker_lon = base_lon + lon_offset
                    else:
                        marker_lat = base_lat
                        marker_lon = base_lon

                    google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={base_lat},{base_lon}"
                    apple_maps_url = f"http://maps.apple.com/?daddr={base_lat},{base_lon}"
                    edit_link = f"/station/{lade_id}"
                    popup_html = f"""
                        <div>
                            <b>ID:</b> {lade_id}<br>
                            <b>Betreiber:</b> {betreiber}<br>
                            <b>Adresse:</b> {adresse}<br>
                            <b>Leistung:</b> {leistung}<br>
                            <b>Zugangsstatus:</b> {status_label}<br>
                            <b>AFIR-QR-Check:</b> {afir_qr_check_label}<br>
                            <b>Säulen am Standort:</b> {count_at_location}<br>
                            <a href=\"{google_maps_url}\" target=\"_blank\">Route mit Google Maps</a><br>
                            <a href=\"{apple_maps_url}\" target=\"_blank\">Route mit Apple Maps</a><br>
                            <a href=\"{edit_link}\" style=\"display:inline-block;margin-top:8px;\">Kontext bearbeiten</a>
                        </div>
                    """

                    if status_key in REVIEWED_PUBLIC_ACCESS_STATUSES:
                        marker_color = PUBLIC_ACCESS_COLORS[status_key]
                    elif has_afir_qr_check:
                        marker_color = AFIR_QR_COLOR
                    else:
                        marker_color = PUBLIC_ACCESS_COLORS.get(status_key, PUBLIC_ACCESS_COLORS[DEFAULT_PUBLIC_ACCESS_STATUS])
                    new_marker_specs[lade_id] = (marker_lat, marker_lon, marker_color, popup_html)

            current_ids = set(active_markers.keys())
            new_ids = set(new_marker_specs.keys())

            # remove markers no longer visible
            for marker_id in (current_ids - new_ids):
                marker = active_markers.pop(marker_id, None)
                if marker is not None:
                    m.remove_layer(marker)
                marker_render_state.pop(marker_id, None)

            # add/update only changed markers
            for marker_id, (marker_lat, marker_lon, marker_color, popup_html) in new_marker_specs.items():
                old_spec = marker_render_state.get(marker_id)
                new_spec = (round(marker_lat, 7), round(marker_lon, 7), marker_color, popup_html)
                if old_spec == new_spec and marker_id in active_markers:
                    continue

                old_marker = active_markers.pop(marker_id, None)
                if old_marker is not None:
                    m.remove_layer(old_marker)

                marker = m.generic_layer(name='circleMarker', args=[
                    {'lat': marker_lat, 'lng': marker_lon},
                    {
                        'radius': 8,
                        'color': marker_color,
                        'fillColor': marker_color,
                        'fillOpacity': 0.9,
                        'weight': 2,
                    },
                ])
                marker.run_method('bindPopup', popup_html)
                active_markers[marker_id] = marker
                marker_render_state[marker_id] = new_spec

            if id_to_open and id_to_open in active_markers:
                active_markers[id_to_open].run_method('openPopup')
                id_to_open = None
        finally:
            view_update_spinner.set_visibility(False)
            is_view_updating = False

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
        nonlocal df, loaded_csv_name
        new_csv = e.value
        dataset_changed = (loaded_csv_name is not None and new_csv != loaded_csv_name)
        if dataset_changed:
            app.storage.user['selected_operators'] = []
            app.storage.user['selected_powers'] = []
            operator_select.value = []
            power_select.value = []
            operator_select.update()
            power_select.update()
        new_df, error_message, stats = await run.io_bound(load_data, new_csv)
        if error_message:
            ui.notify(error_message, type='negative')
            df = None
        else:
            df = new_df
            loaded_csv_name = new_csv
            if stats:
                removed_count = stats['raw'] - stats['cleaned']
                ui.notify(
                    f"'{os.path.basename(new_csv)}' geladen: {stats['cleaned']:,} von {stats['raw']:,} Ladesäulen geladen. "
                    f"({removed_count:,} Einträge wegen fehlender Daten entfernt).",
                    type='positive', multi_line=True, close_button=True
                )
            # Update station index last_seen for current dataset
            try:
                dataset_name = os.path.basename(new_csv)
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

    async def on_map_moveend(e: Any):
        await update_view()

    async def on_map_zoomend(e: Any):
        await update_view()

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
                            ui.navigate.to(f"/station/{target_id}")
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

            async def set_parkraumgesellschaft_bw():
                nonlocal df
                if df is None:
                    return
                operator_series = df[operator_col].astype(str)
                target_operator = 'Parkraumgesellschaft Baden-Württemberg GmbH'
                candidates = sorted({
                    op for op in operator_series.unique()
                    if op == target_operator
                })
                if not candidates:
                    ui.notify(f'Betreiber "{target_operator}" im Datensatz nicht gefunden.', type='warning')
                    return
                app.storage.user['selected_operators'] = candidates
                operator_select.value = candidates
                operator_select.update()
                await update_view()

            with ui.row().classes('w-full mt-2 gap-2'):
                ui.button('Karte aktualisieren', on_click=update_view).classes('grow')
                ui.button('Ein ❤️ für Betrug', on_click=set_parkraumgesellschaft_bw).props('color=negative').classes('grow')

            with ui.column().classes('w-full items-center mt-4') as progress_container:
                progress_bar = ui.linear_progress(value=0).props('instant-feedback').classes('w-full')
                progress_label = ui.label("").classes('text-sm text-gray-500')

            with ui.row().classes('w-full items-center gap-2 mt-2 text-sm text-gray-500') as view_update_spinner:
                ui.spinner(size='sm')
                ui.label('Kartenausschnitt wird aktualisiert...')
                view_update_spinner.set_visibility(False)
            
            def update_progress():
                with download_state.lock:
                    is_running = download_state.is_running
                    progress_container.set_visibility(is_running)
                    if is_running:
                        progress_bar.value = download_state.progress
                        progress_label.text = f"Download: {download_state.downloaded_mb:.2f} / {download_state.total_mb:.2f} MB"
            
            ui.timer(0.1, update_progress, active=True)

        with ui.column().classes('h-full p-0 m-0 grow'):
            initial_center, initial_zoom = get_initial_map_view()
            m = ui.leaflet(center=initial_center, zoom=initial_zoom, additional_resources=[
                'https://unpkg.com/leaflet-rotatedmarker@0.2.0/leaflet.rotatedMarker.js',
            ]).classes('h-full')
            m.on('map-moveend', on_map_moveend, throttle=0.5)
            m.on('map-zoomend', on_map_zoomend, throttle=0.5)
            
            toggle_button = ui.button(icon='menu_open', on_click=toggle_panel) \
                .props('fab-mini flat color=grey-8').classes('absolute top-2 left-2 z-10')

    try:
        await ui.context.client.connected(timeout=10.0)
    except TimeoutError:
        logging.info('No client connection established within timeout; continuing without blocking initial load.')
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
    try:
        await m.initialized(timeout=5.0)
    except TimeoutError:
        logging.info('Map did not initialize in time; attempting update anyway.')
    await update_view()

    # no dialog open on load anymore

from app.auth import load_storage_secret as _load_storage_secret
def load_storage_secret(filepath: str = ".secret") -> str:
    return _load_storage_secret(filepath)

ui.run(
    storage_secret=load_storage_secret(),
    port=8484,
)
