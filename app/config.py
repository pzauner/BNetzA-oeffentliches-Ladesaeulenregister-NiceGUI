import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# SCRIPT_DIR here points to .../app; project root is one directory up
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DOWNLOAD_DIR = os.path.join(PROJECT_ROOT, 'register-downloads')
CONTEXT_DIR = os.path.join(PROJECT_ROOT, 'station-context')
BNETZA_PAGE_URL = 'https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html'
MAX_MARKERS_IN_VIEW = 2500
KARLSRUHE_COORDS = (49.0069, 8.4037)
STATION_PAGE_ROUTE = '/station/{station_id}'
INDEX_DIR = os.path.join(CONTEXT_DIR, 'index')
STATION_INDEX_PATH = os.path.join(INDEX_DIR, 'station-index.json')

