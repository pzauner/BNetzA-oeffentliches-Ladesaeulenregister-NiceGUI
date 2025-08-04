# BNetzA Öffentliches Ladesäulenregister mit NiceGUI

Eine interaktive Kartenvisualisierung des öffentlichen Ladesäulenregisters der Bundesnetzagentur, erstellt mit [NiceGUI](https://nicegui.io/).

![Screenshot](https://raw.githubusercontent.com/umgefahren/BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI/main/screenshot.png)

## Features

*   **Interaktive Karte**: Visualisiert die Ladesäulen auf einer OpenStreetMap-Karte.
*   **Dynamische Filter**: Filtern Sie nach Betreiber und Ladeleistung. Die Filteroptionen passen sich dynamisch an den sichtbaren Kartenausschnitt an.
*   **Suchfunktion**: Suchen Sie direkt nach einer Ladeeinrichtungs-ID, um schnell zu einer bestimmten Säule zu springen.
*   **Automatischer Daten-Download**: Prüft beim Start, ob die Ladesäulendaten vorhanden sind und lädt sie bei Bedarf automatisch von der [BNetzA-Webseite](https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html) herunter.
*   **Manuelle Updates**: Ein Klick auf den Refresh-Button prüft auf neue Datensätze und lädt diese herunter.
*   **Download-Fortschrittsanzeige**: Ein Ladebalken informiert über den Fortschritt beim Herunterladen großer CSV-Dateien.
*   **Datensatz-Auswahl**: Wenn mehrere CSV-Dateien im `register-downloads`-Verzeichnis vorhanden sind, können Sie über ein Dropdown-Menü den zu verwendenden Datensatz auswählen.
*   **Detaillierte Lade-Statistiken**: Zeigt an, wie viele Ladesäulen aus der Rohdatei geladen und wie viele aufgrund fehlender Daten verworfen wurden.
*   **Ein-/ausblendbares Bedienpanel**: Das linke Bedienpanel kann ein- und ausgeblendet werden, um mehr Platz für die Karte zu schaffen.

## Installation & Ausführung

Dieses Projekt verwendet [`uv`](https://github.com/astral-sh/uv) zur Verwaltung der Abhängigkeiten und zur Ausführung der App. `uv` ist ein extrem schneller Python-Paketmanager und -Installer.

1.  **`uv` installieren:**
    Folgen Sie der [offiziellen Installationsanleitung](https://github.com/astral-sh/uv#installation), um `uv` auf Ihrem System zu installieren.

2.  **Projekt klonen:**
    ```bash
    git clone https://github.com/umgefahren/BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI.git
    cd BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI
    ```

3.  **Setup-Skript ausführen (empfohlen):**
    Für eine einfache und sichere Ersteinrichtung können Sie das mitgelieferte Setup-Skript verwenden. Es installiert die Abhängigkeiten und setzt ein sicheres, zufälliges `storage_secret` in der `app.py`.
    ```bash
    ./setup.sh
    ```
    *Hinweis: Möglicherweise müssen Sie das Skript zuerst ausführbar machen mit `chmod +x setup.sh`.*

4.  **Anwendung starten:**
    Führen Sie den folgenden Befehl im Projektverzeichnis aus:
    ```bash
    uv run app.py
    ```
    `uv` erstellt automatisch eine virtuelle Umgebung, installiert die in `pyproject.toml` definierten Abhängigkeiten (falls noch nicht geschehen) und startet die NiceGUI-Anwendung.

    Beim ersten Start werden die Ladesäulendaten (ca. 90 MB) von der Bundesnetzagentur heruntergeladen. Dies kann einen Moment dauern. Ein Fortschrittsbalken zeigt den Status an.

Die Anwendung ist dann unter [http://127.0.0.1:8080](http://127.0.0.1:8080) in Ihrem Browser verfügbar.

## Wie es funktioniert

Die Anwendung lädt die CSV-Datei des Ladesäulenregisters in einen `pandas`-DataFrame. Die [Leaflet.js](https://leafletjs.com/)-Integration von NiceGUI wird verwendet, um eine interaktive Karte darzustellen.

Wenn der Benutzer die Karte verschiebt (`dragend`) oder zoomt (`zoomend`), werden die Grenzen des sichtbaren Bereichs erfasst. Die Daten im DataFrame werden dann gefiltert, um nur die Ladestationen anzuzeigen, die sich innerhalb dieser Grenzen befinden. Um eine Überlastung des Browsers zu vermeiden, ist die Anzahl der gleichzeitig angezeigten Marker auf `2500` begrenzt.

Die Filter für Betreiber und Ladeleistung werden ebenfalls dynamisch auf Basis der im sichtbaren Bereich verfügbaren Daten aktualisiert.

## Lizenz

Dieses Projekt steht unter der [MIT-Lizenz](LICENSE).
