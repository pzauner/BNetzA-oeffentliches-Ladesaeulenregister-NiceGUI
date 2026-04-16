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

Dieses Projekt verwendet [`uv`](https://github.com/astral-sh/uv) für Abhängigkeiten und Start der App.

### Lokal starten (empfohlen)

1. **`uv` installieren**
   Folgen Sie der [offiziellen Installationsanleitung](https://github.com/astral-sh/uv#installation).

2. **Projekt klonen**
   ```bash
   git clone https://github.com/umgefahren/BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI.git
   cd BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI
   ```

3. **Setup ausführen**
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```
   Das Skript:
   - installiert Abhängigkeiten via `uv`,
   - erstellt/aktualisiert `.secret`,
   - erzeugt (falls nicht vorhanden) sichere Default-Werte für:
     - `STORAGE_SECRET`
     - `AUTH_USERNAME`
     - `AUTH_PASSWORD`

4. **App starten**
   ```bash
   uv run app.py
   ```

Die App läuft dann unter [http://127.0.0.1:8484](http://127.0.0.1:8484).

Hinweise:
- Beim ersten Start werden die Registerdaten automatisch geladen (mehrere 10 MB).
- Login erfolgt über `/login` mit den Werten aus `.secret`.

### `.secret` Format

Die Datei `.secret` liegt im Projektroot und nutzt `key=value`:

```env
STORAGE_SECRET=...
AUTH_USERNAME=admin
AUTH_PASSWORD=...
```

### Als systemd-Service betreiben (Server)

Beispiel für einen dauerhaften Dienst:

`/etc/systemd/system/lsr.service`

```ini
[Unit]
Description=LSR NiceGUI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
ExecStart=/bin/bash -lc 'cd ~/BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI && exec ~/.local/bin/uv run app.py'
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Aktivieren:

```bash
systemctl daemon-reload
systemctl enable --now lsr.service
systemctl status lsr.service
journalctl -u lsr.service -f
```

## Wie es funktioniert

Die Anwendung lädt die CSV-Datei des Ladesäulenregisters in einen `pandas`-DataFrame. Die [Leaflet.js](https://leafletjs.com/)-Integration von NiceGUI wird verwendet, um eine interaktive Karte darzustellen.

Wenn der Benutzer die Karte verschiebt (`dragend`) oder zoomt (`zoomend`), werden die Grenzen des sichtbaren Bereichs erfasst. Die Daten im DataFrame werden dann gefiltert, um nur die Ladestationen anzuzeigen, die sich innerhalb dieser Grenzen befinden. Um eine Überlastung des Browsers zu vermeiden, ist die Anzahl der gleichzeitig angezeigten Marker auf `2500` begrenzt.

Die Filter für Betreiber und Ladeleistung werden ebenfalls dynamisch auf Basis der im sichtbaren Bereich verfügbaren Daten aktualisiert.

## Designentscheidungen: Upload-Verhalten (Mobile)

Auf der Stationsseite verwenden wir einen direkten Upload-Handler, der die vom NiceGUI-Upload-Event bereitgestellten Bytes (`e.content.read()`) unmittelbar in die jeweilige Stationsablage (`station-context/<ID>/`) schreibt. Dieser Ansatz vermeidet zusätzliche Bestätigungsschritte und funktioniert auf mobilen Browsern angenehmer.

Hinweis:
- Der Upload löst nach erfolgreichem Speichern eine UI-Aktualisierung der Dateiliste aus.
- Lösch- und Umbenenn-Aktionen sind weiterhin ausdrücklich dialogbestätigt, um versehentliches Ändern zu vermeiden.

## Lizenz

Dieses Projekt steht unter der [MIT-Lizenz](LICENSE).
