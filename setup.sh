#!/bin/bash
# Einfaches Setup-Skript für die BNetzA Ladesäulenregister-Anwendung

echo ">> 1. Erstelle virtuelle Umgebung und installiere Abhängigkeiten mit uv..."
uv pip sync pyproject.toml

# Prüfen, ob die app.py existiert
if [ ! -f "app.py" ]; then
    echo "FEHLER: app.py nicht im aktuellen Verzeichnis gefunden."
    exit 1
fi

echo ">> 2. Generiere ein sicheres, zufälliges storage_secret..."

# Generiere einen langen, zufälligen String und speichere ihn in .secret.
# Überschreibe die Datei jedes Mal, um sicherzustellen, dass sie aktuell ist.
head -c 48 /dev/urandom | base64 | tr -d '/+' | head -c 64 > .secret

echo "SUCCESS: .secret wurde erfolgreich erstellt/aktualisiert."


echo ""
echo "Setup abgeschlossen!"
echo "Sie können die Anwendung jetzt starten mit:"
echo "uv run app.py"
