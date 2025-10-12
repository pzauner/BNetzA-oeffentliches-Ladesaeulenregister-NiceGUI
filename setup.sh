#!/bin/bash
# Einfaches Setup-Skript für die BNetzA Ladesäulenregister-Anwendung

echo ">> 1. Erstelle virtuelle Umgebung und installiere Abhängigkeiten mit uv..."
uv pip sync pyproject.toml

# Prüfen, ob die app.py existiert
if [ ! -f "app.py" ]; then
    echo "FEHLER: app.py nicht im aktuellen Verzeichnis gefunden."
    exit 1
fi

echo ">> 2. Generiere Secrets und Credentials in .secret (key=value) ..."

# Wenn .secret existiert, nicht blind überschreiben: vorhandene Keys erhalten
if [ -f .secret ]; then
  # Sicherung erstellen
  cp .secret .secret.bak
fi

STORAGE_SECRET=$(grep '^STORAGE_SECRET=' .secret 2>/dev/null | cut -d'=' -f2-)
AUTH_USERNAME=$(grep '^AUTH_USERNAME=' .secret 2>/dev/null | cut -d'=' -f2-)
AUTH_PASSWORD=$(grep '^AUTH_PASSWORD=' .secret 2>/dev/null | cut -d'=' -f2-)

if [ -z "$STORAGE_SECRET" ]; then
  STORAGE_SECRET=$(head -c 48 /dev/urandom | base64 | tr -d '/+' | head -c 64)
fi
if [ -z "$AUTH_USERNAME" ]; then
  AUTH_USERNAME="admin"
fi
if [ -z "$AUTH_PASSWORD" ]; then
  AUTH_PASSWORD=$(head -c 18 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 14)
fi

{
  echo "STORAGE_SECRET=$STORAGE_SECRET"
  echo "AUTH_USERNAME=$AUTH_USERNAME"
  echo "AUTH_PASSWORD=$AUTH_PASSWORD"
} > .secret

echo "SUCCESS: .secret wurde erstellt/aktualisiert."
echo "Login-Credentials: $AUTH_USERNAME / $AUTH_PASSWORD"


echo ""
echo "Setup abgeschlossen!"
echo "Sie können die Anwendung jetzt starten mit:"
echo "uv run app.py"
