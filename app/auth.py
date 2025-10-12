from __future__ import annotations
from typing import Dict, Tuple
import os
from nicegui import ui, app
from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware


def parse_secret_file(filepath: str = ".secret") -> Dict[str, str]:
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        if not content:
            return {}
        if '=' not in content and '\n' not in content:
            return {'STORAGE_SECRET': content}
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


def load_storage_secret(filepath: str = ".secret") -> str:
    try:
        values = parse_secret_file(filepath)
        secret = values.get('STORAGE_SECRET') or values.get('storage_secret')
        if not secret:
            return "temp_secret_please_run_setup"
        return secret
    except FileNotFoundError:
        return "temp_secret_please_run_setup"


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
    unrestricted = {'/login', '/'}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path.startswith('/_nicegui'):
            return await call_next(request)
        if path in self.unrestricted or path.startswith('/station/') or path.startswith('/station-files/'):
            return await call_next(request)
        if path.startswith('/api/edit/') and not app.storage.user.get('authenticated', False):
            return RedirectResponse(f"/login?redirect_to={path}")
        return await call_next(request)

