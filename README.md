# AI Usage Monitor

Floating PySide6 usage monitor for Claude Code, Codex, and Gemini CLI.

## Features

- Claude Code OAuth usage bars.
- Codex rate limit status via `codex app-server`.
- Gemini Code Assist quota via local Gemini CLI credentials.
- Frameless always-on-top window with corner alignment menu.

## Run

```powershell
pip install -r requirements.txt
python main.py
```

## Build

```powershell
py -m PyInstaller --onefile --windowed --name AIUsageMonitor2 --distpath dist --workpath build --specpath . main.py
```

## Credentials

The app reads existing local credentials from:

- `%USERPROFILE%\.claude\.credentials.json`
- `%USERPROFILE%\.codex\auth.json`
- `%USERPROFILE%\.gemini\oauth_creds.json`

Credential files and generated executables are not tracked by git.
