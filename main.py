"""
AI Usage Monitor — Claude Code + Codex
PySide6 rewrite — UI faithfully matches the original Tkinter version.
"""

import base64
import json
import os
import queue
import re
import shutil
import ssl
import subprocess
import sys
import threading
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

from PySide6.QtCore import Qt, QTimer, Signal, QObject, QPoint, QRect
from PySide6.QtGui import (
    QColor, QFont, QIcon, QPainter, QPixmap,
    QBrush, QPainterPath, QPen,
)
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QFrame, QMenu,
)

# ─── Config ──────────────────────────────────────────────

REFRESH_MS = 5 * 60 * 1000
TZ_OFFSET  = timezone(timedelta(hours=9))
TZ_NAME    = "Asia/Tokyo"

CLAUDE_CREDS = Path.home() / ".claude" / ".credentials.json"
CODEX_AUTH   = Path.home() / ".codex" / "auth.json"
GEMINI_AUTH     = Path.home() / ".gemini" / "oauth_creds.json"
GEMINI_ACCTS    = Path.home() / ".gemini" / "google_accounts.json"
GEMINI_CLI_BUNDLE = Path.home() / "AppData" / "Roaming" / "npm" / "node_modules" / "@google" / "gemini-cli" / "bundle"
# ─── Catppuccin Mocha ────────────────────────────────────

C = {
    "base":     "#1E1E2E",
    "mantle":   "#181825",
    "surface0": "#313244",
    "surface1": "#45475A",
    "text":     "#CDD6F4",
    "subtext":  "#A6ADC8",
    "green":    "#A6E3A1",
    "yellow":   "#F9E2AF",
    "red":      "#F38BA8",
    "blue":     "#89B4FA",
    "lavender": "#B4BEFE",
    "mauve":    "#CBA6F7",
    "teal":     "#94E2D5",
}

# ─── Rate-limit cooldown + cache ─────────────────────────

_cooldown: dict = {}
_cache: dict    = {}


def _in_cooldown(key):
    return datetime.now().timestamp() < _cooldown.get(key, 0)


def _set_cooldown(key, seconds):
    _cooldown[key] = datetime.now().timestamp() + seconds


# ─── HTTP ────────────────────────────────────────────────

def http_get(url, headers, key):
    if _in_cooldown(key):
        wait = int(_cooldown[key] - datetime.now().timestamp())
        if key in _cache:
            return {**_cache[key], "_cached": True, "_wait": wait}
        return {"_error": "rate_limited", "_wait": wait}
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=10, context=ctx)
        data = json.loads(resp.read().decode())
        _cache[key] = data
        _cooldown.pop(key, None)
        return data
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        if e.code == 429:
            _set_cooldown(key, int(e.headers.get("retry-after", 3600)))
        return {"_error": str(e.code), "_body": body[:200], "_http": e.code}
    except Exception as e:
        return {"_error": str(e)}


# ─── Time helpers ────────────────────────────────────────

def parse_reset(resets_at):
    if not resets_at:
        return None
    if isinstance(resets_at, str):
        try:
            return datetime.fromisoformat(resets_at.replace("Z", "+00:00")).astimezone(TZ_OFFSET)
        except Exception:
            return None
    return datetime.fromtimestamp(float(resets_at), tz=TZ_OFFSET)


def fmt_reset_time(resets_at):
    dt = parse_reset(resets_at)
    if not dt:
        return ""
    now = datetime.now(tz=TZ_OFFSET)
    h = dt.strftime("%I").lstrip("0") or "0"
    t = f"{h}:{dt.strftime('%M%p').lower()}"
    if dt.date() == now.date():
        return f"{t} ({TZ_NAME})"
    if dt.date() == (now + timedelta(days=1)).date():
        return f"Tomorrow {t}"
    return f"{dt.strftime('%b %d')} {t}"


def time_until(resets_at):
    dt = parse_reset(resets_at)
    if not dt:
        return ""
    secs = int((dt - datetime.now(tz=TZ_OFFSET)).total_seconds())
    if secs <= 0:
        return "now"
    d, r = divmod(secs, 86400)
    h, r = divmod(r, 3600)
    m = r // 60
    if d: return f"{d}d {h}h"
    if h: return f"{h}h {m}m"
    return f"{m}m"


def decode_jwt_payload(token):
    try:
        part = token.split(".")[1]
        part += "=" * (4 - len(part) % 4)
        return json.loads(base64.b64decode(part))
    except Exception:
        return {}


# ─── Claude fetchers ─────────────────────────────────────

def load_claude_token():
    try:
        d = json.loads(CLAUDE_CREDS.read_text(encoding="utf-8"))
        oauth = d.get("claudeAiOauth", {})
        token = oauth.get("accessToken")
        expires = oauth.get("expiresAt", 0)
        if expires and int(datetime.now().timestamp() * 1000) + 300_000 >= expires:
            return None
        return token
    except Exception:
        return None


def fetch_claude_profile():
    token = load_claude_token()
    if not token:
        return None
    return http_get(
        "https://api.anthropic.com/api/oauth/profile",
        {"Authorization": f"Bearer {token}", "anthropic-beta": "oauth-2025-04-20",
         "User-Agent": "claude-code/2.1.91"},
        "claude_profile",
    )


def fetch_claude_usage():
    token = load_claude_token()
    if not token:
        return None
    return http_get(
        "https://api.anthropic.com/api/oauth/usage",
        {"Authorization": f"Bearer {token}", "anthropic-beta": "oauth-2025-04-20",
         "Content-Type": "application/json", "User-Agent": "claude-code/2.1.91",
         "anthropic-version": "2023-06-01"},
        "claude_usage",
    )


# ─── Codex fetchers ──────────────────────────────────────

def load_codex_token():
    try:
        d = json.loads(CODEX_AUTH.read_text(encoding="utf-8"))
        return d.get("tokens", {}).get("access_token")
    except Exception:
        return None


def fetch_codex_profile():
    try:
        d = json.loads(CODEX_AUTH.read_text(encoding="utf-8"))
        payload = decode_jwt_payload(d.get("tokens", {}).get("id_token", ""))
        return {"email": payload.get("email", ""), "name": payload.get("name", "")}
    except Exception:
        return None


def fetch_codex_usage():
    try:
        result = _codex_rpc_rate_limits()
        if result:
            return result
    except Exception:
        pass
    return _codex_usage_from_jsonl()


def _codex_rpc_rate_limits():
    codex_cmd = _find_codex_cmd()
    if not codex_cmd:
        return None

    proc = subprocess.Popen(
        [codex_cmd, "app-server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    try:
        lines = queue.Queue()

        def _reader():
            for line in proc.stdout:
                lines.put(line)

        threading.Thread(target=_reader, daemon=True).start()

        def send(msg):
            proc.stdin.write(json.dumps(msg) + "\n")
            proc.stdin.flush()

        def recv(timeout=8):
            try:
                return lines.get(timeout=timeout)
            except queue.Empty:
                return None

        send({"id": 1, "method": "initialize",
              "params": {"clientInfo": {"name": "ai-usage-monitor", "version": "1.0"},
                         "capabilities": {}}})
        send({"method": "initialized"})

        for _ in range(15):
            raw = recv(5)
            if not raw:
                break
            try:
                msg = json.loads(raw)
                if msg.get("id") == 1:
                    break
            except Exception:
                pass

        send({"id": 2, "method": "account/rateLimits/read"})

        for _ in range(20):
            raw = recv(5)
            if not raw:
                break
            try:
                msg = json.loads(raw)
                if msg.get("id") == 2 and "result" in msg:
                    rl = (msg["result"] or {}).get("rateLimits") or {}
                    primary = rl.get("primary")
                    secondary = rl.get("secondary")
                    if primary:
                        def conv(w):
                            return {
                                "used_percent": w.get("usedPercent", 0),
                                "window_minutes": w.get("windowDurationMins"),
                                "resets_at": w.get("resetsAt"),
                            }
                        return {
                            "rate_limits": {
                                "primary": conv(primary) if primary else None,
                                "secondary": conv(secondary) if secondary else None,
                                "plan_type": rl.get("planType", ""),
                            },
                            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                            "source": "rpc",
                        }
            except Exception:
                pass
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            pass
    return None


def _find_codex_cmd():
    if os.name == "nt":
        for name in ("codex.cmd", "codex.exe", "codex"):
            found = shutil.which(name)
            if found:
                return found
        npm_cmd = Path.home() / "AppData" / "Roaming" / "npm" / "codex.cmd"
        if npm_cmd.exists():
            return str(npm_cmd)
    return shutil.which("codex")


def _codex_usage_from_jsonl():
    sessions_dir = Path.home() / ".codex" / "sessions"
    if not sessions_dir.exists():
        return None
    for f in sorted(sessions_dir.rglob("rollout-*.jsonl"), reverse=True):
        try:
            for line in reversed(f.read_bytes().split(b"\n")):
                if b"token_count" not in line or b"rate_limits" not in line:
                    continue
                try:
                    obj = json.loads(line)
                    payload = obj.get("payload", {})
                    if payload.get("type") != "token_count":
                        continue
                    rl = payload.get("rate_limits")
                    if rl and rl.get("primary"):
                        return {"rate_limits": rl, "timestamp": obj.get("timestamp", ""),
                                "source": "jsonl", "source_file": f.name}
                except Exception:
                    continue
        except Exception:
            continue
    return None


# ─── Gemini fetchers ─────────────────────────────────────

def load_gemini_creds():
    try:
        creds = json.loads(GEMINI_AUTH.read_text(encoding="utf-8"))
    except Exception:
        return None
    expiry = creds.get("expiry_date", 0)
    if expiry and datetime.now().timestamp() * 1000 + 300_000 < expiry:
        return creds
    return refresh_gemini_creds(creds)


def refresh_gemini_creds(creds):
    refresh_token = creds.get("refresh_token")
    if not refresh_token:
        return creds
    oauth_client = load_gemini_oauth_client()
    if not oauth_client:
        return creds
    body = urllib.parse.urlencode({
        "client_id": oauth_client["client_id"],
        "client_secret": oauth_client["client_secret"],
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10, context=ssl.create_default_context()) as resp:
            data = json.loads(resp.read().decode())
        creds.update(data)
        creds["refresh_token"] = refresh_token
        if data.get("expires_in"):
            creds["expiry_date"] = int((datetime.now().timestamp() + int(data["expires_in"])) * 1000)
        GEMINI_AUTH.write_text(json.dumps(creds, indent=2), encoding="utf-8")
    except Exception:
        pass
    return creds


def load_gemini_oauth_client():
    """Read Gemini CLI's public desktop OAuth client from the installed bundle."""
    try:
        if not GEMINI_CLI_BUNDLE.exists():
            return None
        for f in GEMINI_CLI_BUNDLE.glob("chunk-*.js"):
            text = f.read_text(encoding="utf-8", errors="ignore")
            if "OAUTH_CLIENT_ID" not in text or "OAUTH_CLIENT_SECRET" not in text:
                continue
            cid = re.search(r'OAUTH_CLIENT_ID\s*=\s*"([^"]+)"', text)
            sec = re.search(r'OAUTH_CLIENT_SECRET\s*=\s*"([^"]+)"', text)
            if cid and sec:
                return {"client_id": cid.group(1), "client_secret": sec.group(1)}
    except Exception:
        return None
    return None


def fetch_gemini_profile():
    try:
        d = load_gemini_creds()
        if not d:
            return None
        payload = decode_jwt_payload(d.get("id_token", ""))
        email = payload.get("email", "")
        name = payload.get("name", "")
        if not email:
            accts = json.loads(GEMINI_ACCTS.read_text(encoding="utf-8"))
            email = accts.get("active", "")
        return {"email": email, "name": name}
    except Exception:
        return None


def fetch_gemini_quota():
    creds = load_gemini_creds()
    if not creds:
        return {"_error": "OAuth credentials not found or refresh failed"}
    token = creds.get("access_token", "")
    if not token:
        return {"_error": "OAuth access token is missing"}

    if _in_cooldown("gemini_quota"):
        wait = int(_cooldown["gemini_quota"] - datetime.now().timestamp())
        if "gemini_quota" in _cache:
            return {**_cache["gemini_quota"], "_cached": True, "_wait": wait}
        return {"_error": f"Rate limited, retry in {wait}s", "_wait": wait}

    try:
        headers = {"Authorization": f"Bearer {token}",
                   "Content-Type": "application/json",
                   "User-Agent": "GeminiCLI/0.39.1"}
        meta = {"ideType": "IDE_UNSPECIFIED", "platform": "PLATFORM_UNSPECIFIED",
                "pluginType": "GEMINI"}
        load = _gemini_post("loadCodeAssist", {"metadata": meta}, headers)
        project = load.get("cloudaicompanionProject", "")
        tier = load.get("paidTier") or load.get("currentTier") or {}
        data = _gemini_post("retrieveUserQuota", {"project": project} if project else {}, headers)
        data["_project"] = project
        data["_tier"] = tier.get("name", "")
        data["_pooled"] = _gemini_pooled_quota(data, tier)
        _cache["gemini_quota"] = data
        _cooldown.pop("gemini_quota", None)
        return data
    except urllib.error.HTTPError as e:
        if e.code == 429:
            _set_cooldown("gemini_quota", int(e.headers.get("retry-after", 3600)))
        return {"_error": f"HTTP {e.code} from Code Assist quota API", "_http": e.code}
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}


def _gemini_post(method, body, headers):
    ctx = ssl.create_default_context()
    req = urllib.request.Request(
        f"https://cloudcode-pa.googleapis.com/v1internal:{method}",
        data=json.dumps(body).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
        return json.loads(resp.read().decode())


def _gemini_pooled_quota(quota, tier):
    buckets = quota.get("buckets", [])
    by_model = {b.get("modelId"): b for b in buckets if b.get("modelId")}
    for pair in [
        ("gemini-3-pro-preview", "gemini-3-flash-preview"),
        ("gemini-3.1-pro-preview", "gemini-3.1-flash-lite-preview"),
        ("gemini-2.5-pro", "gemini-2.5-flash"),
    ]:
        pair_buckets = [by_model.get(m) for m in pair]
        if not all(pair_buckets):
            continue
        if any(b.get("resetTime") == "1970-01-01T00:00:00Z" for b in pair_buckets):
            continue
        if any(b.get("remainingFraction") is None for b in pair_buckets):
            continue
        limit = 100 * len(pair_buckets)
        remaining = sum(round(float(b.get("remainingFraction", 0)) * 100) for b in pair_buckets)
        resets = [b.get("resetTime") for b in pair_buckets if b.get("resetTime")]
        return {
            "remaining": remaining,
            "limit": limit,
            "resetTime": sorted(resets, reverse=True)[0] if resets else None,
            "source": "quota-fraction",
        }

    if tier.get("id") == "g1-pro-tier":
        return {
            "remaining": 200,
            "limit": 200,
            "resetTime": (datetime.now(tz=TZ_OFFSET) + timedelta(hours=24)).isoformat(),
            "source": "tier-fallback",
        }
    return None


# ─── Async fetch bridge ──────────────────────────────────

class Fetcher(QObject):
    done = Signal(dict)

    def __init__(self, services=None):
        super().__init__()
        self.services = set(services or ("claude", "codex", "gemini"))

    def run(self):
        data = {"_services": list(self.services)}
        if "claude" in self.services:
            data["claude_usage"] = fetch_claude_usage()
            data["claude_profile"] = fetch_claude_profile()
        if "codex" in self.services:
            data["codex_profile"] = fetch_codex_profile()
            data["codex_usage"] = fetch_codex_usage()
        if "gemini" in self.services:
            data["gemini_profile"] = fetch_gemini_profile()
            data["gemini_quota"] = fetch_gemini_quota()
        self.done.emit(data)


# ─── Tray icon ───────────────────────────────────────────

def make_tray_icon(bg="#5E5086") -> QIcon:
    s = 64
    px = QPixmap(s, s)
    px.fill(Qt.transparent)
    p = QPainter(px)
    p.setRenderHint(QPainter.Antialiasing)
    path = QPainterPath()
    path.addRoundedRect(2, 2, s - 4, s - 4, 12, 12)
    p.fillPath(path, QColor(bg))
    p.setPen(QColor("white"))
    f = QFont("Arial", 20, QFont.Bold)
    p.setFont(f)
    p.drawText(QRect(0, 0, s, s), Qt.AlignCenter, "AI")
    p.end()
    return QIcon(px)


# ─── Inline progress bar (mimics original tk.Frame fill) ─

class BarWidget(QWidget):
    def __init__(self, pct: float, color: str, parent=None):
        super().__init__(parent)
        self._pct   = min(max(pct, 0.0), 1.0)
        self._color = QColor(color)
        self.setFixedHeight(14)

    def paintEvent(self, _):
        p = QPainter(self)
        w, h = self.width(), self.height()
        # Track (surface1)
        p.fillRect(0, 0, w, h, QColor(C["surface1"]))
        # Fill
        if self._pct > 0:
            p.fillRect(0, 0, int(w * self._pct), h, self._color)
        p.end()


# ─── Toggle button (mimics Tkinter Checkbutton) ──────────

class ToggleBtn(QLabel):
    toggled = Signal(bool)

    def __init__(self, text: str, color: str, checked: bool = True, parent=None):
        super().__init__(parent)
        self._checked = checked
        self._color   = color
        self._text    = text
        self._update()
        self.setCursor(Qt.PointingHandCursor)
        self.setFont(QFont("Consolas", 8))

    def _update(self):
        mark = "☑" if self._checked else "☐"
        self.setText(f" {mark} {self._text}")
        self.setStyleSheet(
            f"color: {self._color if self._checked else C['surface1']};"
            f"background: transparent; padding: 0 4px;"
        )

    def mousePressEvent(self, _):
        self._checked = not self._checked
        self._update()
        self.toggled.emit(self._checked)

    def isChecked(self):
        return self._checked


# ─── Resize grip (one per edge) ──────────────────────────

class EdgeGrip(QWidget):
    def __init__(self, edge: str, win: "MainWindow", parent=None):
        super().__init__(parent)
        self._edge = edge
        self._win  = win
        self._start: dict = {}
        cursors = {
            "l": Qt.SizeHorCursor, "r": Qt.SizeHorCursor,
            "t": Qt.SizeVerCursor, "b": Qt.SizeVerCursor,
            "br": Qt.SizeFDiagCursor,
        }
        self.setCursor(cursors[edge])
        self.setStyleSheet("background: transparent;")

    def mousePressEvent(self, e):
        if e.button() == Qt.LeftButton:
            self._start = dict(
                mx=e.globalPosition().toPoint().x(),
                my=e.globalPosition().toPoint().y(),
                wx=self._win.x(), wy=self._win.y(),
                ww=self._win.width(), wh=self._win.height(),
            )

    def mouseMoveEvent(self, e):
        if not (e.buttons() & Qt.LeftButton) or not self._start:
            return
        s  = self._start
        gp = e.globalPosition().toPoint()
        dx, dy = gp.x() - s["mx"], gp.y() - s["my"]
        x, y, w, h = s["wx"], s["wy"], s["ww"], s["wh"]
        mn_w, mn_h = 300, 180
        ed = self._edge
        if ed in ("r",  "br"): w = max(mn_w, s["ww"] + dx)
        if ed in ("b",  "br"): h = max(mn_h, s["wh"] + dy)
        if ed == "l":
            nw = max(mn_w, s["ww"] - dx); x = s["wx"] + s["ww"] - nw; w = nw
        if ed == "t":
            nh = max(mn_h, s["wh"] - dy); y = s["wy"] + s["wh"] - nh; h = nh
        self._win.setGeometry(x, y, w, h)


# ─── Main window ─────────────────────────────────────────

FONT_NAME = "JetBrains Mono"
FONT_FB   = "Consolas"


def _font(size=9, bold=False):
    f = QFont(FONT_NAME, size)
    f.setBold(bold)
    return f


def _label(text="", fg=C["text"], size=9, bold=False, parent=None) -> QLabel:
    w = QLabel(text, parent)
    w.setFont(_font(size, bold))
    w.setStyleSheet(f"color: {fg}; background: transparent;")
    return w


class MainWindow(QWidget):
    GRIP = 6

    def __init__(self):
        super().__init__()
        self._drag_pos  = None
        self._pinned    = True
        self._anchor_corner = "br"
        self._layout_signature = None
        self._data: dict = {}
        self._loading_services = set()

        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setWindowTitle("AI Usage Monitor")
        self.setWindowIcon(make_tray_icon())
        self.setAttribute(Qt.WA_NoSystemBackground)
        self.setWindowOpacity(0.95)

        screen = QApplication.primaryScreen().availableGeometry()
        self.resize(440, 280)
        self.move(screen.right() - 456, screen.bottom() - 320)

        self._build_ui()

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(REFRESH_MS)
        self._refresh()

    # ── UI skeleton ──────────────────────────────────────

    def _build_ui(self):
        self.setStyleSheet(f"background: {C['base']};")
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Title bar
        self._titlebar = QWidget()
        self._titlebar.setFixedHeight(26)
        self._titlebar.setStyleSheet(f"background: {C['mantle']};")
        tb_lay = QHBoxLayout(self._titlebar)
        tb_lay.setContentsMargins(6, 0, 4, 0)
        tb_lay.setSpacing(0)

        title = _label("  AI Usage Monitor", C["lavender"], 9, bold=True)
        tb_lay.addWidget(title, stretch=1)

        # Buttons: P, position menu, X.
        self._pin_btn  = self._tbtn(" P ", C["yellow"], self._toggle_pin)
        self._pos_btn  = self._tbtn(" ▾ ", C["yellow"], self._show_position_menu, bold=True)
        self._cls_btn  = self._tbtn(" X ", C["red"],    self.quit)
        for b in [self._pin_btn, self._pos_btn, self._cls_btn]:
            tb_lay.addWidget(b)

        for w in [self._titlebar, title]:
            w.mousePressEvent = self._drag_start
            w.mouseMoveEvent  = self._drag_move

        root.addWidget(self._titlebar)

        # Toggle bar
        tog = QWidget()
        tog.setFixedHeight(24)
        tog.setStyleSheet(f"background: {C['surface0']};")
        tog_lay = QHBoxLayout(tog)
        tog_lay.setContentsMargins(6, 0, 6, 0)
        tog_lay.setSpacing(0)
        tog_lay.addWidget(_label(" Show:", C["subtext"], 8))

        self._cb_claude = ToggleBtn("Claude Code", C["green"])
        self._cb_codex  = ToggleBtn("Codex",       C["teal"])
        self._cb_gemini = ToggleBtn("Gemini",      C["yellow"])
        self._cb_claude.toggled.connect(lambda checked: self._toggle_service("claude", checked))
        self._cb_codex.toggled.connect(lambda checked: self._toggle_service("codex", checked))
        self._cb_gemini.toggled.connect(lambda checked: self._toggle_service("gemini", checked))
        tog_lay.addWidget(self._cb_claude)
        tog_lay.addWidget(self._cb_codex)
        tog_lay.addWidget(self._cb_gemini)
        tog_lay.addStretch()
        root.addWidget(tog)

        # Body — replaced wholesale on each render
        self._root_layout = root
        self._body_widget  = None
        self._body         = None
        self._body_placeholder = QWidget()   # keeps index stable in root layout
        self._body_placeholder.setStyleSheet(f"background: {C['base']};")
        root.addWidget(self._body_placeholder)

        # Status bar
        self._status = _label("", C["subtext"], 7)
        self._status.setContentsMargins(8, 1, 8, 2)
        self._status.setStyleSheet(f"color: {C['subtext']}; background: {C['mantle']};")
        root.addWidget(self._status)

    def _tbtn(self, text: str, fg: str, slot, bold=False) -> QLabel:
        w = QLabel(text)
        w.setFont(_font(9, bold))
        w.setStyleSheet(f"color: {fg}; background: transparent; padding: 0 2px;")
        w.setCursor(Qt.PointingHandCursor)
        w.mousePressEvent = lambda _: slot()
        return w

    # ── Refresh ──────────────────────────────────────────

    def _refresh(self, services=None):
        services = list(services or self._visible_services())
        if not services:
            return
        self._loading_services.update(services)
        self._render_layout_changed()
        self._fetcher = Fetcher(services)
        self._thread  = threading.Thread(target=self._fetcher.run, daemon=True)
        self._fetcher.done.connect(self._on_data)
        self._thread.start()

    def _on_data(self, data: dict):
        services = set(data.pop("_services", []))
        self._loading_services.difference_update(services)
        self._data.update(data)
        self._render()
        now = datetime.now().strftime("%H:%M")
        nxt = (datetime.now() + timedelta(milliseconds=REFRESH_MS)).strftime("%H:%M")
        self._status.setText(f"  Refreshed {now}  |  Next: {nxt}")

    # ── Render ───────────────────────────────────────────

    def _render_layout_changed(self):
        self._layout_signature = None
        self._render()

    def _visible_services(self):
        services = []
        if self._cb_claude.isChecked():
            services.append("claude")
        if self._cb_codex.isChecked():
            services.append("codex")
        if self._cb_gemini.isChecked():
            services.append("gemini")
        return services

    def _toggle_service(self, service, checked):
        self._render_layout_changed()
        if checked and not self._service_has_data(service):
            self._refresh([service])

    def _service_has_data(self, service):
        if service == "claude":
            return bool(self._data.get("claude_usage") or self._data.get("claude_profile"))
        if service == "codex":
            return bool(self._data.get("codex_usage") or self._data.get("codex_profile"))
        if service == "gemini":
            return bool(self._data.get("gemini_quota") or self._data.get("gemini_profile"))
        return False

    def _render(self):
        # Replace body widget wholesale — avoids deleteLater async overlap issues
        old = self._body_widget
        idx = self._root_layout.indexOf(self._body_placeholder if old is None
                                        else old)

        new_widget = QWidget()
        new_widget.setStyleSheet(f"background: {C['base']};")
        new_layout = QVBoxLayout(new_widget)
        new_layout.setContentsMargins(12, 4, 12, 6)
        new_layout.setSpacing(2)

        self._body_widget = new_widget
        self._body        = new_layout

        self._root_layout.insertWidget(idx, new_widget)
        if old is not None:
            old.hide()
            old.deleteLater()
        else:
            self._body_placeholder.hide()

        shown = []
        if self._cb_claude.isChecked():
            shown.append(self._render_claude)
        if self._cb_codex.isChecked():
            shown.append(self._render_codex)
        if self._cb_gemini.isChecked():
            shown.append(self._render_gemini)

        for i, render in enumerate(shown):
            if i > 0:
                sep = QFrame()
                sep.setFrameShape(QFrame.HLine)
                sep.setStyleSheet(f"background: {C['surface0']};")
                sep.setFixedHeight(1)
                sep.setContentsMargins(0, 4, 0, 4)
                self._body.addWidget(sep)
            render()

        QTimer.singleShot(0, self._auto_size)

    def _render_claude(self):
        profile = self._data.get("claude_profile")
        data    = self._data.get("claude_usage")

        acct = ""
        if profile and "_error" not in profile:
            a    = profile.get("account", {})
            name = a.get("display_name") or a.get("full_name") or ""
            email = a.get("email_address") or a.get("email") or ""
            plan = profile.get("organization", {}).get("name", "")
            acct = f"{name} - {email}" if name and email else name or email
            if plan:
                acct += f" [{plan}]"

        self._add_header("Claude Code", C["green"], acct, "claude")

        if data is None:
            self._body.addWidget(_label("No credentials (OAuth not configured)", C["red"], 8))
            return

        is_cached = data.get("_cached", False)
        if "_error" in data and not is_cached:
            if data.get("_http") == 429:
                wait   = data.get("_wait", 0)
                resume = (datetime.now() + timedelta(seconds=wait)).strftime("%H:%M")
                self._body.addWidget(_label(f"Rate limited  —  available ~{resume}", C["yellow"], 8))
            else:
                self._body.addWidget(_label(f"Error: {data['_error']}", C["red"], 8))
            return

        if is_cached:
            self._body.addWidget(_label("(showing cached data)", C["subtext"], 7))

        for key, label, color in [
            ("five_hour",        "Current session",           C["green"]),
            ("seven_day",        "Current week (all models)", C["blue"]),
            ("seven_day_sonnet", "Current week (Sonnet)",     C["mauve"]),
        ]:
            info = data.get(key)
            if not info or info.get("utilization") is None:
                continue
            self._add_bar_row(label, info["utilization"] / 100.0, info.get("resets_at"), color)

    def _render_codex(self):
        profile = self._data.get("codex_profile")
        usage   = self._data.get("codex_usage")

        acct = ""
        if profile:
            name = profile.get("name", "")
            email = profile.get("email", "")
            acct = f"{name} - {email}" if name and email else name or email
        if usage:
            plan = usage.get("rate_limits", {}).get("plan_type", "")
            if plan and plan not in acct:
                acct += f" [{plan}]"

        self._add_header("Codex", C["teal"], acct, "codex")

        if not usage:
            self._body.addWidget(_label("No recent session data", C["subtext"], 8))
            return

        ts = usage.get("timestamp", "")
        if ts and usage.get("source") == "jsonl":
            try:
                t   = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(TZ_OFFSET)
                h   = t.strftime("%I").lstrip("0") or "0"
                age = f"From last session ({h}:{t.strftime('%M%p').lower()})"
            except Exception:
                age = "From last session"
            self._body.addWidget(_label(age, C["subtext"], 7))

        rl = usage.get("rate_limits", {})
        for key, tpl, color in [
            ("primary",   "Current session ({u}h window)", C["teal"]),
            ("secondary", "Current week ({u}d window)",    C["blue"]),
        ]:
            info = rl.get(key)
            if not info:
                continue
            mins = info.get("window_minutes", 300 if key == "primary" else 10080)
            unit = mins // 60 if key == "primary" else mins // 1440
            self._add_bar_row(tpl.format(u=unit),
                              info.get("used_percent", 0) / 100.0,
                              info.get("resets_at"), color)

    def _render_gemini(self):
        profile = self._data.get("gemini_profile")
        quota = self._data.get("gemini_quota")

        acct = ""
        if profile:
            name = profile.get("name", "")
            email = profile.get("email", "")
            acct = f"{name} - {email}" if name and email else name or email
        if quota:
            tier = quota.get("_tier", "")
            if tier and tier not in acct:
                acct += f" [{tier}]"

        self._add_header("Gemini CLI", C["yellow"], acct, "gemini")

        if not quota:
            self._body.addWidget(_label("No quota data", C["subtext"], 8))
            return
        if "_error" in quota:
            self._body.addWidget(_label(f"Error: {quota['_error']}", C["red"], 8))
            return

        pooled = quota.get("_pooled")
        if pooled and pooled.get("limit"):
            remaining = pooled.get("remaining", 0)
            limit = pooled.get("limit", 0)
            used_pct = 1.0 - (remaining / limit if limit else 0)
            self._add_bar_row(f"Auto (Gemini 3) daily limit ({limit})",
                              used_pct, pooled.get("resetTime"), C["yellow"])

        if not pooled:
            self._body.addWidget(_label("No model quota data", C["subtext"], 8))

    # ── Widget builders ──────────────────────────────────

    def _add_header(self, title: str, color: str, subtitle: str = "", service: str | None = None):
        row = QHBoxLayout()
        row.setSpacing(6)
        row.setContentsMargins(0, 4, 0, 2)
        row.addWidget(_label(title, color, 10, bold=True))
        if subtitle:
            row.addWidget(_label(subtitle, C["subtext"], 7))
        row.addStretch()
        if service:
            text = "..." if service in self._loading_services else "⟳"
            btn = self._tbtn(f" {text} ", C["subtext"], lambda s=service: self._refresh([s]), bold=True)
            btn.setToolTip(f"Refresh {title}")
            row.addWidget(btn)
        self._body.addLayout(row)

    def _add_bar_row(self, label: str, pct: float, resets_at, color: str):
        bar_color = (color if pct < 0.5 else C["yellow"] if pct < 0.8 else C["red"])

        bar_lbl = _label(label, C["text"], 8, bold=True)
        bar_lbl.setContentsMargins(0, 4, 0, 1)
        self._body.addWidget(bar_lbl)

        row = QHBoxLayout()
        row.setSpacing(0)
        row.setContentsMargins(0, 0, 0, 0)
        bar = BarWidget(pct, bar_color)
        row.addWidget(bar, stretch=1)
        pct_lbl = _label(f"  {int(pct * 100)}% used", C["text"], 8)
        pct_lbl.setFixedWidth(90)
        row.addWidget(pct_lbl)
        self._body.addLayout(row)

        if resets_at:
            reset_lbl = _label(f"Resets {fmt_reset_time(resets_at)}", C["subtext"], 7)
            reset_lbl.setContentsMargins(0, 1, 0, 2)
            self._body.addWidget(reset_lbl)

    # ── Auto-size ────────────────────────────────────────

    def _auto_size(self, force=False):
        sig = self._current_layout_signature()
        if not force and sig == self._layout_signature:
            return
        self._layout_signature = sig

        screen = QApplication.screenAt(self.frameGeometry().center()) or QApplication.primaryScreen()
        area = screen.availableGeometry()
        hint = self.sizeHint()
        new_w = min(max(hint.width(), 440), max(area.width() - 16, 440))
        new_h = min(max(hint.height(), 120), max(area.height() - 16, 120))
        if self._anchor_corner:
            x, y = self._corner_xy(self._anchor_corner, new_w, new_h, area)
        else:
            x = min(max(self.x(), area.left() + 4), area.right() - new_w - 4)
            y = min(max(self.y(), area.top() + 4), area.bottom() - new_h - 4)
        self.setGeometry(x, y, new_w, new_h)

    def _current_layout_signature(self):
        return (
            self._cb_claude.isChecked(), bool(self._data.get("claude_usage")), bool(self._data.get("claude_profile")),
            self._cb_codex.isChecked(), bool(self._data.get("codex_usage")), bool(self._data.get("codex_profile")),
            self._cb_gemini.isChecked(), bool(self._data.get("gemini_quota")), bool(self._data.get("gemini_profile")),
        )

    def _corner_xy(self, corner, w, h, area):
        margin = 8
        x = area.left() + margin if "l" in corner else area.right() - w - margin
        y = area.top() + margin if "t" in corner else area.bottom() - h - margin
        return x, y

    # ── Window controls ──────────────────────────────────

    def _toggle_pin(self):
        self._pinned = not self._pinned
        flags = self.windowFlags()
        self.setWindowFlags(
            flags | Qt.WindowStaysOnTopHint if self._pinned
            else flags & ~Qt.WindowStaysOnTopHint
        )
        self.show()
        self._pin_btn.setStyleSheet(
            f"color: {C['yellow'] if self._pinned else C['surface1']};"
            f"background: transparent; padding: 0 2px;"
        )

    def _show_position_menu(self):
        menu = QMenu(self)
        menu.setStyleSheet(
            f"QMenu {{ background: {C['surface0']}; color: {C['text']}; }}"
            f"QMenu::item:selected {{ background: {C['surface1']}; }}"
        )
        for label, corner in [("Top Left", "tl"), ("Top Right", "tr"), ("Bottom Left", "bl"), ("Bottom Right", "br")]:
            menu.addAction(label, lambda c=corner: self._align_window(c))
        menu.exec(self._pos_btn.mapToGlobal(self._pos_btn.rect().bottomLeft()))

    def _align_window(self, corner):
        self._anchor_corner = corner
        self._auto_size(force=True)

    def quit(self):
        QApplication.quit()

    # ── Drag ─────────────────────────────────────────────

    def _drag_start(self, event):
        self._anchor_corner = None
        if hasattr(event, 'button') and event.button() == Qt.LeftButton:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
        elif not hasattr(event, 'button'):
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def _drag_move(self, event):
        if (event.buttons() & Qt.LeftButton) and self._drag_pos:
            self.move(event.globalPosition().toPoint() - self._drag_pos)

    def mousePressEvent(self, e):
        self._drag_start(e)

    def mouseMoveEvent(self, e):
        self._drag_move(e)


# ─── Entry point ─────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(True)

    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
