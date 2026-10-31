#!/usr/bin/env python3
"""Loopback proxy that injects the Nitro5 Qwen system prompt.

Reads a vault note, appends the current Europe/Zurich date, and forwards
/v1/chat/completions to llama-server. Every other request is passed through.

    VAULT_NOTE="$HOME/Obsidian/Vault/System/Nitro5 Qwen.md" \
    python3 contrib/chat_proxy.py

    python3 contrib/chat_proxy.py --self-test
"""

from __future__ import annotations

import json
import os
import re
import sys
from calendar import month_name
from datetime import datetime, timedelta
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

HOST = os.environ.get("BIND", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8082"))
UPSTREAM = os.environ.get("UPSTREAM", "http://127.0.0.1:8080")
NOTE = Path(
    os.environ.get(
        "VAULT_NOTE",
        str(Path.home() / "Obsidian/Vault/System/Nitro5 Qwen.md"),
    )
)
VAULT_ROOT = Path(
    os.environ.get("VAULT_ROOT", str(Path.home() / "Obsidian/Vault"))
).resolve()
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "https://nitro5.tailf52603.ts.net").rstrip("/")
VAULT_URL_PREFIX = "/vault/"
ZONE = ZoneInfo("Europe/Zurich")
PATH_IN_TEXT = re.compile(
    r"(?<![\w./])((?:Notes|Daily|Inbox|System|Archive)/[^\s\]\)\"'<>]+?\.md)"
)

CHAT_PATHS = {
    "/v1/chat/completions",
    "/chat/completions",
}
HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "host",
}

_cached_mtime: float | None = None
_cached_text: str = ""


def strip_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :].lstrip("\n")


def load_note(path: Path = NOTE) -> str:
    global _cached_mtime, _cached_text
    mtime = path.stat().st_mtime
    if _cached_mtime == mtime and _cached_text:
        return _cached_text
    _cached_text = strip_frontmatter(path.read_text(encoding="utf-8"))
    _cached_mtime = mtime
    return _cached_text


def date_block(now: datetime) -> str:
    today = now.date()
    yesterday = today - timedelta(days=1)
    week_start = today - timedelta(days=7)
    if today.month == 1:
        month_year, month = today.year - 1, 12
    else:
        month_year, month = today.year, today.month - 1
    return (
        f"Current date: {today.isoformat()} ({today.strftime('%A')}), Europe/Zurich.\n"
        f"Yesterday = {yesterday.isoformat()}.\n"
        f"Last week ≈ {week_start.isoformat()} to {yesterday.isoformat()}.\n"
        f"Last month = {month_name[month]} {month_year}.\n"
        f"Last year = {today.year - 1}.\n"
        'Use these exact dates for "today", "yesterday", "last week", '
        '"last month", "last year". Never invent a date.'
    )


def source_link_block() -> str:
    return (
        "Source links are required whenever you use vault_search.\n"
        f"Prefix each tool `path:` with {PUBLIC_BASE}{VAULT_URL_PREFIX}\n"
        "Example: path Notes/nitro5.md becomes "
        f"[nitro5]({PUBLIC_BASE}{VAULT_URL_PREFIX}Notes/nitro5.md)\n"
        "End the answer with a Sources section, one link per path. "
        "Never invent a path."
    )


def system_text(note: str, now: datetime) -> str:
    parts = [p for p in (note.strip(), date_block(now), source_link_block()) if p]
    return "\n\n".join(parts)


def resolve_vault_file(root: Path, url_path: str) -> Path | None:
    """Map /vault/<rel> to a file inside root. None if unsafe or not a file."""
    raw = unquote(url_path.split("?", 1)[0])
    if not raw.startswith(VAULT_URL_PREFIX):
        return None
    rel = raw[len(VAULT_URL_PREFIX) :]
    if not rel or "\x00" in rel or "\\" in rel or rel.startswith("/"):
        return None
    parts = Path(rel).parts
    if not parts or any(part in ("", ".", "..") for part in parts):
        return None
    root_resolved = root.resolve()
    candidate = (root_resolved / rel).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


def content_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".md", ".txt", ".csv"}:
        return "text/plain; charset=utf-8"
    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        kind = "jpeg" if ext == ".jpg" else ext[1:]
        return f"image/{kind}"
    if ext == ".pdf":
        return "application/pdf"
    return "application/octet-stream"


def ensure_source_links(text: str) -> str:
    """Add a Sources list when the reply names vault paths but no /vault/ link."""
    if VAULT_URL_PREFIX in text:
        return text
    seen: list[str] = []
    for match in PATH_IN_TEXT.findall(text):
        if match not in seen:
            seen.append(match)
    if not seen:
        return text
    lines = ["", "Sources:"]
    for rel in seen:
        title = Path(rel).stem
        lines.append(f"- [{title}]({PUBLIC_BASE}{VAULT_URL_PREFIX}{rel})")
    return text.rstrip() + "\n" + "\n".join(lines) + "\n"


def inject_messages(payload: dict, prompt: str) -> dict:
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return payload
    client_system: list[str] = []
    rest = []
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "system":
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                client_system.append(content.strip())
            continue
        rest.append(message)
    content = prompt if not client_system else prompt + "\n\n" + "\n\n".join(client_system)
    payload = dict(payload)
    payload["messages"] = [{"role": "system", "content": content}, *rest]
    return payload


def rewrite_chat(body: bytes, now: datetime, note: str) -> bytes:
    if not body:
        return body
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body
    if not isinstance(payload, dict) or "messages" not in payload:
        return body
    updated = inject_messages(payload, system_text(note, now))
    return json.dumps(updated).encode("utf-8")


def _upstream_parts() -> tuple[str, int]:
    parts = urlsplit(UPSTREAM)
    return parts.hostname or "127.0.0.1", parts.port or 80


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), fmt % args))

    def do_GET(self) -> None:
        if self._serve_vault(head=False):
            return
        self._proxy()

    def do_HEAD(self) -> None:
        if self._serve_vault(head=True):
            return
        self._proxy()

    def _serve_vault(self, head: bool) -> bool:
        path = unquote(self.path.split("?", 1)[0])
        if path.rstrip("/") == "/vault":
            self.send_error(404, "name a note under /vault/")
            return True
        if not path.startswith(VAULT_URL_PREFIX):
            return False
        file_path = resolve_vault_file(VAULT_ROOT, path)
        if file_path is None:
            self.send_error(404, "not in the vault")
            return True
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type(file_path))
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        if not head:
            self.wfile.write(data)
        return True

    def do_POST(self) -> None:
        self._proxy()

    def do_OPTIONS(self) -> None:
        self._proxy()

    def _proxy(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if self.command == "POST" and path in CHAT_PATHS:
            try:
                body = rewrite_chat(body, datetime.now(ZONE), load_note())
            except OSError as exc:
                self.send_error(502, f"system prompt unreadable: {exc}")
                return
        host, port = _upstream_parts()
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP
        }
        if body:
            headers["Content-Length"] = str(len(body))
        conn = HTTPConnection(host, port, timeout=600)
        try:
            conn.request(self.command, self.path, body=body or None, headers=headers)
            resp = conn.getresponse()
        except OSError as exc:
            self.send_error(502, f"upstream unavailable: {exc}")
            conn.close()
            return
        rewrite = (
            self.command == "POST"
            and path in CHAT_PATHS
            and b'"stream": true' not in body
            and b'"stream":true' not in body
        )
        raw = None
        if rewrite:
            raw = resp.read()
            try:
                payload = json.loads(raw)
                message = payload["choices"][0]["message"]
                if isinstance(message.get("content"), str):
                    message["content"] = ensure_source_links(message["content"])
                    raw = json.dumps(payload).encode("utf-8")
            except (json.JSONDecodeError, KeyError, IndexError, TypeError):
                pass
        self.send_response(resp.status)
        for key, value in resp.getheaders():
            if key.lower() not in HOP:
                self.send_header(key, value)
        if raw is not None:
            self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD" or raw is not None:
            if raw is not None and self.command != "HEAD":
                self.wfile.write(raw)
            resp.close()
            conn.close()
            return
        while True:
            chunk = resp.read(16384)
            if not chunk:
                break
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                break
        resp.close()
        conn.close()


def self_test() -> None:
    fixed = datetime(2026, 10, 31, 15, 0, tzinfo=ZONE)
    block = date_block(fixed)
    assert "Current date: 2026-10-31 (Saturday)" in block
    assert "Yesterday = 2026-10-30." in block
    assert "Last week ≈ 2026-10-24 to 2026-10-30." in block
    assert "Last month = September 2026." in block
    assert "Last year = 2025." in block
    january = date_block(datetime(2026, 1, 2, tzinfo=ZONE))
    assert "Last month = December 2025." in january
    note = strip_frontmatter("---\ntitle: x\n---\n\n# Hello\n\nRules.\n")
    assert note.startswith("# Hello")
    rewritten = json.loads(
        rewrite_chat(
            json.dumps(
                {
                    "messages": [
                        {"role": "system", "content": "be brief"},
                        {"role": "user", "content": "hi"},
                    ]
                }
            ).encode(),
            fixed,
            note,
        )
    )
    messages = rewritten["messages"]
    assert messages[0]["role"] == "system"
    assert messages[0]["content"].startswith("# Hello")
    assert "2026-10-31" in messages[0]["content"]
    assert "be brief" in messages[0]["content"]
    assert messages[1] == {"role": "user", "content": "hi"}
    assert f"{PUBLIC_BASE}/vault/Notes/nitro5.md" in messages[0]["content"]
    linked = ensure_source_links("See Notes/nitro5.md for the host.")
    assert f"{PUBLIC_BASE}/vault/Notes/nitro5.md" in linked
    assert ensure_source_links(linked) == linked
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        note = root / "Notes"
        note.mkdir()
        (note / "A.md").write_text("hello", encoding="utf-8")
        outside = root.parent / "secret.md"
        found = resolve_vault_file(root, "/vault/Notes/A.md")
        assert found == (note / "A.md").resolve()
        assert resolve_vault_file(root, "/vault/Notes/../../secret.md") is None
        assert resolve_vault_file(root, "/vault/../secret.md") is None
        assert resolve_vault_file(root, "/health") is None
        assert not outside.exists() or resolve_vault_file(root, "/vault/../secret.md") is None
    print("self-test ok")


def main() -> None:
    if "--self-test" in sys.argv:
        self_test()
        return
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"chat proxy on http://{HOST}:{PORT} -> {UPSTREAM} note={NOTE}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
