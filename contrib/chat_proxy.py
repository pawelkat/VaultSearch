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
import sys
from calendar import month_name
from datetime import datetime, timedelta
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
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
ZONE = ZoneInfo("Europe/Zurich")

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


def system_text(note: str, now: datetime) -> str:
    body = note.strip()
    return f"{body}\n\n{date_block(now)}" if body else date_block(now)


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
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

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
        self.send_response(resp.status)
        for key, value in resp.getheaders():
            if key.lower() not in HOP:
                self.send_header(key, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD":
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
