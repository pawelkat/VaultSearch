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
import subprocess
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
        "These dates override anything you remember. "
        "A remembered date such as 2026-10-14 is wrong.\n"
        "Before answering about today, yesterday, last week, last month, "
        "last year, or what was done on a day, call vault_search. "
        f"For yesterday the query is Daily/{yesterday.isoformat()}. "
        f"For today the query is Daily/{today.isoformat()}. "
        "Do not answer that question until the tool result is back. "
        "Use only this date block and that tool result."
    )


def source_link_block() -> str:
    return (
        "Source links are required whenever you use vault_search.\n"
        f"Prefix each tool `path:` with {PUBLIC_BASE}{VAULT_URL_PREFIX}\n"
        "Example: path Daily/2026-10-31.md becomes "
        f"[2026-10-31]({PUBLIC_BASE}{VAULT_URL_PREFIX}Daily/2026-10-31.md)\n"
        "Do not add a Notes/ prefix. The path value is already from the vault root.\n"
        "End the answer with a Sources section, one link per path. "
        "Never invent a path."
    )


def system_text(note: str, now: datetime) -> str:
    # Date block last so the live dates and the tool rule sit next to the question.
    parts = [p for p in (note.strip(), source_link_block(), date_block(now)) if p]
    return "\n\n".join(parts)


def _file_inside(root: Path, rel: str) -> Path | None:
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


def resolve_vault_file(root: Path, url_path: str) -> Path | None:
    """Map /vault/<rel> to a file inside root. None if unsafe or not a file.

    Qwen sometimes prefixes Notes/ onto a path that already starts at the
    vault root (Daily/…, System/…). Try the path as given, then without
    that extra Notes/ segment.
    """
    raw = unquote(url_path.split("?", 1)[0])
    if not raw.startswith(VAULT_URL_PREFIX):
        return None
    rel = raw[len(VAULT_URL_PREFIX) :]
    found = _file_inside(root, rel)
    if found is not None:
        return found
    if rel.startswith("Notes/"):
        return _file_inside(root, rel[len("Notes/") :])
    return None


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


def search_socket() -> str:
    configured = os.environ.get("VAULT_SEARCH_SOCKET")
    if configured:
        return configured
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg:
        return str(Path(xdg) / "vault-search.sock")
    return f"/tmp/vault-search-{os.getuid()}.sock"


def run_vault_search(query: str, k: int = 4) -> str:
    query = query.strip()
    if not query:
        return "query is empty"
    k = max(1, min(int(k), 4))
    binary = os.environ.get(
        "VAULT_SEARCHD", str(Path.home() / ".local/bin/vault-searchd")
    )
    try:
        done = subprocess.run(
            [binary, "query", "--socket", search_socket(), "--k", str(k), query],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"vault_search failed: {exc}"
    text = (done.stdout or done.stderr or "").strip()
    if done.returncode != 0:
        return text or f"vault_search exited {done.returncode}"
    note = daily_note_text(query)
    if note:
        text = (text + "\n\n" + note).strip() if text else note
    return text or "No matching notes."


def daily_note_text(query: str) -> str:
    """The search hit for a daily note is often only the title. Read the file."""
    match = re.fullmatch(r"Daily/(\d{4}-\d{2}-\d{2})(?:\.md)?", query.strip())
    if not match:
        return ""
    path = _file_inside(VAULT_ROOT, f"Daily/{match.group(1)}.md")
    if path is None:
        return ""
    body = path.read_text(encoding="utf-8").strip()
    if len(body) > 4000:
        body = body[:4000].rstrip() + "\n…"
    return f"Full note Daily/{match.group(1)}.md:\n{body}"


_tools_cache: list | None = None


def tool_definitions() -> list:
    global _tools_cache
    if _tools_cache is not None:
        return _tools_cache
    host, port = _upstream_parts()
    conn = HTTPConnection(host, port, timeout=5)
    try:
        conn.request("GET", "/tools")
        resp = conn.getresponse()
        raw = resp.read()
    except OSError:
        return []
    finally:
        conn.close()
    try:
        listed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    _tools_cache = [
        item["definition"]
        for item in listed
        if isinstance(item, dict) and item.get("definition")
    ]
    return _tools_cache


def complete_with_tools(payload: dict, post, search, rounds: int = 3) -> dict:
    """Run vault_search when the model asks, then continue until a final answer."""
    messages = list(payload.get("messages") or [])
    result: dict = {}
    for _ in range(rounds):
        body = dict(payload)
        body["messages"] = messages
        body["stream"] = False
        result = post(body)
        message = result["choices"][0]["message"]
        calls = message.get("tool_calls") or []
        if not calls:
            if isinstance(message.get("content"), str):
                message["content"] = ensure_source_links(message["content"])
            return result
        messages.append(message)
        for call in calls:
            function = call.get("function") or {}
            name = function.get("name") or ""
            raw_args = function.get("arguments") or "{}"
            if isinstance(raw_args, str):
                try:
                    args = json.loads(raw_args)
                except json.JSONDecodeError:
                    args = {}
            else:
                args = raw_args
            if name == "vault_search":
                text = search(str(args.get("query") or ""), args.get("k") or 4)
            else:
                text = f"Unknown tool: {name}"
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call.get("id") or "call",
                    "content": text,
                }
            )
    return result


def post_chat(payload: dict) -> dict:
    host, port = _upstream_parts()
    raw = json.dumps(payload).encode("utf-8")
    conn = HTTPConnection(host, port, timeout=600)
    try:
        conn.request(
            "POST",
            "/v1/chat/completions",
            body=raw,
            headers={"Content-Type": "application/json", "Content-Length": str(len(raw))},
        )
        resp = conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise OSError(f"upstream {resp.status}: {data[:200]!r}")
    finally:
        conn.close()
    return json.loads(data)


def as_sse(result: dict) -> bytes:
    message = result["choices"][0]["message"]
    chunks = [
        {
            "choices": [
                {
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": message.get("content") or "",
                    },
                    "finish_reason": None,
                }
            ]
        },
        {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
    ]
    lines = [f"data: {json.dumps(item)}\n\n" for item in chunks]
    lines.append("data: [DONE]\n\n")
    return "".join(lines).encode("utf-8")


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

    def _chat(self, body: bytes) -> None:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            self.send_error(400, "chat body is not JSON")
            return
        if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
            self.send_error(400, "chat body needs messages")
            return
        try:
            note = load_note()
        except OSError as exc:
            self.send_error(502, f"system prompt unreadable: {exc}")
            return
        want_stream = bool(payload.get("stream"))
        prompt = system_text(note, datetime.now(ZONE))
        payload = inject_messages(payload, prompt)
        payload["stream"] = False
        if not payload.get("tools"):
            tools = tool_definitions()
            if tools:
                payload["tools"] = tools
                payload.setdefault("tool_choice", "auto")
        try:
            result = complete_with_tools(payload, post_chat, run_vault_search)
        except (OSError, json.JSONDecodeError, KeyError, IndexError) as exc:
            self.send_error(502, f"chat failed: {exc}")
            return
        if want_stream:
            raw = as_sse(result)
            content_type = "text/event-stream"
        else:
            raw = json.dumps(result).encode("utf-8")
            content_type = "application/json"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:
        self._proxy()

    def do_OPTIONS(self) -> None:
        self._proxy()

    def _proxy(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if self.command == "POST" and path in CHAT_PATHS:
            self._chat(body)
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
    assert "Daily/2026-10-30" in block
    assert "call vault_search" in block
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
    assert f"{PUBLIC_BASE}/vault/Daily/2026-10-31.md" in messages[0]["content"]

    def fake_post(body: dict) -> dict:
        queries = [m for m in body["messages"] if m.get("role") == "tool"]
        if not queries:
            return {
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "vault_search",
                                        "arguments": '{"query":"Daily/2026-10-30","k":4}',
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Yesterday is in Daily/2026-10-30.md.",
                    }
                }
            ]
        }

    seen = []

    def fake_search(query: str, k: int) -> str:
        seen.append((query, int(k)))
        return "path: Daily/2026-10-30.md\nscore: 0.900\nworked on the proxy\n"

    finished = complete_with_tools(
        {"messages": [{"role": "user", "content": "What we did yesterday?"}]},
        fake_post,
        fake_search,
    )
    assert seen == [("Daily/2026-10-30", 4)]
    assert "Daily/2026-10-30.md" in finished["choices"][0]["message"]["content"]
    assert "/vault/Daily/2026-10-30.md" in finished["choices"][0]["message"]["content"]
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
        daily = root / "Daily"
        daily.mkdir()
        (daily / "2026-10-31.md").write_text("day", encoding="utf-8")
        found = resolve_vault_file(root, "/vault/Notes/A.md")
        assert found == (note / "A.md").resolve()
        assert resolve_vault_file(root, "/vault/Notes/Daily/2026-10-31.md") == (
            daily / "2026-10-31.md"
        ).resolve()
        assert resolve_vault_file(root, "/vault/Daily/2026-10-31.md") == (
            daily / "2026-10-31.md"
        ).resolve()
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
