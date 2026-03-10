"""Landing page ASGI middleware for the function dev server.

Provides:
- GET /          → HTML landing page with function info, CDF warning, and live logs
- GET /api/logs  → SSE endpoint streaming log lines
- GET /api/status → JSON with function info and last-reload timestamp
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

# ASGI type aliases (same convention as cognite_function_apps.devserver.asgi)
Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class _LogCollector(logging.Handler):
    """Logging handler that appends formatted records to a deque."""

    def __init__(self, buffer: deque[dict[str, str]], *, maxlen: int = 500) -> None:
        super().__init__()
        self.buffer = buffer
        self._seq = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._seq += 1
            self.buffer.append(
                {
                    "seq": self._seq,
                    "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                    "level": record.levelname,
                    "name": record.name,
                    "message": self.format(record),
                }
            )
        except Exception:
            self.handleError(record)


class LandingPageMiddleware:
    """ASGI middleware that adds a landing page, status API, and log streaming."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        handler_name: str,
        handler_path: str,
        cdf_project: str,
        cdf_cluster: str,
    ) -> None:
        self.app = app
        self.handler_name = handler_name
        self.handler_path = handler_path
        self.cdf_project = cdf_project
        self.cdf_cluster = cdf_cluster
        self.last_reload = datetime.now(tz=timezone.utc)
        self._start_time = time.monotonic()

        # Log collection
        self._log_buffer: deque[dict[str, str]] = deque(maxlen=500)
        self._log_handler = _LogCollector(self._log_buffer)
        self._log_handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(self._log_handler)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")

        if method == "GET" and path == "/":
            await self._serve_landing_page(send)
        elif method == "GET" and path == "/api/logs":
            await self._serve_sse_logs(send)
        elif method == "GET" and path == "/api/status":
            await self._serve_status(send)
        else:
            await self.app(scope, receive, send)

    async def _send_response(self, send: Send, *, status: int, content_type: str, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", content_type.encode()),
                    (b"content-length", str(len(body)).encode()),
                ],
                "trailers": False,
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})

    async def _serve_status(self, send: Send) -> None:
        uptime_s = round(time.monotonic() - self._start_time, 1)
        payload = {
            "handler_name": self.handler_name,
            "handler_path": self.handler_path,
            "cdf_project": self.cdf_project,
            "cdf_cluster": self.cdf_cluster,
            "last_reload": self.last_reload.isoformat(),
            "uptime_seconds": uptime_s,
        }
        body = json.dumps(payload).encode()
        await self._send_response(send, status=200, content_type="application/json", body=body)

    async def _serve_sse_logs(self, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"text/event-stream"),
                    (b"cache-control", b"no-cache"),
                    (b"connection", b"keep-alive"),
                    (b"x-accel-buffering", b"no"),
                ],
                "trailers": False,
            }
        )

        # Send all buffered lines first
        for entry in list(self._log_buffer):
            chunk = f"data: {json.dumps(entry)}\n\n".encode()
            await send({"type": "http.response.body", "body": chunk, "more_body": True})

        last_seq = self._log_buffer[-1]["seq"] if self._log_buffer else 0

        # Tail new entries
        try:
            while True:
                await asyncio.sleep(0.5)
                new_entries = [e for e in self._log_buffer if e["seq"] > last_seq]
                for entry in new_entries:
                    chunk = f"data: {json.dumps(entry)}\n\n".encode()
                    await send({"type": "http.response.body", "body": chunk, "more_body": True})
                    last_seq = entry["seq"]
        except (asyncio.CancelledError, Exception):
            # Client disconnected
            await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _serve_landing_page(self, send: Send) -> None:
        html = _build_landing_html(
            handler_name=self.handler_name,
            handler_path=self.handler_path,
            cdf_project=self.cdf_project,
            cdf_cluster=self.cdf_cluster,
            last_reload=self.last_reload.strftime("%Y-%m-%d %H:%M:%S UTC"),
        )
        await self._send_response(send, status=200, content_type="text/html; charset=utf-8", body=html.encode())


def _build_landing_html(
    *,
    handler_name: str,
    handler_path: str,
    cdf_project: str,
    cdf_cluster: str,
    last_reload: str,
) -> str:
    # Escape for safe HTML embedding
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(handler_name)} — CDF Dev Server</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: #0f1117; color: #e0e0e0; line-height: 1.5; }}
  .warning-banner {{
    background: #4a2800; border-bottom: 2px solid #ff9800; padding: 12px 20px;
    text-align: center; font-size: 14px; color: #ffcc80;
  }}
  .warning-banner strong {{ color: #ffa726; }}
  .container {{ max-width: 900px; margin: 0 auto; padding: 24px 20px; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; color: #fff; }}
  .subtitle {{ color: #888; font-size: 13px; margin-bottom: 20px; }}
  .card {{
    background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 8px;
    padding: 16px 20px; margin-bottom: 16px;
  }}
  .card h2 {{ font-size: 14px; text-transform: uppercase; letter-spacing: 0.5px;
              color: #888; margin-bottom: 10px; }}
  .info-row {{ display: flex; justify-content: space-between; padding: 6px 0;
               border-bottom: 1px solid #2a2d3a; font-size: 14px; }}
  .info-row:last-child {{ border-bottom: none; }}
  .info-label {{ color: #888; }}
  .info-value {{ color: #e0e0e0; font-family: monospace; }}
  a {{ color: #64b5f6; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .docs-link {{
    display: inline-block; background: #1565c0; color: #fff; padding: 8px 20px;
    border-radius: 6px; font-size: 14px; font-weight: 500; margin-top: 8px;
  }}
  .docs-link:hover {{ background: #1976d2; text-decoration: none; }}
  #log-viewer {{
    background: #12141c; border: 1px solid #2a2d3a; border-radius: 6px;
    padding: 12px; height: 350px; overflow-y: auto; font-family: "SF Mono", "Fira Code",
    "Cascadia Code", monospace; font-size: 12px; line-height: 1.7;
  }}
  .log-line {{ white-space: pre-wrap; word-break: break-all; }}
  .log-line .ts {{ color: #666; }}
  .log-line .lvl-INFO {{ color: #64b5f6; }}
  .log-line .lvl-WARNING {{ color: #ffb74d; }}
  .log-line .lvl-ERROR {{ color: #ef5350; }}
  .log-line .lvl-DEBUG {{ color: #81c784; }}
  #reload-ts {{ font-family: monospace; color: #aaa; }}
</style>
</head>
<body>
<div class="warning-banner">
  &#9888;&#65039; Connected to CDF project <strong>{esc(cdf_project)}</strong>
  ({esc(cdf_cluster)}).
  Handlers have <strong>full read/write access</strong>.
</div>
<div class="container">
  <h1>{esc(handler_name)}</h1>
  <p class="subtitle">CDF Function Dev Server</p>

  <div class="card">
    <h2>Function</h2>
    <div class="info-row">
      <span class="info-label">Handler</span>
      <span class="info-value">{esc(handler_path)}</span>
    </div>
    <div class="info-row">
      <span class="info-label">CDF Project</span>
      <span class="info-value">{esc(cdf_project)}</span>
    </div>
    <div class="info-row">
      <span class="info-label">Cluster</span>
      <span class="info-value">{esc(cdf_cluster)}</span>
    </div>
    <div class="info-row">
      <span class="info-label">Last Reload</span>
      <span class="info-value" id="reload-ts">{esc(last_reload)}</span>
    </div>
    <div style="margin-top: 12px;">
      <a href="/docs" class="docs-link">Open API Docs</a>
    </div>
  </div>

  <div class="card">
    <h2>Server Logs</h2>
    <div id="log-viewer"></div>
  </div>
</div>

<script>
(function() {{
  const viewer = document.getElementById('log-viewer');
  const reloadTs = document.getElementById('reload-ts');
  let autoScroll = true;

  viewer.addEventListener('scroll', function() {{
    autoScroll = (viewer.scrollTop + viewer.clientHeight >= viewer.scrollHeight - 30);
  }});

  function appendLog(entry) {{
    const div = document.createElement('div');
    div.className = 'log-line';
    const ts = entry.ts ? entry.ts.substring(11, 19) : '';
    const lvl = entry.level || 'INFO';
    div.innerHTML = '<span class="ts">' + ts + '</span> '
      + '<span class="lvl-' + lvl + '">' + lvl.padEnd(8) + '</span> '
      + escapeHtml(entry.message || '');
    viewer.appendChild(div);
    if (autoScroll) viewer.scrollTop = viewer.scrollHeight;
  }}

  function escapeHtml(s) {{
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }}

  const evtSource = new EventSource('/api/logs');
  evtSource.onmessage = function(e) {{
    try {{
      appendLog(JSON.parse(e.data));
    }} catch(err) {{}}
  }};

  // Poll status for reload timestamp updates
  setInterval(function() {{
    fetch('/api/status').then(r => r.json()).then(d => {{
      if (d.last_reload) {{
        const dt = new Date(d.last_reload);
        reloadTs.textContent = dt.toISOString().replace('T', ' ').substring(0, 19) + ' UTC';
      }}
    }}).catch(function() {{}});
  }}, 3000);
}})();
</script>
</body>
</html>"""
