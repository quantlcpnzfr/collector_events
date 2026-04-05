from __future__ import annotations

import json
import logging
import os
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from collector_events.telemetry import track
from collector_events.share.snapshots import build_currency_strength_snapshot
from collector_events.share.tokens import ShareTokenError, create_signed_token, token_fingerprint, verify_signed_token


logger = logging.getLogger(__name__)


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _render_share_new_page() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Share Currency Strength Snapshot</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; padding: 24px; max-width: 820px; margin: 0 auto; }
      .card { border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; }
      button { background: #111827; color: white; border: 0; border-radius: 10px; padding: 10px 14px; cursor: pointer; }
      button:disabled { opacity: 0.5; cursor: not-allowed; }
      input { width: 100%; padding: 10px 12px; border: 1px solid #e5e7eb; border-radius: 10px; font-size: 14px; }
      .muted { color: #6b7280; font-size: 13px; }
      .row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
      .row > * { flex: 1; }
      .row button { flex: 0 0 auto; }
      .ok { color: #065f46; }
      .err { color: #991b1b; white-space: pre-wrap; }
      code { background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }
    </style>
  </head>
  <body>
    <h1>Share Currency Strength Snapshot</h1>
    <p class="muted">Generates a signed link that anyone can open (no login), so teammates can quickly see the current strength ranking.</p>
    <div class="card">
      <div class="row">
        <button id="btn">Generate share link</button>
        <span id="status" class="muted"></span>
      </div>
      <div style="height: 12px"></div>
      <label class="muted" for="link">Share link</label>
      <div class="row">
        <input id="link" readonly placeholder="Click “Generate share link”" />
        <button id="copy" disabled>Copy</button>
        <button id="nativeShare" disabled>Share…</button>
      </div>
      <div style="height: 10px"></div>
      <div class="muted">Requires <code>SHARE_TOKEN_SECRET</code> and a working <code>DATABASE_URL</code> (to pull latest <code>currency_strength</code> rows).</div>
      <div id="error" class="err"></div>
    </div>
    <script>
      const btn = document.getElementById('btn');
      const link = document.getElementById('link');
      const copy = document.getElementById('copy');
      const nativeShare = document.getElementById('nativeShare');
      const status = document.getElementById('status');
      const error = document.getElementById('error');

      function setStatus(text, cls) {
        status.textContent = text || '';
        status.className = cls || 'muted';
      }

      btn.addEventListener('click', async () => {
        error.textContent = '';
        setStatus('Generating…', 'muted');
        btn.disabled = true;
        try {
          const res = await fetch('/api/share/currency-strength', { method: 'POST' });
          const data = await res.json();
          if (!res.ok) throw new Error(data && data.error ? data.error : 'Request failed');
          link.value = data.url;
          copy.disabled = false;
          nativeShare.disabled = !(navigator.share);
          setStatus('Ready.', 'ok');
        } catch (e) {
          error.textContent = String(e && e.message ? e.message : e);
          setStatus('Error.', 'err');
        } finally {
          btn.disabled = false;
        }
      });

      copy.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(link.value);
          setStatus('Copied.', 'ok');
        } catch {
          setStatus('Copy failed.', 'err');
        }
      });

      nativeShare.addEventListener('click', async () => {
        try {
          await navigator.share({ title: 'Currency Strength Snapshot', url: link.value });
        } catch {}
      });
    </script>
  </body>
</html>"""


def _render_snapshot_page(snapshot: dict[str, Any], share_url: str, as_of: str | None) -> str:
    strongest = snapshot.get("top", {}).get("strongest", [])
    weakest = snapshot.get("top", {}).get("weakest", [])
    items = snapshot.get("items", [])

    def fmt_row(item: dict[str, Any]) -> str:
        ccy = escape(str(item.get("currency", "")))
        score = escape(f'{item.get("strength_score", 0.0):.1f}')
        trend = escape(str(item.get("trend") or ""))
        mom = escape(f'{item.get("momentum", 0.0):.1f}')
        return f"<tr><td>{ccy}</td><td style='text-align:right'>{score}</td><td>{trend}</td><td style='text-align:right'>{mom}</td></tr>"

    def fmt_list(items_list: list[dict[str, Any]]) -> str:
        parts = []
        for item in items_list:
            score = f"{item.get('strength_score', 0.0):.1f}"
            parts.append(
                f"<li><strong>{escape(str(item.get('currency')))}</strong> "
                f"{escape(score)}</li>"
            )
        return "\n".join(parts) or "<li class='muted'>No data.</li>"

    title = "Currency Strength Snapshot"
    desc = "Fast snapshot of strongest vs weakest currencies."
    if strongest and weakest:
        desc = f"Strongest: {strongest[0].get('currency')} | Weakest: {weakest[0].get('currency')}"

    as_of_line = f"As of {escape(as_of)}" if as_of else "As of (unknown)"

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{escape(title)}</title>
    <meta property="og:title" content="{escape(title)}" />
    <meta property="og:description" content="{escape(desc)}" />
    <meta property="og:type" content="website" />
    <style>
      body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; padding: 24px; max-width: 920px; margin: 0 auto; }}
      .muted {{ color: #6b7280; font-size: 13px; }}
      .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
      .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; }}
      table {{ width: 100%; border-collapse: collapse; }}
      th, td {{ border-bottom: 1px solid #f3f4f6; padding: 10px 6px; font-size: 14px; }}
      th {{ text-align: left; color: #374151; }}
      button {{ background: #111827; color: white; border: 0; border-radius: 10px; padding: 8px 12px; cursor: pointer; }}
      input {{ width: 100%; padding: 10px 12px; border: 1px solid #e5e7eb; border-radius: 10px; font-size: 14px; }}
      ul {{ margin: 0; padding-left: 18px; }}
    </style>
  </head>
  <body>
    <h1>{escape(title)}</h1>
    <div class="muted">{as_of_line}</div>
    <div style="height: 14px"></div>
    <div class="card">
      <div class="muted">Share this link</div>
      <div style="height: 8px"></div>
      <div style="display:flex; gap:10px; align-items:center; flex-wrap: wrap;">
        <input id="link" readonly value="{escape(share_url)}" />
        <button id="copy">Copy</button>
        <button id="nativeShare" style="display:none;">Share…</button>
      </div>
    </div>
    <div style="height: 16px"></div>
    <div class="grid">
      <div class="card">
        <h3 style="margin-top:0;">Strongest</h3>
        <ul>{fmt_list(strongest)}</ul>
      </div>
      <div class="card">
        <h3 style="margin-top:0;">Weakest</h3>
        <ul>{fmt_list(weakest)}</ul>
      </div>
    </div>
    <div style="height: 16px"></div>
    <div class="card">
      <h3 style="margin-top:0;">All currencies</h3>
      <table>
        <thead>
          <tr><th>Currency</th><th style="text-align:right">Strength</th><th>Trend</th><th style="text-align:right">Momentum</th></tr>
        </thead>
        <tbody>
          {''.join(fmt_row(i) for i in items) if items else "<tr><td colspan='4' class='muted'>No data.</td></tr>"}
        </tbody>
      </table>
    </div>
    <script>
      const link = document.getElementById('link');
      const copy = document.getElementById('copy');
      const nativeShare = document.getElementById('nativeShare');
      if (navigator.share) nativeShare.style.display = 'inline-block';
      copy.addEventListener('click', async () => {{
        try {{ await navigator.clipboard.writeText(link.value); copy.textContent = 'Copied'; setTimeout(() => copy.textContent='Copy', 1200); }} catch {{}}
      }});
      nativeShare.addEventListener('click', async () => {{
        try {{ await navigator.share({{ title: 'Currency Strength Snapshot', url: link.value }}); }} catch {{}}
      }});
    </script>
  </body>
</html>"""


class ShareServerHandler(BaseHTTPRequestHandler):
    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: Any) -> None:
        self._send(status, "application/json; charset=utf-8", _json_bytes(payload))

    def _send_html(self, status: int, html: str) -> None:
        self._send(status, "text/html; charset=utf-8", html.encode("utf-8"))

    def _base_url(self) -> str:
        configured = os.getenv("PUBLIC_BASE_URL")
        if configured:
            return configured.rstrip("/")
        host = self.headers.get("Host") or "localhost"
        return f"http://{host}".rstrip("/")

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        logger.info("%s - %s", self.address_string(), format % args)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", ""):
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "/share/new")
            self.end_headers()
            return

        if path == "/share/new":
            self._send_html(HTTPStatus.OK, _render_share_new_page())
            return

        if path.startswith("/s/"):
            token = path[len("/s/") :]
            secret = os.getenv("SHARE_TOKEN_SECRET", "")
            try:
                payload = verify_signed_token(token, secret=secret)
            except ShareTokenError as exc:
                self._send_html(
                    HTTPStatus.UNAUTHORIZED,
                    f"<h1>Invalid share link</h1><p>{escape(str(exc))}</p>",
                )
                return

            snapshot = payload.get("snapshot") if isinstance(payload, dict) else None
            if not isinstance(snapshot, dict):
                self._send_html(HTTPStatus.BAD_REQUEST, "<h1>Invalid snapshot payload</h1>")
                return

            share_url = f"{self._base_url()}/s/{token}"
            track(
                "share_opened",
                {
                    "share_type": payload.get("type"),
                    "as_of": snapshot.get("as_of"),
                    "token_fp": token_fingerprint(token),
                    "user_agent": self.headers.get("User-Agent"),
                },
            )

            self._send_html(
                HTTPStatus.OK,
                _render_snapshot_page(snapshot=snapshot, share_url=share_url, as_of=snapshot.get("as_of")),
            )
            return

        self._send_html(HTTPStatus.NOT_FOUND, "<h1>Not found</h1>")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/share/currency-strength":
            secret = os.getenv("SHARE_TOKEN_SECRET", "")
            if not secret:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "Missing SHARE_TOKEN_SECRET in environment."},
                )
                return

            try:
                snapshot = build_currency_strength_snapshot()
            except Exception as exc:  # noqa: BLE001
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": f"Could not build snapshot from DB. Check DATABASE_URL. Details: {exc}"},
                )
                return

            token = create_signed_token({"type": "currency_strength_snapshot", "snapshot": snapshot}, secret=secret)
            url = f"{self._base_url()}/s/{token}"

            track(
                "share_clicked",
                {
                    "share_type": "currency_strength_snapshot",
                    "as_of": snapshot.get("as_of"),
                    "items": len(snapshot.get("items", []) or []),
                    "token_fp": token_fingerprint(token),
                },
            )

            self._send_json(HTTPStatus.OK, {"url": url, "expires_in_seconds": 7 * 24 * 60 * 60})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})


def run_share_server(host: str = "127.0.0.1", port: int = 8080) -> None:
    server = ThreadingHTTPServer((host, port), ShareServerHandler)
    logger.info("Share server listening on http://%s:%s", host, port)
    server.serve_forever()
