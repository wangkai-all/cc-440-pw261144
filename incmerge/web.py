"""Read-mostly JSON API + static page for the local dashboard.

Uses only the standard library.  Mutating endpoints are limited to the
documented recovery actions (merge, backfill, resolve) and every one of them is
audited by the engine.
"""

from __future__ import annotations

import json
import mimetypes
import os
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Engine
from .util import IncmergeError, ValidationError, format_hour, parse_hour

WEB_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "web")
MAX_BODY = 8 * 1024 * 1024
_lock = threading.Lock()
_db_path = "incmerge.db"


def _engine(actor: str) -> Engine:
    return Engine(_db_path, actor=actor)


class Handler(BaseHTTPRequestHandler):
    server_version = "incmerge/0.1"

    def log_message(self, fmt: str, *args) -> None:  # quieter test output
        if os.environ.get("INCMERGE_HTTP_LOG"):
            super().log_message(fmt, *args)

    # ------------------------------------------------------------------ http
    def _send_json(self, status: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            raise ValidationError("request body too large")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(raw or b"{}")
        except json.JSONDecodeError as exc:
            raise ValidationError(f"invalid JSON body: {exc}") from exc
        if not isinstance(data, dict):
            raise ValidationError("request body must be a JSON object")
        return data

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path
        try:
            if path in {"/", "/index.html"}:
                return self._serve_static("index.html")
            if path.startswith("/static/"):
                return self._serve_static(path[len("/static/"):])
            if path == "/api/state":
                return self._send_json(200, self._state(query))
            if path == "/api/dataset":
                return self._send_json(200, self._dataset(query))
            if path == "/api/audit":
                return self._send_json(200, self._audit(query))
            if path == "/api/health":
                return self._send_json(200, {"ok": True})
            return self._send_error_json(404, f"no route for {path}")
        except IncmergeError as exc:
            return self._send_error_json(400, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            return self._send_error_json(500, f"internal error: {exc}")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/merge":
                body = self._read_json()
                return self._post_merge(body)
            if path == "/api/deliver":
                body = self._read_json()
                return self._post_deliver(body)
            if path == "/api/backfill":
                body = self._read_json()
                return self._post_backfill(body)
            if path == "/api/resolve":
                body = self._read_json()
                return self._post_resolve(body)
            return self._send_error_json(404, f"no route for {path}")
        except IncmergeError as exc:
            return self._send_error_json(400, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            return self._send_error_json(500, f"internal error: {exc}")

    def _serve_static(self, name: str) -> None:
        safe = os.path.normpath(name).replace("\\", "/")
        if safe.startswith("..") or os.path.isabs(safe):
            return self._send_error_json(404, "not found")
        full = os.path.join(WEB_ROOT, safe)
        root = os.path.abspath(WEB_ROOT)
        if not os.path.abspath(full).startswith(root) or not os.path.isfile(full):
            return self._send_error_json(404, "not found")
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        with open(full, "rb") as fh:
            data = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    # ------------------------------------------------------------------- api
    def _query_dataset(self, query: dict) -> str | None:
        value = query.get("dataset", [None])[0]
        return value or None

    def _state(self, query: dict) -> dict:
        engine = _engine("dashboard")
        try:
            datasets = engine.datasets()
            names = [d["name"] for d in datasets]
            selected = self._query_dataset(query) or (names[0] if names else None)
            payload: dict = {"datasets": names, "selected": selected, "audit": engine.verify_audit()}
            if selected:
                payload.update(self._dataset_payload(engine, selected))
            return payload
        finally:
            engine.close()

    def _dataset(self, query: dict) -> dict:
        engine = _engine("dashboard")
        try:
            name = self._query_dataset(query)
            if not name:
                raise ValidationError("dataset query parameter is required")
            return self._dataset_payload(engine, name)
        finally:
            engine.close()

    def _dataset_payload(self, engine: Engine, name: str) -> dict:
        from .engine import CONFLICT_RULES

        status = engine.status(name)["datasets"][0]
        return {
            "status": status,
            "timeline": engine.timeline(name),
            "conflicts": engine.conflicts(name, limit=200),
            "unresolved": engine.unresolved_conflicts(name),
            "audit": engine.verify_audit(),
            "audit_entries": engine.audit_trail(60),
            "records": engine.records(name, limit=60),
            "runs": engine.merge_runs(20),
            "conflict_rules": CONFLICT_RULES,
        }

    def _audit(self, query: dict) -> dict:
        engine = _engine("dashboard")
        try:
            limit = int(query.get("limit", ["200"])[0])
            return {"verification": engine.verify_audit(), "entries": engine.audit_trail(limit)}
        finally:
            engine.close()

    def _post_merge(self, body: dict) -> None:
        batch_id = body.get("batch_id")
        if batch_id is None:
            raise ValidationError("batch_id is required")
        engine = _engine(_actor(body))
        try:
            with _lock:
                result = engine.merge(int(batch_id))
            return self._send_json(200, result)
        finally:
            engine.close()

    def _post_deliver(self, body: dict) -> None:
        dataset = body.get("dataset")
        hour = body.get("hour")
        content = body.get("csv")
        if not dataset or not hour or content is None:
            raise ValidationError("dataset, hour and csv are required")
        engine = _engine(_actor(body))
        try:
            with _lock:
                result = engine.deliver_bytes(
                    str(dataset), str(hour), str(content).encode("utf-8"),
                    filename=body.get("filename") or "dashboard.csv",
                )
            return self._send_json(200, result)
        finally:
            engine.close()

    def _post_backfill(self, body: dict) -> None:
        dataset = body.get("dataset")
        hour = body.get("hour")
        if not dataset or not hour:
            raise ValidationError("dataset and hour are required")
        engine = _engine(_actor(body))
        try:
            with _lock:
                if body.get("csv") is not None:
                    result = engine.backfill(
                        str(dataset), str(hour), str(body["csv"]).encode("utf-8"),
                        filename=body.get("filename") or f"{hour}.csv",
                    )
                else:
                    # Re-drive an already-delivered pending batch for this hour.
                    hour_label = format_hour(parse_hour(str(hour)))
                    row = engine.conn.execute(
                        "SELECT id FROM batches WHERE dataset=? AND hour=?",
                        (str(dataset), hour_label),
                    ).fetchone()
                    if row is None:
                        raise ValidationError(
                            "no stored batch for this hour; supply csv to fill the gap"
                        )
                    result = {"outcome": "backfilled", "merge": engine.merge(int(row["id"]))}
            return self._send_json(200, result)
        finally:
            engine.close()

    def _post_resolve(self, body: dict) -> None:
        delivery_id = body.get("delivery_id")
        action = body.get("action")
        if delivery_id is None or not action:
            raise ValidationError("delivery_id and action are required")
        engine = _engine(_actor(body))
        try:
            with _lock:
                result = engine.resolve_conflict(int(delivery_id), str(action))
            return self._send_json(200, result)
        finally:
            engine.close()


def _actor(body: dict) -> str:
    actor = str(body.get("actor") or "dashboard").strip()
    return actor[:80] or "dashboard"


def serve(db: str, *, host: str = "127.0.0.1", port: int = 8765, open_browser: bool = False) -> None:
    global _db_path
    _db_path = db
    httpd = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}/"
    print(f"incmerge dashboard: {url}  (db: {db})")
    if open_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
