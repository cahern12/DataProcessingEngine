"""Read-only paginated local source API. Cursors address file/byte positions."""

import base64
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .common import canonical, read_json, safe_file
from .ingestion import validate_batch


def make_server(root, host="127.0.0.1", port=8765):
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("mock API binds to loopback only")
    checked = validate_batch(root)
    manifest = read_json(root / "manifest.json")
    files = [f for f in manifest["files"] if f["dataset"] == "incident_events"]
    if any(f["format"] != "jsonl" for f in files):
        raise ValueError("event API expects JSONL source partitions")
    identity = checked["manifest_sha256"]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            expected = "/v1/batches/" + manifest["batch_id"] + "/incident-events"
            if parsed.path != expected:
                self.send_error(404)
                return
            try:
                query = parse_qs(parsed.query)
                limit = int(query.get("limit", ["100"])[0])
                if not 1 <= limit <= 1000:
                    raise ValueError("limit must be 1..1000")
                cursor = query.get("cursor", [""])[0]
                part, offset = 0, 0
                if cursor:
                    mark, part, offset = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                    if mark != identity or type(part) is not int or type(offset) is not int or part < 0 or part >= len(files) or offset < 0 or offset > files[part]["bytes"]:
                        raise ValueError("invalid cursor")
                records = []
                while part < len(files) and len(records) < limit:
                    with safe_file(root, files[part]["path"]).open("rb") as stream:
                        stream.seek(offset)
                        while len(records) < limit:
                            line = stream.readline()
                            if not line:
                                break
                            records.append(json.loads(line))
                        offset = stream.tell()
                    if offset == files[part]["bytes"]:
                        part, offset = part + 1, 0
                next_cursor = None if part == len(files) else base64.urlsafe_b64encode(canonical([identity, part, offset]).encode()).decode()
                payload = canonical({"batch_id": manifest["batch_id"], "manifest_sha256": identity,
                                     "records": records, "next_cursor": next_cursor}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (ValueError, TypeError, KeyError, IndexError, UnicodeError):
                self.send_error(400, "Invalid pagination request")

    return ThreadingHTTPServer((host, port), Handler)
