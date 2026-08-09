#!/usr/bin/env python3
"""Minimal S3 stub: HEAD + ranged GET over a deterministic in-memory object.

Modes (per-request, chosen by the key name):
  /b/ok        -- behaves correctly
  /b/noetag    -- omits the ETag header on HEAD
  /b/short     -- returns one byte fewer than requested, once per range
  /b/stall     -- sends headers then never sends the body
  /b/precon    -- answers ranged GETs with 412 (simulates a changed object)
"""
import hashlib
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SIZE = 5 * 1024 * 1024 + 12345  # deliberately not a block multiple
BLOB = bytes((i * 7 + (i >> 8) * 13) & 0xFF for i in range(SIZE))
ETAG = '"%s"' % hashlib.md5(BLOB).hexdigest()
served_short = set()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _mode(self):
        return self.path.strip("/").split("/")[-1].split("?")[0]

    def do_HEAD(self):
        mode = self._mode()
        self.send_response(200)
        self.send_header("Content-Length", str(SIZE))
        self.send_header("Accept-Ranges", "bytes")
        if mode != "noetag":
            self.send_header("ETag", ETAG)
        self.end_headers()

    def do_GET(self):
        mode = self._mode()
        rng = self.headers.get("Range", "")
        if not rng.startswith("bytes="):
            self.send_response(200)
            self.send_header("Content-Length", str(SIZE))
            self.end_headers()
            self.wfile.write(BLOB)
            return
        a, b = rng[len("bytes="):].split("-")
        a, b = int(a), int(b)
        body = BLOB[a:b + 1]

        if mode == "precon":
            self.send_response(412)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if mode == "short" and a not in served_short:
            served_short.add(a)
            body = body[:-1]  # one byte short, first attempt only
        if mode == "stall":
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {a}-{b}/{SIZE}")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", ETAG)
            self.end_headers()
            time.sleep(600)  # headers sent, body never arrives
            return

        self.send_response(206)
        self.send_header("Content-Range", f"bytes {a}-{a + len(body) - 1}/{SIZE}")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("ETag", ETAG)
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(
        f"port={srv.server_address[1]} size={SIZE} "
        f"sha256={hashlib.sha256(BLOB).hexdigest()}",
        flush=True,
    )
    while True:
        time.sleep(3600)
