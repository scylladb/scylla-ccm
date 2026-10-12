import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

from urllib3.util.retry import Retry

from ccmlib.utils import net


def test_http_get_retries_5xx_then_succeeds():
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(1)
            self.send_response(503 if len(hits) < 3 else 200)
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'ok')

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with patch.object(Retry, 'sleep'):
            resp = net.http_get(f'http://127.0.0.1:{server.server_port}/')
    finally:
        server.shutdown()
    assert resp.status_code == 200 and len(hits) == 3


def test_http_get_returns_last_5xx_when_retries_exhausted():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with patch.object(Retry, 'sleep'):
            resp = net.http_get(f'http://127.0.0.1:{server.server_port}/')
    finally:
        server.shutdown()
    assert resp.status_code == 503


def test_s3_config_has_retries():
    assert net.s3_config().retries == {'max_attempts': 10, 'mode': 'standard'}
