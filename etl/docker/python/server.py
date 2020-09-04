#!/usr/bin/env python

import os
import imp
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler

host_target = os.environ["AIS_TARGET_URL"]

mod = imp.load_source("function", "/code/%s.py" % os.getenv("MOD_NAME"))
transform = getattr(mod, os.getenv("FUNC_HANDLER"))


class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

    def do_PUT(self):
        content_length = int(self.headers["Content-Length"])

        input_bytes = self.rfile.read(content_length)
        result = transform(input_bytes)
        self._set_headers()
        self.wfile.write(result)

    def do_GET(self):
        if self.path == "/health":
            self._set_headers()
            self.wfile.write(b"OK")
            return

        global host_target

        input_bytes = requests.get(host_target + "/v1/objects" + self.path)
        result = transform(input_bytes)
        self._set_headers()
        self.wfile.write(result)


def run(server_class=HTTPServer, handler_class=S, addr="0.0.0.0", port=80):
    server_address = (addr, port)
    httpd = server_class(server_address, handler_class)

    print(f"Starting httpd server on {addr}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run(addr="0.0.0.0", port=80)
