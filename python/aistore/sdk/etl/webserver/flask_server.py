import os
from urllib.parse import unquote, quote

import requests
from flask import Flask, request, Response, jsonify

from aistore.sdk.etl.webserver.base_etl_server import ETLServer


class FlaskServer(ETLServer):
    """
    Flask server implementation for ETL transformations.
    Compatible with environments where Flask is preferred over FastAPI.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 80):
        super().__init__()
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self._register_routes()

    def _register_routes(self):
        self.app.add_url_rule("/health", view_func=self._health, methods=["GET"])
        self.app.add_url_rule(
            "/<path:path>", view_func=self._handle_request, methods=["GET", "PUT"]
        )

    def _health(self):
        return Response(response=b"Running", status=200)

    def _handle_request(self, path):
        try:
            if request.method == "GET":
                return self._handle_get(path)
            return self._handle_put(path)
        except FileNotFoundError:
            self.logger.error("File not found: %s", path)
            return (
                jsonify(
                    {
                        "error": f"Local file not found: {path}. This typically indicates the container was not \
                        started with the correct volume mounts. Please verify your pod or container configuration \
                        includes the necessary mount paths."
                    }
                ),
                404,
            )
        except requests.RequestException as e:
            self.logger.error("Request to AIS target failed: %s", str(e))
            return jsonify({"error": str(e)}), 502
        except Exception as e:
            self.logger.exception("Unhandled error")
            return jsonify({"error": str(e)}), 500

    def _handle_get(self, path):
        if self.arg_type == "fqn":
            content = self._get_fqn_content(path)
        else:
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET to: %s", target_url)
            resp = requests.get(target_url, timeout=None)
            resp.raise_for_status()
            content = resp.content

        transformed = self.transform(content, path)
        return Response(response=transformed, content_type=self.get_mime_type())

    def _handle_put(self, path):
        if self.arg_type == "fqn":
            content = self._get_fqn_content(path)
        else:
            content = request.get_data()

        transformed = self.transform(content, path)
        return Response(response=transformed, content_type=self.get_mime_type())

    def _get_fqn_content(self, path: str) -> bytes:
        decoded_path = unquote(path)
        safe_path = os.path.normpath(os.path.join("/", decoded_path.lstrip("/")))
        self.logger.debug("Reading local file: %s", safe_path)
        with open(safe_path, "rb") as f:
            return f.read()

    # Example Gunicorn command to run this server:
    # command: ["gunicorn", "your_module:flask_app", "--bind", "0.0.0.0:8000", "--workers", "4"]
    def start(self):
        self.logger.info("Starting Flask ETL server at %s:%d", self.host, self.port)
        self.app.run(
            host=self.host, port=self.port, threaded=True
        )  # threaded=True for multi-threaded support
