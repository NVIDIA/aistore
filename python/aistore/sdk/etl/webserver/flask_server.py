#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
from urllib.parse import unquote, quote

import requests
from flask import Flask, request, Response, jsonify

from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.utils import compose_etl_direct_put_url
from aistore.sdk.const import HEADER_NODE_URL, STATUS_NO_CONTENT, QPARAM_ETL_ARGS


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
            transformed = None
            if request.method == "GET":
                transformed = self._handle_get(path)
            else:
                transformed = self._handle_put(path)

            direct_put_url = request.headers.get(HEADER_NODE_URL)
            if direct_put_url:
                resp = self._direct_put(direct_put_url, transformed)
                if resp is not None:
                    return resp

            return Response(response=transformed, content_type=self.get_mime_type())
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
        etl_args = request.args.get(QPARAM_ETL_ARGS, "")
        if self.arg_type == "fqn":
            content = self._get_fqn_content(path)
        else:
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET to: %s", target_url)
            resp = requests.get(target_url, timeout=None)
            resp.raise_for_status()
            content = resp.content
        return self.transform(content, path, etl_args)

    def _handle_put(self, path):

        etl_args = request.args.get(QPARAM_ETL_ARGS, "")

        if self.arg_type == "fqn":
            content = self._get_fqn_content(path)
        else:
            content = request.get_data()

        return self.transform(content, path, etl_args)

    def _get_fqn_content(self, path: str) -> bytes:
        decoded_path = unquote(path)
        safe_path = os.path.normpath(os.path.join("/", decoded_path.lstrip("/")))
        self.logger.debug("Reading local file: %s", safe_path)
        with open(safe_path, "rb") as f:
            return f.read()

    def _direct_put(self, direct_put_url: str, data: bytes):
        """
        Sends the transformed object directly to the specified AIS node (`direct_put_url`),
        eliminating the additional network hop through the original target.
        Used only in bucket-to-bucket offline transforms.
        """
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target)
            resp = requests.put(url, data, timeout=None)
            if resp.status_code == 200:
                return Response(status=STATUS_NO_CONTENT)

            error = resp.text()
            self.logger.error(
                "Failed to deliver object to %s: HTTP %s, %s",
                direct_put_url,
                resp.status_code,
                error,
            )
        except Exception as e:
            self.logger.error("Exception during delivery to %s: %s", direct_put_url, e)

        return None

    # Example Gunicorn command to run this server:
    # command: ["gunicorn", "your_module:flask_app", "--bind", "0.0.0.0:8000", "--workers", "4"]
    def start(self):
        self.logger.info("Starting Flask ETL server at %s:%d", self.host, self.port)
        self.app.run(
            host=self.host, port=self.port, threaded=True
        )  # threaded=True for multi-threaded support
