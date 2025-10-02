#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import os
from urllib.parse import unquote, quote
from typing import Tuple

import requests
from flask import Flask, request, Response, jsonify

from aistore.sdk.etl.webserver.base_etl_server import ETLServer
from aistore.sdk.etl.webserver.utils import (
    compose_etl_direct_put_url,
    parse_etl_pipeline,
)
from aistore.sdk.errors import InvalidPipelineError
from aistore.sdk.const import (
    HEADER_NODE_URL,
    STATUS_OK,
    QPARAM_ETL_ARGS,
    QPARAM_ETL_FQN,
    HEADER_DIRECT_PUT_LENGTH,
    STATUS_INTERNAL_SERVER_ERROR,
)


class FlaskServer(ETLServer):
    """
    Flask server implementation for ETL transformations.
    Compatible with environments where Flask is preferred over FastAPI.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
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

            pipeline_header = request.headers.get(HEADER_NODE_URL)
            self.logger.debug("pipeline_header: %r", pipeline_header)
            if pipeline_header:
                first_url, remaining_pipeline = parse_etl_pipeline(pipeline_header)
                if first_url:
                    status_code, transformed, direct_put_length = self._direct_put(
                        first_url, transformed, remaining_pipeline, path
                    )
                    return Response(
                        response=transformed,
                        status=status_code,
                        headers=(
                            {HEADER_DIRECT_PUT_LENGTH: str(direct_put_length)}
                            if direct_put_length != 0
                            else {}
                        ),
                    )

            return Response(
                response=transformed,
                status=STATUS_OK,
                content_type=self.get_mime_type(),
            )

        except InvalidPipelineError as e:
            self.logger.error("Invalid pipeline header: %s", str(e))
            return Response(f"Invalid pipeline header: {str(e)}", status=400)
        except FileNotFoundError as exc:
            fs_path = exc.filename or path
            self.logger.error(
                "Error processing object %r: file not found at %r",
                path,
                fs_path,
            )
            return (
                jsonify(
                    {
                        "error": (
                            f"Error processing object {path!r}: file not found at {fs_path!r}. "
                        )
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
        etl_args = request.args.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.args.get(QPARAM_ETL_FQN, "").strip()
        if fqn:
            content = self._get_fqn_content(fqn)
        else:
            obj_path = quote(path, safe="@")
            target_url = f"{self.host_target}/{obj_path}"
            self.logger.debug("Forwarding GET to: %s", target_url)
            resp = requests.get(target_url, timeout=None)
            resp.raise_for_status()
            content = resp.content
        return self.transform(content, path, etl_args)

    def _handle_put(self, path):
        etl_args = request.args.get(QPARAM_ETL_ARGS, "").strip()
        fqn = request.args.get(QPARAM_ETL_FQN, "").strip()
        if fqn:
            content = self._get_fqn_content(fqn)
        else:
            content = request.get_data()

        return self.transform(content, path, etl_args)

    def _get_fqn_content(self, path: str) -> bytes:
        decoded_path = unquote(path)
        safe_path = os.path.normpath(os.path.join("/", decoded_path.lstrip("/")))
        self.logger.debug("Reading local file: %s", safe_path)
        with open(safe_path, "rb") as f:
            return f.read()

    def _direct_put(
        self,
        direct_put_url: str,
        data: bytes,
        remaining_pipeline: str = "",
        path: str = "",
    ) -> Tuple[int, bytes, int]:
        """
        Sends the transformed object directly to the specified AIS node (`direct_put_url`),
        eliminating the additional network hop through the original target.
        Used only in bucket-to-bucket offline transforms.

        Args:
            direct_put_url: The first URL in the ETL pipeline
            data: The transformed data to send
            remaining_pipeline: Comma-separated remaining pipeline stages to pass as header
            path: The path of the object.
        Returns:
            status code, transformed data, length of the transformed data (if any)
        """
        try:
            url = compose_etl_direct_put_url(direct_put_url, self.host_target, path)
            headers = {}
            if remaining_pipeline:
                headers[HEADER_NODE_URL] = remaining_pipeline

            resp = self.client_put(url, data, headers=headers)
            return self.handle_direct_put_response(resp, data)

        except Exception as e:
            error = str(e).encode()
            self.logger.error("Exception in direct put to %s: %s", direct_put_url, e)
            return STATUS_INTERNAL_SERVER_ERROR, error, 0

    # Example Gunicorn command to run this server:
    # command: ["gunicorn", "your_module:flask_app", "--bind", "0.0.0.0:8000", "--workers", "4"]
    def start(self):
        self.logger.info("Starting Flask ETL server at %s:%d", self.host, self.port)
        self.app.run(
            host=self.host, port=self.port, threaded=True
        )  # threaded=True for multi-threaded support
