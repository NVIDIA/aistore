# Defaults
DEFAULT_ETL_COMM = "hpush"
DEFAULT_ETL_TIMEOUT = "5m"
DEFAULT_ETL_OBJ_TIMEOUT = "45s"

# ETL comm types
# ext/etl/api.go Hpush
ETL_COMM_HPUSH = "hpush"
# ext/etl/api.go Hpull
ETL_COMM_HPULL = "hpull"
# ext/etl/api.go WebSocket
ETL_COMM_WS = "ws"

# ETL lifecycle stages (see docs/etl.md#etl-pod-lifecycle)
ETL_STAGE_INIT = "Initializing"
ETL_STAGE_RUNNING = "Running"
ETL_STAGE_ABORTED = "Aborted"

ETL_COMM_OPTIONS = [ETL_COMM_HPUSH, ETL_COMM_HPULL, ETL_COMM_WS]

ETL_SUPPORTED_PYTHON_VERSIONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]


# Commands for ETL Containers

FASTAPI_CMD = [
    "uvicorn",
    "fastapi_server:fastapi_app",
    "--host",
    "0.0.0.0",
    "--port",
    "8000",
    "--workers",
    "4",
    "--no-access-log",
]
