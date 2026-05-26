#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""CLI entrypoint for `python -m aistore.mcp`.

Delegates to :func:`aistore.mcp.server.main`. Set `AIS_ENDPOINT` first.
"""

from aistore.mcp.server import main

main()
