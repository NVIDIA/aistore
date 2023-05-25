import logging
from aistore.sdk.client import Client
from aistore.version import __version__

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
