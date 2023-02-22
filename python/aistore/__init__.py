import logging
import importlib.metadata
from aistore.sdk.client import Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

try:
    __version__ = importlib.metadata.version("aistore")
except ImportError:
    pass
