import importlib.metadata
from aistore.sdk.client import Client

try:
    __version__ = importlib.metadata.version("aistore")
except ImportError:
    pass
