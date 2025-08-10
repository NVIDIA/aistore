import os
from aistore import Client

AIS_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
client = Client(AIS_ENDPOINT)
