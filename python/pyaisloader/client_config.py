import os
from aistore import Client

ENDPOINT = os.environ["AIS_ENDPOINT"]
client = Client(ENDPOINT)
