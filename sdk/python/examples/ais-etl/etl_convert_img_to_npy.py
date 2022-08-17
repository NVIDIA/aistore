"""
ETL to convert images to numpy arrays.
Communication Type: hpush://

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""
from aistore import Client
import numpy as np
import cv2

client = Client(
    "http://192.168.49.2:8080"
)  # ip addr of aistore cluster (in k8s or minikube)


def transform(input_bytes):
    nparr = np.fromstring(input_bytes, np.uint8)
    return cv2.imdecode(nparr, cv2.IMREAD_COLOR)


# other opencv packages dont work in dockerized environments
deps = ["opencv-python-headless==4.5.3.56"]

# initialize ETL
client.etl().init_code(transform=transform, etl_id="etl-img-to-npy", dependencies=deps)

# Transform bucket with given ETL id
xaction_id = client.bucket("from-bck").transform(
    etl_id="etl-img-to-npy", to_bck="to-bck", ext={"jpg": "npy"}
)
client.xaction().wait_for_xaction_finished(xaction_id)

# load a object from transformed bucket
print(
    np.frombuffer(
        client.bucket("to-bck").object("obj-id.npy").get().read_all(), dtype=np.uint8
    )
)
