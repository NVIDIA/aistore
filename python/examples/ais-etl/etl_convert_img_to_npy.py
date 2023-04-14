"""
ETL to convert images to numpy arrays.
Communication Type: hpush://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
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
client.etl("etl-img-to-npy").init_code(transform=transform, dependencies=deps)

to_bck = client.bucket("to-bck")

# Transform bucket with given ETL name
job_id = client.bucket("from-bck").transform(
    etl_name="etl-img-to-npy", to_bck=to_bck, ext={"jpg": "npy"}
)
client.job(job_id).wait()

# load an object from transformed bucket
print(np.frombuffer(to_bck.object("obj-id.npy").get().read_all(), dtype=np.uint8))
