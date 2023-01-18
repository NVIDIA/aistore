"""
ETL to transform images using torchvision.
Communication Type: hpush://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""
from aistore import Client
from torchvision import transforms
from PIL import Image
import io
import torch
import numpy as np

client = Client("http://192.168.49.2:8080")

# cannot apply transforms.PILToTensor() as the expected return type is bytes and not tensor
# if you want to convert it to tensor, return it in "bytes-like" object


def apply_image_transforms(reader, writer):
    transform = transforms.Compose(
        [transforms.Resize(256), transforms.CenterCrop(224), transforms.PILToTensor()]
    )
    for b in reader:
        buffer = io.BytesIO()
        torch.save(transform(Image.open(io.BytesIO(b))), buffer)
        buffer.seek(0)
        writer.write(buffer.read())


# initialize ETL
client.etl().init_code(
    transform=apply_image_transforms,
    etl_name="etl-torchvision",
    dependencies=["Pillow", "torchvision"],
    timeout="10m",
)

# Transform bucket with given ETL id
job_id = client.bucket("from-bck").transform(
    etl_name="etl-img-to-npy", to_bck="to-bck", ext={"jpg": "npy"}
)
client.job().wait_for_job(job_id)

# read the numpy array
np.frombuffer(
    client.bucket("to-bck").object("obj-id.npy").get().read_all(), dtype=np.uint8
)
