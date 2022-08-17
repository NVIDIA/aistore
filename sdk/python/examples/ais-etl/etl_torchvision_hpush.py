"""
ETL to transform images using torchvision.
Communication Type: hpush://

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
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


def before(context):
    context["before"] = transforms.Compose(
        [transforms.Resize(256), transforms.CenterCrop(224), transforms.PILToTensor()]
    )
    return context


def apply_image_transforms(input_bytes, context):
    transform = context["before"]
    buffer = io.BytesIO()
    torch.save(transform(Image.open(io.BytesIO(input_bytes))), buffer)
    buffer.seek(0)
    return buffer.read()


# no after()

# initialize ETL
client.etl().init_code(
    before=before,
    transform=apply_image_transforms,
    etl_id="etl-torchvision",
    dependencies=["Pillow", "torchvision"],
    timeout="10m",
)

# Transform bucket with given ETL id
xaction_id = client.bucket("from-bck").transform(
    etl_id="etl-img-to-npy", to_bck="to-bck", ext={"jpg": "npy"}
)
client.xaction().wait_for_xaction_finished(xaction_id)

# read the numpy array
np.frombuffer(
    client.bucket("to-bck").object("obj-id.npy").get().read_all(), dtype=np.uint8
)
