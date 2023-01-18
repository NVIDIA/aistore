"""
ETL to transform images using torchvision.
Communication Type: io://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""
from aistore import Client
from torchvision import transforms
from PIL import Image
import io
import sys

client = Client("http://192.168.49.2:8080")

# cannot apply transforms.PILToTensor() as the expected return type is bytes and not tensor
# if you want to convert it to tensor, return it in "bytes-like" object


def apply_image_transforms():
    transform = transforms.Compose(
        [transforms.Resize(256), transforms.CenterCrop(224), transforms.PILToTensor()]
    )
    input_bytes = sys.stdin.buffer.read()
    sys.stdout.buffer.write(transform(Image.open(io.BytesIO(input_bytes))))


deps = ["Pillow", "torchvision"]

# initialize ETL
client.etl().init_code(
    transform=apply_image_transforms,
    etl_name="etl_torchvision_io",
    dependencies=deps,
    communication_type="io",
)

# Transform bucket with given ETL id
job_id = client.bucket("from-bck").transform(
    etl_name="torchvision_io1", to_bck="to-bck", ext={"jpg": "npy"}
)
client.job().wait_for_job(job_id)
