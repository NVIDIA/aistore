import io
import os

import torchvision
import webdataset as wds
from PIL import Image
from aistore.sdk import Client
from torch.utils.data import IterableDataset
from torch.utils.data.dataset import T_co

AIS_ENDPOINT = os.getenv("AIS_ENDPOINT")
bucket_name = "images"
etl_name = "wd-transform"


def show_image(image_data):
    with Image.open(io.BytesIO(image_data)) as image:
        image.show()


def wd_etl(object_url):
    def img_to_bytes(img):
        buf = io.BytesIO()
        img = img.convert("RGB")
        img.save(buf, format="JPEG")
        return buf.getvalue()

    def process_trimap(trimap_bytes):
        image = Image.open(io.BytesIO(trimap_bytes))
        preprocessing = torchvision.transforms.Compose(
            [
                torchvision.transforms.CenterCrop(350),
                torchvision.transforms.Lambda(img_to_bytes)
            ]
        )
        return preprocessing(image)

    def process_image(image_bytes):
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        preprocessing = torchvision.transforms.Compose(
            [
                torchvision.transforms.CenterCrop(350),
                torchvision.transforms.ToTensor(),
                # Means and stds from ImageNet
                torchvision.transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
                torchvision.transforms.ToPILImage(),
                torchvision.transforms.Lambda(img_to_bytes),
            ]
        )
        return preprocessing(image)

    # Initialize a WD object from the AIS URL
    dataset = wds.WebDataset(object_url)
    # Map the files for each individual sample to the appropriate processing function
    processed_shard = dataset.map_dict(**{"image.jpg": process_image, "trimap.png": process_trimap})

    # Write the output to a memory buffer and return the value
    buffer = io.BytesIO()
    with wds.TarWriter(fileobj=buffer) as dst:
        for sample in processed_shard:
            dst.write(sample)
    return buffer.getvalue()


def create_wd_etl(client):
    client.etl(etl_name).init_code(
        transform=wd_etl,
        preimported_modules=["torch"],
        dependencies=["webdataset", "pillow", "torch", "torchvision"],
        communication_type="hpull",
        transform_url=True
    )


class LocalTarDataset(IterableDataset):
    """
    Builds a PyTorch IterableDataset from bytes in memory as if was read from a URL by WebDataset. This lets us
    initialize a WebDataset Pipeline without writing to local disk and iterate over each record from a shard.
    """
    def __getitem__(self, index) -> T_co:
        raise NotImplemented

    def __init__(self, input_bytes):
        self.data = [{"url": "input_data", "stream": io.BytesIO(input_bytes)}]

    def __iter__(self):
        files = wds.tariterators.tar_file_expander(self.data)
        samples = wds.tariterators.group_by_keys(files)
        return samples


def read_object_tar(shard_data):
    local_dataset = LocalTarDataset(shard_data)
    sample = next(iter(local_dataset))
    show_image(sample.get('image.jpg'))


def transform_object_inline():
    single_object = client.bucket(bucket_name).object("samples-00.tar")
    # Get object contents with ETL applied
    processed_shard = single_object.get(etl_name=etl_name).read_all()
    read_object_tar(processed_shard)


def transform_bucket_offline():
    dest_bucket = client.bucket("processed-samples").create(exist_ok=True)
    # Transform the entire bucket, placing the output in the destination bucket
    transform_job = client.bucket(bucket_name).transform(to_bck=dest_bucket, etl_name=etl_name)
    client.job(transform_job).wait(verbose=True)
    processed_shard = dest_bucket.object("samples-00.tar").get().read_all()
    read_object_tar(processed_shard)


if __name__ == "__main__":
    client = Client(AIS_ENDPOINT)
    image_bucket = client.bucket(bucket_name)
    create_wd_etl(client)
    transform_object_inline()
    transform_bucket_offline()
