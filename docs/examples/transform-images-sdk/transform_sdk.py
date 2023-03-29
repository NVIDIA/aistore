import os
import io
import sys
from PIL import Image
from torchvision import transforms
import torch

from aistore.pytorch import AISDataset
from aistore.sdk import Client
from aistore.sdk.multiobj import ObjectRange

AISTORE_ENDPOINT = os.getenv("AIS_ENDPOINT", "http://192.168.49.2:8080")
client = Client(AISTORE_ENDPOINT)


def etl():
    def img_to_bytes(img):
        buf = io.BytesIO()
        img = img.convert('RGB')
        img.save(buf, format='JPEG')
        return buf.getvalue()

    input_bytes = sys.stdin.buffer.read()
    image = Image.open(io.BytesIO(input_bytes)).convert('RGB')
    preprocessing = transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        transforms.ToPILImage(),
        transforms.Lambda(img_to_bytes),
    ])
    processed_bytes = preprocessing(image)
    sys.stdout.buffer.write(processed_bytes)


def show_image(image_data):
    with Image.open(io.BytesIO(image_data)) as image:
        image.show()


def load_data():
    # First, let's create a bucket and put the data into ais
    bucket = client.bucket("images").create()
    bucket.put_files("images", pattern="*.jpg")
    # Show a random (non-transformed) image from the dataset
    image_data = bucket.object("Bengal_171.jpg").get().read_all()
    show_image(image_data)


def create_etl():
    client.etl().init_code(etl_name="transform-images",
                           transform=etl,
                           dependencies=["torchvision"],
                           communication_type="io")


def show_etl():
    print(client.etl().list())
    print(client.etl().view("transform-images"))


def get_with_etl():
    transformed_data = client.bucket("images").object("Bengal_171.jpg").get(etl_name="transform-images").read_all()
    show_image(transformed_data)


def etl_bucket():
    dest_bucket = client.bucket("transformed-images").create()
    transform_job = client.bucket("images").transform(etl_name="transform-images", to_bck=dest_bucket)
    client.job(transform_job).wait()
    print(entry.name for entry in dest_bucket.list_all_objects())


def etl_group():
    dest_bucket = client.bucket("transformed-selected-images").create()
    # Select a range of objects from the source bucket
    object_range = ObjectRange(min_index=0, max_index=100, prefix="Bengal_", suffix=".jpg")
    object_group = client.bucket("images").objects(obj_range=object_range)
    transform_job = object_group.transform(etl_name="transform-images", to_bck=dest_bucket)
    client.job(transform_job).wait_for_idle(timeout=300)
    print([entry.name for entry in dest_bucket.list_all_objects()])


def create_dataloader():
    # Construct a dataset and dataloader to read data from the transformed bucket
    dataset = AISDataset(AISTORE_ENDPOINT, "ais://transformed-images")
    train_loader = torch.utils.data.DataLoader(dataset, shuffle=True)
    return train_loader


if __name__ == "__main__":
    load_data()
    create_etl()
    show_etl()
    get_with_etl()
    etl_bucket()
    etl_group()
    data_loader = create_dataloader()
