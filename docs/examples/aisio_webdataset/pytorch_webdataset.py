import os

import torchvision

from aistore.pytorch import AISSourceLister
from aistore.sdk import Client
import webdataset as wds


AIS_ENDPOINT = os.getenv("AIS_ENDPOINT")
client = Client(AIS_ENDPOINT)
bucket_name = "images"
etl_name = "wd-transform"


def show_image_tensor(image_data):
    transform = torchvision.transforms.ToPILImage()
    image = transform(image_data)
    image.show()
    

def create_dataset() -> wds.WebDataset:
    bucket = client.bucket(bucket_name)
    # Get a list of urls for each object in AIS, with ETL applied, converted to the format WebDataset expects
    sources = AISSourceLister(ais_sources=[bucket], etl_name=etl_name).map(lambda source_url: {"url": source_url})\
        .shuffle()
    # Load shuffled list of transformed shards into WebDataset pipeline
    dataset = wds.WebDataset(sources)
    # Shuffle samples and apply built-in webdataset decoder for image files
    dataset = dataset.shuffle(size=1000).decode("torchrgb")
    # Return iterator over samples as tuples in batches
    return dataset.to_tuple("cls", "image.jpg", "trimap.png").batched(16)


def create_dataloader(dataset) -> wds.WebLoader:
    loader = wds.WebLoader(dataset, num_workers=4, batch_size=None)
    return loader.unbatched().shuffle(1000).batched(64)


def view_data(dataloader):
    # Get the first batch
    batch = next(iter(dataloader))
    classes, images, trimaps = batch
    # Result is a set of tensors with the first dimension being the batch size
    print(classes.shape, images.shape, trimaps.shape)
    # View the first images in the first batch
    show_image_tensor(images[0])
    show_image_tensor(trimaps[0])


if __name__ == '__main__':
    wd_dataset = create_dataset()
    wd_dataloader = create_dataloader(wd_dataset)
    view_data(wd_dataloader)
    first_batch = next(iter(wd_dataloader))
    classes, images, trimaps = first_batch
