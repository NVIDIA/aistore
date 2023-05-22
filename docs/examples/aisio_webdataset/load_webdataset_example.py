import os
from pathlib import Path

from aistore.sdk import Client

import webdataset as wds

AIS_ENDPOINT = os.getenv("AIS_ENDPOINT", "http://192.168.49.2:8080")
bucket_name = "images"


def parse_annotations(annotations_file):
    classes = {}
    # Parse the annotations file into a dictionary from file name -> pet class
    with open(annotations_file, "r") as annotations:
        for line in annotations.readlines():
            if line[0] == "#":
                continue
            file_name, pet_class = line.split(" ")[:2]
            classes[file_name] = pet_class
    return classes


def create_sample_generator(image_dir, trimap_dir, annotations_file):
    classes = parse_annotations(annotations_file)
    # Iterate over all image files
    for index, image_file in enumerate(Path(image_dir).glob("*.jpg")):
        # Use the image name to look up class and trimap files and create a sample entry
        sample = create_sample(classes, trimap_dir, index, image_file)
        if sample is None:
            continue
        # Yield optimizes memory by returning a generator that only generates samples as requested
        yield sample


def create_sample(classes, trimap_dir, index, image_file):
    file_name = str(image_file).split("/")[-1].split(".")[0]
    try:
        with open(image_file, "rb") as f:
            image_data = f.read()
        pet_class = classes.get(file_name)
        with open(trimap_dir.joinpath(file_name + ".png"), "rb") as f:
            trimap_data = f.read()
        if not image_data or not pet_class or not trimap_data:
            # Ignore incomplete records
            return None
        return {
            "__key__": "sample_%04d" % index,
            "image.jpg": image_data,
            "cls": pet_class,
            "trimap.png": trimap_data
            }    
    # Ignoring records with any missing files
    except FileNotFoundError as err:
        print(err)
        return None


def load_data(bucket, sample_generator):

    def upload_shard(filename):
        bucket.object(filename).put_file(filename)
        os.unlink(filename)

    # Writes data as tar to disk, uses callback function "post" to upload to AIS and delete
    with wds.ShardWriter("samples-%02d.tar", maxcount=400, post=upload_shard) as writer:
        for sample in sample_generator:
            writer.write(sample)


def view_shuffled_shards():
    objects = client.bucket("images").list_all_objects(prefix="shuffled")
    print([entry.name for entry in objects])


if __name__ == "__main__":
    client = Client(AIS_ENDPOINT)
    image_bucket = client.bucket(bucket_name).create(exist_ok=True)
    base_dir = Path("/home/aaron/pets")
    pet_image_dir = base_dir.joinpath("images")
    pet_trimap_dir = base_dir.joinpath("annotations").joinpath("trimaps")
    pet_annotations_file = base_dir.joinpath("annotations").joinpath("list.txt")
    samples = create_sample_generator(pet_image_dir, pet_trimap_dir, pet_annotations_file)
    load_data(image_bucket, samples)
