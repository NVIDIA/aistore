---
layout: post
title: "PyTorch: Loading Data from AIStore"
date: 2022-07-11 18:31:24 -0700
author: Abhishek Gaikwad
categories: aistore pytorch sdk python
---

# PyTorch: Loading Data from AIStore

Listing and loading data from AIS buckets (buckets that are not 3rd
party backend-based) and remote cloud buckets (3rd party backend-based
cloud buckets) using
[AISFileLister](https://pytorch.org/data/0.8/generated/torchdata.datapipes.iter.AISFileLister.html)
and
[AISFileLoader](https://pytorch.org/data/0.8/generated/torchdata.datapipes.iter.AISFileLoader.html).

[AIStore](https://github.com/NVIDIA/aistore) (AIS for short) fully supports
Amazon S3, Google Cloud, and Microsoft Azure backends, providing a
unified namespace across multiple connected backends and/or other AIS
clusters, and [more](https://github.com/NVIDIA/aistore#features).

In the following example, we use the [Caltech-256 Object Category
Dataset](https://data.caltech.edu/records/nyy15-4j048) containing 256
object categories and a total of 30607 images stored on an AIS bucket
and the [Microsoft COCO Dataset](https://cocodataset.org/#home) which
has 330K images with over 200K labels of more than 1.5 million object
instances across 80 object categories stored on Google Cloud.

``` {.python}
# Imports
import os
from IPython.display import Image

from torchdata.datapipes.iter import AISFileLister, AISFileLoader, Mapper
```


### Running the AIStore Cluster

[Getting started with
AIS](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md)
will take only a few minutes (prerequisites boil down to having a Linux
with a disk) and can be done either by running a prebuilt [all-in-one
docker image](https://github.com/NVIDIA/aistore/tree/main/deploy) or
directly from the open-source.

To keep this example simple, we will be running a [minimal standalone
docker
deployment](https://github.com/NVIDIA/aistore/blob/main/deploy/prod/docker/single/README.md)
of AIStore.

``` {.python}
# Running the AIStore cluster in a container on port 51080
# Note: The mounted path should have enough space to load the dataset

! docker run -d \
    -p 51080:51080 \
    -v <path_to_gcp_config>.json:/credentials/gcp.json \
    -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/gcp.json" \
    -e AWS_ACCESS_KEY_ID="AWSKEYIDEXAMPLE" \
    -e AWS_SECRET_ACCESS_KEY="AWSSECRETEACCESSKEYEXAMPLE" \
    -e AWS_REGION="us-east-2" \
    -e AIS_BACKEND_PROVIDERS="gcp aws" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```


To create and put objects (dataset) in the bucket, I am going to be
using [AIS
CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md). But we
can also use the [Python
SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore) for the
same.

``` {.python}
! ais config cli set cluster.url=http://localhost:51080

# create bucket using AIS CLI
! ais create caltech256

# put the downloaded dataset in the created AIS bucket
! ais put -r -y <path_to_dataset> ais://caltech256/
```

> OUTPUT:<br>"ais://caltech256" created (see https://github.com/NVIDIA/aistore/blob/main/docs/bucket.md#default-bucket-properties)<br>
> Files to upload:<br>
    EXTENSION	 COUNT	 SIZE<br>
                    1	 3.06KiB<br>
    .jpg		 30607	 1.08GiB<br>
    TOTAL		30608	1.08GiB<br>
    PUT 30608 objects to "ais://caltech256"<br>


### Preloaded dataset

The following assumes that AIS cluster is running and one of its buckets
contains Caltech-256 dataset.

``` {.python}
# list of prefixes which contain data
image_prefix = ['ais://caltech256/']

# Listing all files starting with these prefixes on AIStore 
dp_urls = AISFileLister(url="http://localhost:51080", source_datapipe=image_prefix)

# list first 5 obj urls
list(dp_urls)[:5]
```

>OUTPUT:<br>
    ['ais://caltech256/002.american-flag/002_0001.jpg',<br>
     'ais://caltech256/002.american-flag/002_0002.jpg',<br>
     'ais://caltech256/002.american-flag/002_0003.jpg',<br>
     'ais://caltech256/002.american-flag/002_0004.jpg',<br>
     'ais://caltech256/002.american-flag/002_0005.jpg']



``` {.python}
# loading data using AISFileLoader
dp_files = AISFileLoader(url="http://localhost:51080", source_datapipe=dp_urls)

# check the first obj
url, img = next(iter(dp_files))

print(f"image url: {url}")

# view the image
# Image(data=img.read())
```

>OUTPUT:<br>
    image url: ais://caltech256/002.american-flag/002_0001.jpg

``` {.python}
def collate_sample(data):
    path, image = data
    dir = os.path.split(os.path.dirname(path))[1]
    label_str, cls = dir.split(".")
    return {"path": path, "image": image, "label": int(label_str), "cls": cls}
```

``` {.python}
# passing it further down the pipeline
for _sample in Mapper(dp_files, collate_sample):
    pass
```

### Remote cloud buckets

AIStore supports multiple [remote
backends](https://aiatscale.org/docs/providers). With AIS, accessing
cloud buckets doesn\'t require any additional setup assuming, of course,
that you have the corresponding credentials (to access cloud buckets).

For the following example, AIStore must be built with `--gcp` build tag.

> `--gcp`, `--aws`, and a number of other [build tags](https://github.com/NVIDIA/aistore/blob/main/Makefile) is the mechanism we use to include optional libraries in the [build](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#build-make-and-development-tools).

``` {.python}
# list of prefixes which contain data
gcp_prefix = ['gcp://webdataset-testing/']

# Listing all files starting with these prefixes on AIStore 
gcp_urls = AISFileLister(url="http://localhost:51080", source_datapipe=gcp_prefix)

# list first 5 obj urls
list(gcp_urls)[:5]
```

> OUTPUT:<br>
    ['gcp://webdataset-testing/coco-train2014-seg-000000.tar',<br>
     'gcp://webdataset-testing/coco-train2014-seg-000001.tar',<br>
     'gcp://webdataset-testing/coco-train2014-seg-000002.tar',<br>
     'gcp://webdataset-testing/coco-train2014-seg-000003.tar',<br>
     'gcp://webdataset-testing/coco-train2014-seg-000004.tar']

``` {.python}
dp_files = AISFileLoader(url="http://localhost:51080", source_datapipe=gcp_urls)
```
``` {.python}
for url, file in dp_files.load_from_tar():
    pass
```

### References

-   [AIStore](https://github.com/NVIDIA/aistore)
-   [AIStore Blog](https://aiatscale.org/blog)
-   [AIS CLI](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md)
-   [AIStore Cloud Backend
    Providers](https://aiatscale.org/docs/providers)
-   [AIStore Documentation](https://aiatscale.org/docs)
-   [AIStore Python
    SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore)
-   [Caltech 256 Dataset](https://data.caltech.edu/records/nyy15-4j048)
-   [Getting started with
    AIStore](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md)
-   [Microsoft COCO Dataset](https://cocodataset.org/#home)