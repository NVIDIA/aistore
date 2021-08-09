---
layout: post
title: ETL WEBDATASET
permalink: /tutorials/etl/etl-webdataset
redirect_from:
 - /tutorials/etl/etl_webdataset.md/
 - /docs/tutorials/etl/etl_webdataset.md/
---

# WebDataset ImageNet preprocessing with ETL

In this example, we will see how to use ETL to preprocess the images of ImageNet using [WebDataset](https://github.com/webdataset/webdataset), a PyTorch Dataset implementation providing efficient access to datasets stored in POSIX tar archives.

## Overview

This tutorial consists of couple steps:
1. Prepare AIStore cluster.
2. Prepare dataset.
3. Prepare WebDataset based transform code (ETL).
4. Online Transform dataset on AIStore cluster with ETL.

## Prerequisites

* AIStore cluster deployed on Kubernetes. We recommend following guide below.
  * [Deploy AIStore on local Kuberenetes cluster](https://github.com/NVIDIA/ais-k8s/blob/master/operator/README.md)
  * [Deploy AIStore on the cloud](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md)

## Prepare dataset

Before we start writing code, let's put an example tarball file with ImageNet images to the AIStore.
The tarball we will be using is [imagenet-train-000999.tar](https://storage.googleapis.com/nvdata-imagenet/imagenet-train-000999.tar) which is already in WebDataset friendly format.

```console
$ tar -tvf imagenet-train-000999.tar | head -n 5
-r--r--r-- bigdata/bigdata   3 2020-06-25 11:11 0196495.cls
-r--r--r-- bigdata/bigdata 109671 2020-06-25 11:11 0196495.jpg
-r--r--r-- bigdata/bigdata      3 2020-06-25 11:11 0877232.cls
-r--r--r-- bigdata/bigdata 104484 2020-06-25 11:11 0877232.jpg
-r--r--r-- bigdata/bigdata      3 2020-06-25 11:11 0600062.cls
$ ais bucket create ais://imagenet
"ais://imagenet" bucket created
$ ais object put imagenet-train-000999.tar ais://imagenet
PUT "imagenet-train-000999.tar" into bucket "ais://imagenet"
```

## Prepare ETL code

As we have ImageNet prepared now we need an ETL code that will do the transformation.
Here we will use `io://` communicator type with  `python3` runtime to install `torch`, `torchvision` and `webdataset` packages.

Our transform code will look like this (`code.py`):
```python
# -*- Python -*-

# Perform imagenet-style augmentation and normalization on the shards
# on stdin, returning a new dataset on stdout.

import sys
from torchvision import transforms
import webdataset as wds

normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])


augment = transforms.Compose(
    [
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        normalize,
    ]
)

dataset = wds.WebDataset("-").decode("pil")

sink = wds.TarWriter("-")
for sample in dataset:
    print(sample.get("__key__"), file=sys.stderr)
    sample["npy"] = augment(sample.pop("jpg")).numpy().astype("float16")
    sink.write(sample)

```

The idea here is that we unpack the tarball, process each image and save it as a numpy array into transformed output tarball.

To make sure that the code runs we need to specify required dependencies (`deps.txt`):
```
git+https://github.com/tmbdev/webdataset.git
torch==1.6.0
torchvision==0.7.0
PyYAML==5.4.1
```

## Transform dataset

Now we can build the ETL:
```console
$ ais etl init code --from-file=code.py --deps-file=deps.txt --runtime=python3 --comm-type="io://"
f90r81wR0
$ ais etl object f90r81wR0 imagenet/imagenet-train-000999.tar preprocessed-train.tar
$ tar -tvf preprocessed-train.tar | head -n 6
-r--r--r-- bigdata/bigdata   3 2021-07-20 23:52 0196495.cls
-r--r--r-- bigdata/bigdata 301184 2021-07-20 23:52 0196495.npy
-r--r--r-- bigdata/bigdata      3 2021-07-20 23:52 0877232.cls
-r--r--r-- bigdata/bigdata 301184 2021-07-20 23:52 0877232.npy
-r--r--r-- bigdata/bigdata      3 2021-07-20 23:52 0600062.cls
-r--r--r-- bigdata/bigdata 301184 2021-07-20 23:52 0600062.npy
```

As expected, the new tarball contains transformed images stored as pickled numpy arrays, each occupying `301184` bytes.
