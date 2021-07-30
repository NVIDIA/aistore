---
layout: post
title: TENSORFLOW
permalink: tensorflow
redirect_from:
 - tensorflow.md/
 - /docs/tensorflow.md/
---

## Table of Contents

- [Overview](#overview)
- [Examples](#examples)

## Overview

AIS cluster provides out-of-the-box integration with TensorFlow TFRecord format

- Creating TensorFlow datasets from TFRecords stored in AIS cluster with `tf.data.TFRecordDataset` API. See [S3 compatibility docs](s3compat.md)
- Creating TensorFlow datasets from *TAR* files stored in AIS cluster with `tf.data.TFRecordDataset` API.
The conversion is executed remotely, on the fly in the cluster.

## Examples

### Create TensorFlow dataset from TFRecords stored in AIS

```python
import tensorflow as tf
import os

os.environ["S3_ENDPOINT"] = CLUSTER_ENDPOINT

# (...)

train_dataset = tf.data.TFRecordDataset(filenames=[
    "s3://tf/train-1.tfrecord",
    "s3://tf/train-2.tfrecord",
]).map(record_parser).batch(BATCH_SIZE)

# (...)

model.fit(train_dataset, ...)
```

### Create TensorFlow dataset from TARs stored in AIS

```python
import tensorflow as tf
import os

os.environ["S3_ENDPOINT"] = CLUSTER_ENDPOINT

# (...)

# ?uuid query param to convert TAR to a transformed data.

train_dataset = tf.data.TFRecordDataset(filenames=[
    "s3://tf/train-1.tar?uuid=<uuid of tensorflow transformer>",
    "s3://tf/train-2.tar?uuid=<uuid of tensorflow transformer>",
]).map(record_parser).batch(BATCH_SIZE)

# (...)

model.fit(train_dataset, ...)
```
