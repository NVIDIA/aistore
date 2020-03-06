# TAR2TF - AIS python client

This client provides an easy way to interact with AIS cluster to create tensorflow datasets.

### Start

```console
$ ./setup.sh
$ source venv/bin/activate
$ ais create bucket $BUCKET

...
Put small tars from gsutil ls gs://lpr-gtc2020 into $BUCKET
and adjust imagenet.py with your $BUCKET and objects template
...

$ python examples/imagenet_in_memory.py
```

### Functions

```python
def load_from_tar(self, template, [path, record_to_example=default_record_to_example])
```

Transform tars of images from AIS into tensorflow compatible format

`template` - object names of tars. Bash range syntax like `{0..10}` is supported.  

`path` - destination path to file where TFRecord file should be saved to. If empty or None, all operations are made in memory

`record_to_example` (optional) - should specify how to translate tar record.
Argument of this function is representation of single tar record: python `dict`. 
Tar record is an abstraction for multiple files with exactly the same path, but different extension. 
The argument of function will have `__key__` entry which value is path to record without an extension.
For each extension `e`, dict with have an entry `e` with value the same as contents of relevant file.  

If deault `record_to_example` was used, `default_record_parser` function should be used to
parse `TFRecord` to `tf.Dataset` interface.

`output_types`, `output_shapes` - specify when using custom `record_to_pair` function

![POC TAR2TF](images/poctar2tf.png)

### Examples

1) Create in-memory dataset from tars with names `"train-{0..7}.tar.gz"` in bucket `BUCKET_NAME`
```python
# Create in-memory tensorflow dataset
ais = AisDataset(BUCKET_NAME, PROXY_URL)
train_dataset = ais.load_from_tar("train-{0..3}.tar.gz").shuffle().batch(BATCH_SIZE)
test_dataset = ais.load_from_tar("train-{4..7}.tar.gz").batch(BATCH_SIZE)
# ...
model.fit(train_dataset, epochs=EPOCHS)
```

2) Create tensorflow dataset with intermediate storing `TFRecord` in filesystem
```python
ais = AisDataset(BUCKET_NAME, PROXY_URL)

ais.load_from_tar("train-{0..3}.tar.gz", path="train.record")
train_dataset = tf.data.TFRecordDataset(filenames=["train.record"])
                       .map(default_record_parser)
                       .shuffle(buffer_size=1024)
                       .batch(BATCH_SIZE)
# ...
model.fit(train_dataset, epochs=EPOCHS)
```

3) Create tensorflow dataset in memory with custom tar-record to datapoint translation
```python
from tf.image import convert_image_dtype as convert
from tf.image import resize
from tf.io import decode_jpeg as decode

# Create in-memory tensorflow dataset
ais = AisDataset(BUCKET_NAME, PROXY_URL)
train_dataset = ais.load_from_tar(
    "train-{0..3}.tar.gz",
    record_to_pair=lambda r: (resize(convert(decode(r['jpg']), tf.float32), (224, 224)), r['cls'])
).shuffle().batch(BATCH_SIZE)
test_dataset = ais.load_from_tar("train-{4..7}.tar.gz").batch(BATCH_SIZE)
test_dataset = ais.load_from_tar("train-{4..7}.tar.gz").batch(BATCH_SIZE)
# ...
model.fit(train_dataset, epochs=EPOCHS)

```

The idea is to move as many python code into cluster workload.
The next steps are:
1. Move extracting of tars into `TFRecord` to cluster and define the semantics.
Right now it's all python so there's no translation, but there must be one.
2. Add shuffling of data in the cluster
3. Add augmenting of data in the cluster
