import os, io, tarfile

import tensorflow as tf
from humanfriendly import parse_size

from queue import Queue
from inspect import isgeneratorfunction

from .aisapi import AisClient
from .downloadworker import DownloadWorker
from .tarutils import tar_records
from .ops import Select, SelectJSON, Decode, Func, Resize, Convert

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

OPS = [Select, SelectJSON, Decode, Func, Resize, Convert]

OUTPUT_SHAPES = "output_shapes"
OUTPUT_TYPES = "output_types"
MAX_SHARD_SIZE = "max_shard_size"
PATH = "path"


def __bytes_feature(value):
    """Returns a bytes_list from a string / byte."""
    if isinstance(value, type(tf.constant(0))):
        value = value.numpy()
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


def __int64_feature(value):
    """Returns an int64_list from a bool / enum / int / uint."""
    return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


# pylint: disable=unused-variable
def default_record_parser(record):
    keys_to_features = __default_image_desc()
    parsed = tf.io.parse_single_example(record, keys_to_features)

    img = tf.image.decode_jpeg(parsed["image_raw"])
    img = tf.cast(img, tf.float32)
    img = tf.image.resize(img, (224, 224))
    img = tf.reshape(img, shape=[224, 224, 3])
    label = tf.cast(parsed["label"], tf.int32)

    return img, label


def __default_image_desc():
    return {
        "height": tf.io.FixedLenFeature([], tf.int64),
        "width": tf.io.FixedLenFeature([], tf.int64),
        "depth": tf.io.FixedLenFeature([], tf.int64),
        "label": tf.io.FixedLenFeature([], tf.int64),
        "image_raw": tf.io.FixedLenFeature([], tf.string),
    }


def default_record_to_pair(record):
    img = tf.image.convert_image_dtype(tf.io.decode_jpeg(record["jpg"]), tf.float32)
    img = tf.image.resize(img, (224, 224))
    return img, record["cls"]


# Create a dictionary with features that may be relevant.
# this is description of serialization for protobuf
def default_record_to_example(record):
    label = int(record["cls"])
    img = tf.image.decode_jpeg(record["jpg"])

    feature_dict = {
        "height": __int64_feature(img.shape[0]),
        "width": __int64_feature(img.shape[1]),
        "depth": __int64_feature(img.shape[2]),
        "label": __int64_feature(label),
        "image_raw": __bytes_feature(record["jpg"]),
    }

    features = tf.train.Features(feature=feature_dict)
    return tf.train.Example(features=features)


def validate_op(op):
    unknown_type_ex = Exception("unknown operation type. Expected one of {}".format(OPS))
    if type(op) == list:
        for o in op:
            if type(o) not in OPS:
                if type(o) == list:
                    raise Exception("list of operations can't contain another list")
                raise unknown_type_ex
    if type(op) not in OPS:
        raise unknown_type_ex


# pylint: disable=unused-variable
class AisDataset:
    def __init__(
        self,
        bucket,
        proxy_url="localhost:8080",
        val_op=Resize(Convert(Decode("jpg"), tf.float32), (224, 224)),
        label_op=Select("cls"),
        num_workers=4
    ):
        self.proxy_url = proxy_url
        self.proxy_client = AisClient(self.proxy_url, bucket)
        self.bucket = bucket
        self.num_workers = num_workers

        validate_op(val_op)
        validate_op(label_op)

        # how to translate record to value / label
        self.val_op = val_op
        self.label_op = label_op

    def __get_object_names(self, template):
        smap = self.proxy_client.get_cluster_info()

        for k in smap["tmap"]:
            for o in self.proxy_client.get_objects_names(smap["tmap"][k]["intra_data_net"]["direct_url"], template).json():
                yield o

    def __default_path_generator(self, path):
        path_template = current_path = ""
        if "{}" in path:
            path_template = path
            yield path_template.format(0)
        else:
            path_template = "{}" + path
            yield path

        i = 1
        while True:
            yield path_template.format(i)
            i += 1

    # args:
    # record_to_pair - specify how to translate tar record to pair
    # path - place to save TFRecord file. If None, everything done on the fly
    def load_from_tar(self, template, **kwargs):
        accepted_args = [OUTPUT_TYPES, OUTPUT_SHAPES, PATH, MAX_SHARD_SIZE]

        for key in kwargs:
            if key not in accepted_args:
                raise Exception("invalid argument name {}".format(key))

        output_types = kwargs.get(OUTPUT_TYPES, (tf.float32, tf.int32))
        output_shapes = kwargs.get(OUTPUT_SHAPES, (tf.TensorShape([224, 224, 3]), tf.TensorShape([])))

        max_shard_size = kwargs.get(MAX_SHARD_SIZE, 0)
        if type(max_shard_size) == str:
            max_shard_size = parse_size(max_shard_size)
        elif type(max_shard_size) != int:
            raise Exception("{} can be either string or int".format(MAX_SHARD_SIZE))

        if PATH in kwargs and kwargs[PATH] is not None:
            path = kwargs[PATH]
            path_gen = path
            # allow path generators so user can put some more logic to path selection
            if not isgeneratorfunction(path) and type(path) != str:
                raise Exception("path argument has to be either string or generator")
            if type(path) == str:
                path_gen = lambda: self.__default_path_generator(path)

            # return list of paths to created TFRecords
            return self.__record_dataset_from_tar(template, path_gen, default_record_to_example, max_shard_size)

        if max_shard_size != 0:
            raise Exception("{} not supported without {} argument".format(MAX_SHARD_SIZE, PATH))

        return tf.data.Dataset.from_generator(lambda: self.__generator_from_template(template), output_types, output_shapes=output_shapes)

    def __objects_generator(self, template):
        smap = self.proxy_client.get_cluster_info()
        if len(smap["tmap"]) == 0:
            yield from ()
            return

        targets_queue = Queue()
        results_queue = Queue(self.num_workers + 1)  # question: how much data do we want to prefetch?
        # each worker will fetch tars from one target
        for k in smap["tmap"]:
            targets_queue.put(smap["tmap"][k])

        for i in range(self.num_workers):
            worker = DownloadWorker(self.proxy_url, self.bucket, template, targets_queue, results_queue)
            worker.daemon = True  # detach from main process
            worker.start()

        done_cnt = 0
        while True:
            tar_bytes = results_queue.get()
            if tar_bytes is None:
                done_cnt += 1
                if done_cnt == self.num_workers:
                    # all workers have finished and all messages have been read
                    break
            else:
                yield tar_bytes

        targets_queue.join()  # should be immediate as we know that workers have finished

    def __generator_from_template(self, template):
        for obj in self.__objects_generator(template):
            tar_bytes = io.BytesIO(obj)
            tar = tarfile.open(mode="r", fileobj=tar_bytes)
            records = tar_records(tar)
            for k in records:
                val, label = self.operation_to_value(self.val_op, records[k]), self.operation_to_value(self.label_op, records[k])
                yield val, label

            tar.close()

    # path - path where to save record dataset
    def __record_dataset_from_tar(self, template, get_path_gen, record_to_example, max_shard_size):
        path_gen = get_path_gen()
        paths = [next(path_gen)]
        writer = tf.io.TFRecordWriter(paths[0])
        total_size = 0

        for tar_obj in self.__objects_generator(template):
            tar_bytes = io.BytesIO(tar_obj)
            tar = tarfile.open(mode="r", fileobj=tar_bytes)
            records = tar_records(tar)

            for k in records:
                record = records[k]
                tf_example = record_to_example(record)

                serialized = tf_example.SerializeToString()
                writer.write(serialized)
                total_size += len(serialized)
                if total_size > max_shard_size > 0:
                    writer.close()
                    total_size = 0
                    paths.append(next(path_gen))
                    writer = tf.io.TFRecordWriter(paths[-1])

            tar.close()

        writer.close()
        return paths

    def operation_to_value(self, op, record, ext_required=False):
        if type(op) == list:
            d = {}
            for o in op:
                d[op.ext_name] = self.operation_to_value(o, record, True)
            return d

        if ext_required and op.ext_name is None:
            raise Exception("required ext_name, but None found")

        return op.do(record)
