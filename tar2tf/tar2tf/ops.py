import json
import tensorflow as tf


# pylint: disable=unused-variable
class Select:
    def __init__(self, ext_name):
        self.ext_name = ext_name

    def do(self, record):
        return record[self.ext_name]


# pylint: disable=unused-variable
class SelectJSON:
    def __init__(self, ext_name, nested_path):
        self.ext_name = ext_name
        if type(nested_path) == str:
            self.path = [nested_path]
        else:
            self.path = nested_path

    def do(self, record):
        d = json.loads(record[self.ext_name])
        val = d
        for field in self.path:
            val = val[field]
        return val


# pylint: disable=unused-variable
class Decode:
    def __init__(self, ext_name):
        self.ext_name = ext_name

    def do(self, record):
        return tf.io.decode_image(record[self.ext_name])


# pylint: disable=unused-variable
class Convert:
    def __init__(self, what, dst_type):
        self.what = what
        self.dst_type = dst_type
        self.ext_name = what.ext_name

    def do(self, record):
        r = self.what.do(record)
        return tf.image.convert_image_dtype(r, self.dst_type)


# pylint: disable=unused-variable
class Resize:
    def __init__(self, what, dst_size):
        self.what = what
        self.dst_size = dst_size
        self.ext_name = what.ext_name

    def do(self, record):
        return tf.image.resize(self.what.do(record), self.dst_size)


# pylint: disable=unused-variable
class Func:
    def __init__(self, func, ext_name=None):
        self.func = func
        self.ext_name = ext_name

    def do(self, record):
        return self.func(record)
