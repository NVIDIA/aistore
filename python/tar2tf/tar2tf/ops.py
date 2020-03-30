#
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
#

import json, random
from copy import copy

import tensorflow as tf
import tensorflow_addons as tfa

TYPE = "type"
IMG_TYPE = "image"
EXT_NAME = "ext_name"

# CONVERSIONS


# pylint: disable=unused-variable
class Decode:
    def __init__(self, ext_name):
        validate_ext_name(ext_name)
        self.ext_name = ext_name

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name

    def on_target_allowed(self):
        return True

    def is_type_and_ext_name(self, ty, ext_name):
        return ty == IMG_TYPE and ext_name == self.ext_name

    def do(self, record):
        record[self.ext_name] = tf.io.decode_image(record[self.ext_name])
        return record

    def do_type(self, types):
        types[self.ext_name] = IMG_TYPE
        return types


# pylint: disable=unused-variable
class Convert:
    def __init__(self, ext_name, dst_type):
        validate_ext_name(ext_name)
        self.dst_type = dst_type
        self.ext_name = ext_name

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name

        if self.dst_type == tf.float32:
            yield "dst_type", "float32"
        elif self.dst_type == int:
            yield "dst_type", "int"
        elif self.dst_type == float:
            yield "dst_type", "float"

    def do(self, record):
        record[self.ext_name] = tf.image.convert_image_dtype(record[self.ext_name], self.dst_type)
        return record

    def on_target_allowed(self):
        return False

    def do_type(self, types):
        return types


# pylint: disable=unused-variable
class Resize:
    def __init__(self, ext_name, dst_size):
        validate_ext_name(ext_name)
        self.dst_size = dst_size
        self.ext_name = ext_name

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name
        yield "dst_size", self.dst_size

    def do(self, record):
        record[self.ext_name] = tf.image.resize(record[self.ext_name], self.dst_size)
        return record

    def on_target_allowed(self):
        return True

    def do_type(self, types):
        if types[self.ext_name] != IMG_TYPE:
            raise Exception("Resize expects decoded image")
        return types


class Rename:
    def __init__(self, **kwargs):
        self.dict = {}
        for k in kwargs:
            self.dict[k] = kwargs[k].split(";")

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield "renames", self.dict

    def do(self, record):
        for dst_name in self.dict:
            src_names = self.dict[dst_name]
            for src_name in src_names:
                if src_name in record:
                    record[dst_name] = record[src_name]
                    del record[src_name]
        return record

    def do_type(self, types):
        for dst_name in self.dict:
            src_names = self.dict[dst_name].split(";")
            for src_name in src_names:
                dst_type = types.get(dst_name, None)
                src_type = types.get(src_name, None)

                if dst_type not in [None, src_type]:
                    raise Exception("Rename operation: can't merge entries of different type")
                if src_name in types:
                    types[dst_name] = src_type
        return types

    def on_target_allowed(self):
        return True


class Rotate:
    def __init__(self, ext_name, angle=0):
        self.ext_name = ext_name
        self.angle = angle

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name
        yield "angle", self.angle

    def do(self, record):
        angle = self.angle
        if angle == 0:
            angle = random.random() * 100

        tfa.image.rotate(record[self.ext_name], angle)
        return record

    def on_target_allowed(self):
        return True

    def do_type(self, types):
        if types[self.ext_name] != IMG_TYPE:
            raise Exception("Rotate expects decoded image")
        return types


# pylint: disable=unused-variable
class Func:
    def __init__(self, func, ext_name=None):
        self.func = func
        self.ext_name = ext_name

    def do(self, record):
        return self.func(record)

    def on_target_allowed(self):
        return False

    def res_type(self):
        return None


CONVERSIONS = [Func, Resize, Rename, Decode, Convert, Rotate]

# SELECTIONS


# pylint: disable=unused-variable
class Select:
    def __init__(self, ext_name):
        validate_ext_name(ext_name)
        self.ext_name = ext_name

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name

    def select(self, record):
        return record[self.ext_name]


# pylint: disable=unused-variable
class SelectJSON:
    def __init__(self, ext_name, nested_path):
        validate_ext_name(ext_name)
        self.ext_name = ext_name
        if type(nested_path) == str:
            self.path = [nested_path]
        else:
            self.path = nested_path

    def __iter__(self):
        yield TYPE, self.__class__.__name__
        yield EXT_NAME, self.ext_name
        yield "path", self.path

    def select(self, record):
        d = json.loads(record[self.ext_name])
        val = d
        for field in self.path:
            val = val[field]
        return val


class SelectList:
    def __init__(self, *args):
        self.args = copy(args)

        for i in range(len(self.args)):
            if type(self.args[i]) == str:
                self.args[i] = Select(self.args[i])
            elif type(args[i]) not in SELECTIONS:
                raise Exception("invalid type of {}: {}".format(args[i], type(args[i])))

    def __iter__(self):
        d_args = []
        for a in self.args:
            d_args.append(dict(a))
        yield TYPE, self.__class__.__name__
        yield "list", d_args

    def select(self, record):
        r = []
        for a in self.args:
            r.append(a.do(record))

        return r


class SelectDict:
    def __init__(self, **kwargs):
        self.args = kwargs

        for i in self.args:
            if type(self.args[i]) == str:
                self.args[i] = Select(self.args[i])
            elif type(self.args[i]) not in SELECTIONS:
                raise Exception("invalid type of {}: {}".format(self.args[i], type(self.args[i])))

    def __iter__(self):
        d_args = {}
        for k in self.args:
            d_args[k] = dict(self.args[k])

        yield TYPE, self.__class__.__name__
        yield "dict", d_args

    def select(self, record):
        r = {}
        for a in self.args:
            r[a] = self.args[a].do(record)

        return r


SELECTIONS = [Select, SelectJSON, SelectDict, SelectList]


def validate_ext_name(ext_name):
    if ext_name is None or ext_name == "":
        raise ValueError("tar key can't be empty")


def entries_types(conversions):
    types = {}

    for c in conversions:
        types = c.do_type(types)

    return types
