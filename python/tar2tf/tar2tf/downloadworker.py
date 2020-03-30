#
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
#

from .aisapi import AisClient
from .ops import entries_types, IMG_TYPE

from queue import Empty
from threading import Thread
import json, base64

import tensorflow as tf


# NOTE: Thread is not state-of-the-art mechanizm (see GIL), but
# threads aren't under global lock when waiting for I/O
# pylint: disable=unused-variable
class TarDownloadWorker(Thread):
    def __init__(self, proxy_url, bucket, template, targets_queue, results_queue):
        Thread.__init__(self)
        self.proxy_url = proxy_url
        self.bucket = bucket
        self.template = template
        self.client = AisClient(self.proxy_url, bucket)

        self.targets_queue = targets_queue
        self.results_queue = results_queue

    def get_object_names(self, target_meta, template):
        for o in self.client.get_objects_names(target_meta["intra_data_net"]["direct_url"], template).json():
            yield o

    def run(self):
        while True:
            try:
                target_meta = self.targets_queue.get_nowait()  # no wait - all targets are put into the queue before starting workers
                for obj_name in self.get_object_names(target_meta, self.template):
                    result = self.client.get_object(obj_name)
                    self.results_queue.put(result)  # waits if queue is full
                self.targets_queue.task_done()
            except Empty:
                break
            except Exception as e:
                print("Unexpected exception {}. Skipping".format(str(e)))
                self.targets_queue.task_done()

        self.results_queue.put(None)  # sign that the worker is done


# downloads transformed samples from cluster
class SampleDownloadWorker(Thread):
    def __init__(self, proxy_url, bucket, target_msg, targets_queue, results_queue, conversions, selections, output_types, output_shapes):
        Thread.__init__(self)
        self.proxy_url = proxy_url
        self.bucket = bucket
        self.target_msg = target_msg
        self.client = AisClient(self.proxy_url, bucket)

        self.targets_queue = targets_queue
        self.results_queue = results_queue

        self.selections = selections
        self.types = entries_types(conversions)

        self.output_shapes = output_shapes
        self.output_types = output_types

    def run(self):
        while True:
            try:
                target_meta = self.targets_queue.get_nowait()  # no wait - all targets are put into the queue before starting workers
                r = self.client.start_target_job_stream(target_meta["intra_data_net"]["direct_url"], self.target_msg)

                for line in r.iter_lines():
                    if not line:
                        continue  # skip empty lines (keep alive etc)
                    res = json.loads(line)
                    # We assume that 'selections' have always 2 elements
                    args = [None] * len(self.selections)
                    for i in range(len(self.selections)):
                        entry_type = self.types.get(self.selections[i].ext_name, None)
                        if entry_type == IMG_TYPE:
                            args[i] = base64.b64decode(res[i])
                            args[i] = tf.io.decode_image(args[i])
                            args[i] = tf.image.convert_image_dtype(args[i], self.output_types[i])
                            continue

                        if self.output_shapes[i].is_compatible_with(tf.TensorShape([None])):
                            # expected array of elements
                            args[i] = tf.io.decode_raw(bytes(res[i], "utf-8"), self.output_types[i])
                        else:
                            # expected single element
                            args[i] = tf.cast(int.from_bytes(bytes(res[i], "utf-8")), self.output_types[i])

                    self.results_queue.put(args)  # waits if queue is full

                self.targets_queue.task_done()
            except Empty:
                break
            except Exception as e:
                print("Unexpected exception {}. Skipping".format(str(e)))
                self.targets_queue.task_done()

        self.results_queue.put(None)  # sign that the worker is done
