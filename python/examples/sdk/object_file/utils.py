#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import logging
import os
import multiprocessing
import tarfile
import time
import shutil
import statistics
import subprocess
from pathlib import Path
from typing import List, Tuple
from kubernetes import client as k8s_client, watch as k8s_watch
from aistore.sdk.bucket import Bucket
from aistore.sdk.obj.object import Object
from aistore.sdk.obj.object_reader import ObjectReader


def generate_and_upload_tar(obj: Object, num_files: int, file_size: int, dest: Path):
    """Creates and uploads a tar file to the given object."""
    tar_path = dest / f"{obj.name}.tar"
    with tarfile.open(tar_path, "w") as tar:
        for i in range(num_files):
            file_path = dest / f"file_{i}.bin"
            with open(file_path, "wb") as f:
                f.write(os.urandom(file_size))
            tar.add(file_path, arcname=f"file_{i}.bin")
            os.remove(file_path)
    obj.get_writer().put_file(tar_path)  # Upload to the specified object
    tar_path.unlink()


def create_and_put_object(obj: Object, obj_size: int) -> bytes:
    """Creates an object with random data and puts it in the bucket."""
    data = os.urandom(obj_size)
    obj.get_writer().put_content(data)
    return data


def create_and_put_objects(bucket: Bucket, obj_size: int, num_objects: int) -> None:
    """Creates and puts multiple objects in the bucket."""
    for i in range(num_objects):
        obj = bucket.object(f"random-obj-{i}")
        create_and_put_object(obj, obj_size)


def obj_file_read(object_reader: ObjectReader, read_size: int, buffer_size: int, max_resume: int) -> Tuple[bytes, int]:
    """Reads the object file from the bucket. Returns the downloaded data and total number of resumes."""
    result = bytearray()
    with object_reader.as_file(buffer_size=buffer_size, max_resume=max_resume) as obj_file:
        while True:
            data = obj_file.read(read_size)
            if not data:
                break
            result.extend(data)
    return bytes(result), obj_file._resume_total


def clear_directory(path: Path):
    """Deletes all files and directories in the given path."""
    for item in path.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def calculate_median_throughput(times: List[float], total_data_size: int) -> float:
    """Returns the median throughput from a list of times and total data size."""
    throughputs = [total_data_size / time for time in times]
    return statistics.median(throughputs)


def start_pod_killer(k8s_client: k8s_client.CoreV1Api, namespace: str, pod_name: str, interval_seconds: float) -> multiprocessing.Process:
    """Starts a separate process to kill the specified pod at fixed intervals."""
    pod_killer_process = multiprocessing.Process(
        target=pod_killer,
        args=(k8s_client, namespace, pod_name, interval_seconds)
    )
    pod_killer_process.start()
    return pod_killer_process


def stop_pod_killer(pod_killer_process: multiprocessing.Process):
    """Stops the pod killer process."""
    pod_killer_process.terminate()
    pod_killer_process.join()


def pod_killer(k8s_client: k8s_client.CoreV1Api, namespace: str, pod_name: str, interval_seconds: float):
    """Continuously kills the specified pod at a given interval."""
    logging.info("Pod killer process started.")
    while True:
        time.sleep(interval_seconds)
        kill_pod(k8s_client, namespace, pod_name)


def kill_pod(k8s_client: k8s_client.CoreV1Api, namespace: str, pod_name: str):
    """Deletes a pod via the Kubernetes CLI and waits until the pod is fully restarted and in Running state."""
    try:
        # Use the kubectl CLI to delete the pod
        command = f"kubectl delete pod {pod_name} -n {namespace} --grace-period=0"
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        logging.info(f"Pod {pod_name} deleted successfully. Output: {result.stdout.decode().strip()}")

        # Wait for the pod to be fully running
        wait_for_pod_running(k8s_client, namespace, pod_name)

    except Exception as err:
        logging.error(f"Failed to delete pod {pod_name}: {err}")


def wait_for_pod_running(k8s_client: k8s_client.CoreV1Api, namespace: str, pod_name: str):
    """Waits for a pod to reach 'Running' state with all containers running."""
    logging.info(f"Waiting for pod {pod_name} to restart and reach 'Running' state with all containers running...")

    w = k8s_watch.Watch()
    for event in w.stream(
        func=k8s_client.list_namespaced_pod,
        namespace=namespace,
        field_selector=f"metadata.name={pod_name}",
        timeout_seconds=60
    ):
        pod_status = event['object'].status
        container_statuses = pod_status.container_statuses

        # Check if all containers are in 'Running' state
        if container_statuses:
            all_containers_running = all(c_state.ready for c_state in container_statuses)
            running_containers = sum(1 for c_state in container_statuses if c_state.ready)
            total_containers = len(container_statuses)

            if all_containers_running:
                logging.info(f"Pod {pod_name} is fully running with {running_containers}/{total_containers} containers.")
                w.stop()
                break
            else:
                logging.info(f"Waiting for pod {pod_name}: {running_containers}/{total_containers} containers are running...")
