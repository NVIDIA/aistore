#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import logging
import multiprocessing
import os
import random
import shutil
import subprocess
import tarfile
import time

from pathlib import Path
from kubernetes import client as k8s_client
from aistore.sdk.bucket import Bucket


def generate_data(bucket: Bucket, num_tars: int, num_files: int, file_size: int, dest: Path) -> None:
    """
    Generates random binary data, packages it into tar archives, and uploads it to 
    the AIStore bucket. Each tar archive contains 'num_files' files, each of size 
    'file_size'. The tar files are uploaded to simulate a realistic data storage scenario.

    Args:
        bucket (Bucket): The AIStore bucket where data will be uploaded.
        num_tars (int): Number of tar archives to create.
        num_files (int): Number of files in each tar archive.
        file_size (int): Size of each file in bytes.
        dest (Path): Path where generated tar files are temporarily stored.
    """
    dest.mkdir(parents=True, exist_ok=True)

    for i in range(num_tars):
        tarfile_path = dest.joinpath(f"random_data_{i}.tar")

        # Create a tarfile and add generated binary files to it
        with tarfile.open(tarfile_path, "w") as tar:
            for j in range(num_files):
                file_name = f"file_{i}_{j}"
                temp_file_path = dest.joinpath(file_name)
                
                # Write random binary data to each file
                with temp_file_path.open("wb") as file:
                    file.write(os.urandom(file_size))

                tar.add(temp_file_path, arcname=file_name)
                temp_file_path.unlink()
                logging.info(f"Added {file_name} to {tarfile_path.name}")

        # Upload the tar file to the AIStore bucket
        bucket.object(tarfile_path.name).put_file(tarfile_path)
        logging.info(f"Uploaded {tarfile_path.name} to {bucket.provider}://{bucket.name}")
        tarfile_path.unlink()

    shutil.rmtree(dest, ignore_errors=True)


def start_pod_killer(namespace: str, k8s_client: k8s_client.CoreV1Api, pod_prefix: str, pod_kill_interval) -> tuple:
    """
    Starts a separate process to kill random pods that match the given prefix at specified intervals.

    Args:
        namespace (str): Kubernetes namespace where target pods reside.
        k8s_client (k8s_client.CoreV1Api): Kubernetes client for interacting with pods.
        pod_prefix (str): Prefix of the pod name to filter the pods.
        pod_kill_interval (tuple): Interval range (in seconds) between pod killings.
    
    Returns:
        stop_event, pod_killer_process: Event and process for managing pod killer thread.
    """
    stop_event = multiprocessing.Event()
    pod_killer_process = multiprocessing.Process(target=pod_killer, args=(stop_event, namespace, k8s_client, pod_prefix, pod_kill_interval))
    pod_killer_process.start()
    return stop_event, pod_killer_process


def stop_pod_killer(stop_event: multiprocessing.Event, pod_killer_process: multiprocessing.Process):
    """
    Stops the pod killer process by setting the event and joining the process.
    
    Args:
        stop_event (multiprocessing.Event): Event to signal the process to stop.
        pod_killer_process (multiprocessing.Process): The pod killer process to stop.
    """
    stop_event.set()
    pod_killer_process.join()


def pod_killer(stop_event: multiprocessing.Event, namespace: str, k8s_client: k8s_client.CoreV1Api, pod_prefix: str, pod_kill_interval: tuple, initial_delay: int = 5):
    """
    Simulates pod failure by killing a random pod with the given prefix at random intervals.

    Args:
        stop_event (multiprocessing.Event): Event to signal when to stop killing pods.
        namespace (str): Kubernetes namespace where pods are located.
        k8s_client (k8s_client.CoreV1Api): Kubernetes client to interact with the cluster.
        pod_prefix (str): Prefix to match the pod names.
        pod_kill_interval (tuple): Interval range (in seconds) between pod killings.
        initial_delay (int): Initial delay before the first pod kill (in seconds).
    """
    logging.info("Pod killer process started.")

    # Wait for the initial small delay before the first pod kill
    stop_event.wait(initial_delay)
    
    while not stop_event.is_set():
        pod_name = random_pod_name_with_prefix(k8s_client, namespace, pod_prefix)
        if pod_name:
            kill_pod(namespace, pod_name)
        else:
            logging.warning(f"No pods found with prefix {pod_prefix}")

        # Wait for the next pod kill based on the random interval
        stop_event.wait(random.randint(*pod_kill_interval))  # Wait for random time

    logging.info("Pod killer process stopping as stop_event is set.")


def random_pod_name_with_prefix(k8s_client: k8s_client.CoreV1Api, namespace: str, pod_prefix: str) -> str:
    """
    Selects a random pod that matches the given prefix from the Kubernetes namespace.

    Args:
        k8s_client (k8s_client.CoreV1Api): Kubernetes client instance.
        namespace (str): Kubernetes namespace where target pods are located.
        pod_prefix (str): Prefix to match the pod names.
    
    Returns:
        str: Name of the randomly selected pod, or None if no matching pods are found.
    """
    pods = k8s_client.list_namespaced_pod(namespace=namespace).items
    matching_pods = [pod for pod in pods if pod.metadata.name.startswith(pod_prefix)]
    
    if matching_pods:
        return random.choice(matching_pods).metadata.name
    return None


# TODO: Replace w/ Native Kubernetes API
def kill_pod(namespace: str, pod_name: str):
    """
    Deletes the specified pod using a Kubernetes command.

    Args:
        namespace (str): Kubernetes namespace where the pod is located.
        pod_name (str): Name of the pod to delete.
    """
    command = f"kubectl delete pod {pod_name} -n {namespace} --grace-period=0"
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logging.info(f"Pod {pod_name} deleted successfully. Output: {result.stdout.decode().strip()}")


def obj_file_read(bucket, read_size, out_dir: Path, max_resume: int = 10):
    """
    Test reading data from the bucket using ObjectFile with specified chunk size.

    Args:
        bucket (Bucket): AIStore bucket to read data from.
        read_size (int): The size of data chunks to read in bytes.
        out_dir (Path): Directory to store output files.
        max_resume (int, optional): Maximum number of resume attempts for ObjectFile.
    """
    outputs = []
    resume_total = 0
    out_dir.mkdir(parents=True, exist_ok=True)
    start_time = time.time()

    for entry in bucket.list_objects_iter():
        logging.info(f"Starting to read object: {entry.name}")
        outfile_name = out_dir.joinpath(entry.name)
        outputs.append(outfile_name)

        with open(outfile_name, 'wb') as outfile:
            with entry.object.as_file(max_resume=max_resume) as obj_file:
                while True:
                    content = obj_file.read(size=read_size)
                    if not content:
                        logging.info(f"Finished reading object: {entry.name}")
                        break
                    outfile.write(content)
                resume_total += obj_file._resume_total

    elapsed_time = time.time() - start_time
    logging.info(f"Completed object file read for read size {read_size} with {resume_total} total resumes in {elapsed_time:.2f} seconds")
    return outputs
