import concurrent.futures
from requests.exceptions import HTTPError


def bucket_exists(bucket):
    try:
        bucket.head()
        return True
    except HTTPError:
        return False


def bucket_size(bck):
    return int(bck.summary()["TotalSize"]["size_on_disk"])


def bucket_obj_count(bck):
    summary = bck.summary()
    return int(summary["ObjCount"]["obj_count_present"]) + int(
        summary["ObjCount"]["obj_count_remote"]
    )


def cleanup(benchmark):
    benchmark.bucket.objects(obj_names=benchmark.objs_created).delete()


def multiworker_deploy(benchmark, worker_function, worker_args=None):
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=benchmark.workers
    ) as executor:
        # Prepare a list of argument tuples for the workers.
        worker_args = [worker_args for _ in range(benchmark.workers)]
        result = list(executor.map(worker_function, *zip(*worker_args)))
    return result
