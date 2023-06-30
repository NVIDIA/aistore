from requests.exceptions import HTTPError

from pyaisloader.utils.print_utils import (
    print_caution,
    print_in_progress,
    print_success,
)
from pyaisloader.utils.random_utils import generate_bytes


def bucket_exists(bucket):
    try:
        bucket.head()
        return True
    except HTTPError:
        return False


def bucket_size(bucket):
    _, bsumm = bucket.info()

    return int(bsumm["TotalSize"]["size_all_present_objs"]) + int(
        bsumm["TotalSize"]["size_all_remote_objs"]
    )


def bucket_obj_count(bucket):
    _, bsumm = bucket.info()

    return int(bsumm["ObjCount"]["obj_count_present"]) + int(
        bsumm["ObjCount"]["obj_count_remote"]
    )


def cleanup(benchmark):
    benchmark.bucket.objects(obj_names=benchmark.objs_created).delete()


def add_one_object(benchmark):
    print_caution("Bucket is empty!")
    print_in_progress("Adding one object")
    content, _ = generate_bytes(1000, 1000)
    obj_name = "initial-object"
    benchmark.bucket.object(obj_name).put_content(content)
    benchmark.objs_created.append(obj_name)
    print_success("Added one object")
