import itertools
import random
import time

from aistore.sdk.const import PROVIDER_AIS

from pyaisloader.utils.bucket_utils import (
    add_one_object,
    bucket_exists,
    bucket_obj_count,
    bucket_size,
)
from pyaisloader.utils.cli_utils import (
    bold,
    confirm_continue,
    print_caution,
    print_in_progress,
    print_sep,
    print_success,
    terminate,
    underline,
)
from pyaisloader.utils.concurrency_utils import multiworker_deploy
from pyaisloader.utils.parse_utils import format_time
from pyaisloader.utils.random_utils import generate_bytes, generate_random_str
from pyaisloader.utils.stat_utils import combine_results, print_results


class BenchmarkStats:
    def __init__(self):
        self.total_op_bytes = 0
        self.total_op_time = 0
        self.total_ops = 0
        self.latencies = []
        self.objs_created = []

    def update(self, size, latency, obj_name=None):
        self.total_ops += 1
        self.total_op_bytes += size
        self.latencies.append(latency)
        self.total_op_time += latency
        if obj_name:
            self.objs_created.append(obj_name)

    def produce_stats(self):
        self.latencies = self.latencies or [0]  # To avoid division by zero
        self.result = {
            "ops": self.total_ops,
            "bytes": self.total_op_bytes,
            "time": self.total_op_time,
            "throughput": self.total_op_bytes / self.total_op_time
            if self.total_op_time != 0
            else 0,
            "latency_min": min(self.latencies),
            "latency_avg": sum(self.latencies) / len(self.latencies),
            "latency_max": max(self.latencies),
        }


class Benchmark:
    """Abstract class for all benchmarks"""

    def __init__(self, bucket, workers, cleanup):
        self.bucket = bucket
        self.workers = workers
        self.cleanup = cleanup
        self.objs_created = []
        # Track for intelligent clean-up (deletes bucket if bucket was created by benchmark, otherwise only deletes objects in bucket created by benchmark)
        self.bck_created = False

        self.setup()

    def run(self, *args, **kwargs):
        raise NotImplementedError("This method should be implemented by subclasses.")

    def setup(self):
        if self.bucket.provider != PROVIDER_AIS:  # Cloud Bucket
            print_caution("You are currently operating on a cloud storage bucket.")
            confirm_continue()
            if not bucket_exists(
                self.bucket
            ):  # Cloud buckets that don't exist are not permitted
                terminate(
                    "Cloud bucket "
                    + bold(f"{self.bucket.provider}://{self.bucket.name}")
                    + " does not exist and AIStore Python SDK does not yet support cloud bucket creation (re-run with existing cloud bucket)."
                )

        if bucket_exists(self.bucket):
            print_caution(
                "The bucket "
                + bold(f"{self.bucket.provider}://{self.bucket.name}")
                + " already exists."
            )
            confirm_continue()
        else:
            print_in_progress(
                "Creating bucket "
                + bold(f"{self.bucket.provider}://{self.bucket.name}")
            )
            self.bucket.create()
            self.bck_created = True
            print_success(
                "Created bucket " + bold(f"{self.bucket.provider}://{self.bucket.name}")
            )

    def prepopulate(self, type_list=False):
        prefix = (
            "PREPOP-" + generate_random_str() + "-"
        )  # Each worker with unique prefix
        objs_created = []
        prepopulated_bytes = 0

        if type_list:
            for suffix in range(self.target):
                _, objs_created = self.__prepopulate_h(objs_created, prefix, suffix)
        else:
            suffix = 0
            while prepopulated_bytes < self.target:
                size, objs_created = self.__prepopulate_h(objs_created, prefix, suffix)
                prepopulated_bytes += size
                suffix += 1

        return objs_created

    def __prepopulate_h(self, objs_created, prefix, suffix):
        content, size = generate_bytes(self.minsize, self.maxsize)
        obj = self.bucket.object(prefix + (str(suffix)))
        obj.put_content(content)
        objs_created.append(obj.name)
        return size, objs_created

    def clean_up(self, new=True):
        if new:
            print_in_progress("Cleaning up", "\U0001F9F9")

        if not self.bck_created and not self.objs_created:
            print_caution("Nothing to delete! Skipping clean-up...")
            return

        if self.bck_created:
            msg = (
                "bucket " + bold(f"{self.bucket.provider}://{self.bucket.name}") + " ? "
            )
        else:
            msg = (
                bold(f"{len(self.objs_created)}")
                + " objects created by the benchmark (and pre-population) in "
                + bold(f"{self.bucket.provider}://{self.bucket.name}")
                + " ? "
            )

        decision = input(
            "\n"
            + "Would you like to proceed w/ deletion of "
            + msg
            + bold("(Y/N)")
            + ": "
        )

        if decision.lower() in ["n", "no"]:
            print_caution("Skipping clean-up...")
            return
        if decision.lower() in ["y", "yes"]:
            if self.bck_created:
                self.bucket.delete()
            else:
                self.bucket.objects(obj_names=self.objs_created).delete()
            print_success("Completed clean-up")
        else:
            self.clean_up(False)


class PutGetMixedBenchmark(Benchmark):
    def __init__(
        self,
        put_pct,
        minsize=None,
        maxsize=None,
        duration=None,
        totalsize=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.put_pct = put_pct
        self.duration = duration
        self.totalsize = totalsize
        self.minsize = minsize
        self.maxsize = maxsize

    def run(self):
        if self.put_pct == 100:
            self.__run_put()
        elif self.put_pct == 0:
            self.__run_prepopulate()
            self.__run_get()
        else:
            self.__run_mixed()

    def __run_put(self):
        totalsize = None if self.totalsize is None else (self.totalsize // self.workers)
        print_in_progress("Performing PUT benchmark")
        results = multiworker_deploy(
            self, self.put_benchmark, (self.duration, totalsize)
        )
        print_success("Completed PUT benchmark")
        result = []
        for worker_result, worker_objs_created in results:
            result.append(worker_result)
            self.objs_created.extend(worker_objs_created)
        result = combine_results(result, self.workers)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print("\n" + underline(bold("Benchmark Results (100% PUT):")))
        print_results(result)

    def __run_get(self):
        if bucket_obj_count(self.bucket) == 0:
            add_one_object(self)
        self.get_objs_queue = self.bucket.list_all_objects()
        print_in_progress("Performing GET benchmark")
        result = multiworker_deploy(self, self.get_benchmark, (self.duration,))
        print_success("Completed GET benchmark")
        result = combine_results(result, self.workers)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print("\n" + underline(bold("Benchmark Results (100% GET):")))
        print_results(result)

    def __run_mixed(self):
        if bucket_obj_count(self.bucket) == 0:
            add_one_object(self)
        print_in_progress("Performing MIXED benchmark")
        result = multiworker_deploy(self, self.mixed_benchmark, (self.duration,))
        print_success("Completed MIXED benchmark")
        workers_objs_created = [
            obj for worker_result in result for obj in worker_result[2]
        ]
        self.objs_created.extend(workers_objs_created)
        results_put = [res[0] for res in result]
        results_get = [res[1] for res in result]
        result_put = combine_results(results_put, self.workers)
        result_get = combine_results(results_get, self.workers)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print("\n" + underline(bold("Benchmark Results for PUT operations:")))
        print_results(result_put)
        print("\n" + underline(bold("Benchmark Results for GET operations:")))
        print_results(result_get)

    def __run_prepopulate(self):
        if self.totalsize is not None:
            curr_bck_size = bucket_size(self.bucket)
            if curr_bck_size < self.totalsize:
                print_in_progress("Pre-Populating Bucket")
                self.target = ((self.totalsize) - curr_bck_size) // self.workers
                result = multiworker_deploy(
                    self,
                    self.prepopulate,
                    (False,),
                )
                self.objs_created.extend(list(itertools.chain(*result)))
                remaining_bytes = ((self.totalsize) - curr_bck_size) % self.workers
                if remaining_bytes != 0:  #
                    self.target = remaining_bytes
                    objs_created = self.prepopulate(type_list=False)
                    self.objs_created.extend(objs_created)
                print_success("Completed Pre-Population")

    def put_benchmark(self, duration, totalsize):  # Done
        prefix = generate_random_str()  # Each worker with unique prefix
        pstats = BenchmarkStats()

        if duration and totalsize:  # Time/Size Based
            while pstats.total_op_time < duration and pstats.total_op_bytes < totalsize:
                self.__put_benchmark_h(pstats, prefix, pstats.total_ops)
        elif duration:  # Time Based
            while pstats.total_op_time < duration:
                self.__put_benchmark_h(pstats, prefix, pstats.total_ops)
        elif totalsize:  # Size Based
            while pstats.total_op_bytes < totalsize:
                size, latency, obj = self.__put_benchmark_h(
                    pstats, prefix, pstats.total_ops
                )
                pstats.objs_created.append(obj.name)
                pstats.update(size, latency, obj.name)

        pstats.produce_stats()

        return pstats.result, pstats.objs_created

    def __put_benchmark_h(self, stats, prefix, suffix):  # Done
        content, size = generate_bytes(self.minsize, self.maxsize)
        obj = self.bucket.object(prefix + str(suffix))
        op_start = time.time()
        obj.put_content(content)
        op_end = time.time()
        latency = op_end - op_start
        stats.objs_created.append(obj.name)
        stats.update(size, latency, obj.name)

        return obj

    def get_benchmark(self, duration):  # Done
        gstats = BenchmarkStats()

        while gstats.total_op_time < duration:
            self.__get_benchmark_h(gstats, self.get_objs_queue)

        gstats.produce_stats()

        return gstats.result

    def __get_benchmark_h(self, stats, objs):  # Done
        op_start = time.time()
        content = self.bucket.object(random.choice(objs).name).get()
        content.read_all()
        op_end = time.time()
        latency = op_end - op_start
        stats.update(content.attributes.size, latency)

    def mixed_benchmark(self, duration):  # Done
        prefix = generate_random_str()  # Each worker with unique prefix

        gstats = BenchmarkStats()
        pstats = BenchmarkStats()

        objs = [obj.object for obj in self.bucket.list_all_objects()]

        while pstats.total_op_time + gstats.total_op_time < duration:
            # Choose whether to perform a PUT or a GET operation
            if random.randint(0, 100) < self.put_pct:
                obj = self.__put_benchmark_h(pstats, prefix, pstats.total_ops)
                objs.append(obj)
            else:
                self.__get_benchmark_h(gstats, objs)

        gstats.produce_stats()
        pstats.produce_stats()

        return pstats.result, gstats.result, pstats.objs_created


class ListBenchmark(Benchmark):
    def __init__(self, num_objects=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num_objects = num_objects
        self.minsize = 1000
        self.maxsize = 1000

        # Pre-Population
        curr_bck_count = bucket_obj_count(self.bucket)
        if self.num_objects and self.num_objects > curr_bck_count:
            print_in_progress("Pre-Populating Bucket")
            self.target = (self.num_objects - curr_bck_count) // self.workers
            result = multiworker_deploy(
                self,
                self.prepopulate,
                (True,),
            )
            self.objs_created.extend(list(itertools.chain(*result)))
            if ((self.num_objects - curr_bck_count) % self.workers) != 0:
                self.target = (self.num_objects - curr_bck_count) % self.workers
                objs_created = self.prepopulate(type_list=True)
                self.objs_created.extend(objs_created)
            print_success("Completed Pre-Population")

    def __print_results(self):
        num_listed_objs = len(self.listed_objs)
        if num_listed_objs != 0:
            print_sep()
            print(
                "\n"
                + underline(
                    bold(f"Benchmark Results (LIST {num_listed_objs} Objects):")
                )
            )
            print(
                "\n"
                + "The benchmark took approximately "
                + bold(format_time(self.benchmark_time))
                + " and listed "
                + bold(str(num_listed_objs))
                + " objects.\n"
            )
        else:
            terminate(
                f"The bucket {self.bucket.provider}://{self.bucket.name} is empty. Please populate the bucket before running the benchmark or use the option --num-objects (or -n)."
            )

    def run(self):
        self.listed_objs = []

        # Start benchmark
        start_time = time.time()
        print_in_progress("Performing LIST benchmark")
        self.listed_objs = self.bucket.list_all_objects()
        print_success("Completed LIST benchmark")
        end_time = time.time()
        self.benchmark_time = end_time - start_time

        if self.cleanup:
            self.clean_up()

        self.__print_results()
