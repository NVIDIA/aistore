import time
import random

from pyaisloader.utils.cli_utils import (
    print_in_progress,
    print_sep,
    print_success,
)
from pyaisloader.utils.concurrency_utils import multiworker_deploy
from pyaisloader.utils.stat_utils import combine_results, print_results
from pyaisloader.client_config import AIS_ENDPOINT
from pyaisloader.benchmark import PutGetMixedBenchmark, BenchmarkStats

from aistore.pytorch import AISMapDataset, AISIterDataset


class AISDatasetBenchmark(PutGetMixedBenchmark):
    def __init__(self, *args, **kwargs):
        super().__init__(put_pct=0, *args, **kwargs)

    def run(self):
        if self.totalsize is not None:
            self._run_prepopulate()
        print_in_progress(f"Performing {self.__class__.__name__} benchmark")
        result = multiworker_deploy(self, self.get_benchmark, (self.duration,))
        print_success(f"Completed {self.__class__.__name__} benchmark")
        result = combine_results(result, self.workers)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print_results(result, title=self.__class__.__name__)

    def get_benchmark(self, duration):
        dataset = AISMapDataset(
            client_url=AIS_ENDPOINT,
            urls_list=f"{self.bucket.provider}://{self.bucket.name}",
        )
        dataset_len = len(dataset)

        stats = BenchmarkStats()

        while stats.total_op_time < duration:
            op_start = time.time()
            content = dataset[random.randint(0, dataset_len - 1)][1]
            latency = time.time() - op_start
            stats.update(len(content), latency)

        stats.produce_stats()

        return stats.result


class AISIterDatasetBenchmark(PutGetMixedBenchmark):
    def __init__(self, iterations=None, *args, **kwargs):
        super().__init__(put_pct=0, *args, **kwargs)
        self.iterations = iterations

    def run(self):
        if self.totalsize is not None:
            self._run_prepopulate()
        print_in_progress(f"Performing {self.__class__.__name__} benchmark")
        result = multiworker_deploy(self, self.get_benchmark, (self.duration,))
        print_success(f"Completed {self.__class__.__name__} benchmark")
        result = combine_results(result, self.workers)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print_results(result, title=self.__class__.__name__)

    def get_benchmark(self, duration):
        iter_dataset = AISIterDataset(
            client_url=ENDPOINT,
            urls_list=f"{self.bucket.provider}://{self.bucket.name}",
        )
        stats = BenchmarkStats()

        while (
            stats.total_op_time < duration
            and self.iterations != None
            and self.iterations > 0
        ):
            op_start = time.time()
            for sample in iter_dataset:
                size = len(sample[1])
                stats.update(size, time.time() - op_start)
                op_start = time.time()
                if stats.total_op_time >= duration:
                    break
            iter_dataset._reset_iterator()
            self.iterations -= 1

        stats.produce_stats()

        return stats.result
