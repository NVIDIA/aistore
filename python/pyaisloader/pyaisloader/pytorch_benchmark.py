import random
import time

from aistore.pytorch import AISIterDataset, AISMapDataset

from pyaisloader.benchmark import BenchmarkStats, PutGetMixedBenchmark
from pyaisloader.utils.cli_utils import (
    print_in_progress,
    print_sep,
    print_success,
)
from pyaisloader.utils.concurrency_utils import multiworker_deploy
from pyaisloader.utils.stat_utils import combine_results, print_results


class AISDatasetBenchmark(PutGetMixedBenchmark):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, put_pct=0, **kwargs)

    def run(self):
        self._prepare_bucket()
        print_in_progress(f"Performing {self.__class__.__name__} benchmark")
        result = multiworker_deploy(self, self.get_benchmark, (self.duration,))
        print_success(f"Completed {self.__class__.__name__} benchmark")
        result = combine_results(result)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print_results(result, title=self.__class__.__name__)

    def get_benchmark(self, duration):
        dataset = AISMapDataset(
            ais_source_list=self.bucket,
            etl_name=self.etl_name,
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
    def __init__(self, *args, iterations=None, **kwargs):
        super().__init__(*args, put_pct=0, **kwargs)
        self.iterations = iterations

    def run(self):
        self._prepare_bucket()
        print_in_progress(f"Performing {self.__class__.__name__} benchmark")
        result = multiworker_deploy(self, self.get_benchmark, (self.duration,))
        print_success(f"Completed {self.__class__.__name__} benchmark")
        result = combine_results(result)
        if self.cleanup:
            self.clean_up()
        print_sep()
        print_results(result, title=self.__class__.__name__)

    def get_benchmark(self, duration):
        iter_dataset = AISIterDataset(
            ais_source_list=self.bucket,
            etl_name=self.etl_name,
        )
        stats = BenchmarkStats()

        while stats.total_op_time < duration and (
            self.iterations is None or self.iterations > 0
        ):
            op_start = time.time()
            samples = 0
            for sample in iter_dataset:
                samples += 1
                size = len(sample[1])
                stats.update(size, time.time() - op_start)
                op_start = time.time()
                if stats.total_op_time >= duration:
                    break
            if samples == 0:
                break
            if self.iterations is not None:
                self.iterations -= 1

        stats.produce_stats()

        return stats.result
