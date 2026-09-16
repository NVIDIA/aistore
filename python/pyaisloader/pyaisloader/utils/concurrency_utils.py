import concurrent.futures


def multiworker_deploy(benchmark, worker_function, worker_args=None):
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=benchmark.workers
    ) as executor:
        # Prepare a list of argument tuples for the workers.
        worker_args = [worker_args for _ in range(benchmark.workers)]
        result = list(executor.map(worker_function, *zip(*worker_args)))
    return result
