from tabulate import tabulate

from pyaisloader.const import BOLD, END
from pyaisloader.utils.parse_utils import format_size, format_time

from pyaisloader.utils.cli_utils import bold, underline


def combine_results(results, num_workers):
    result = {
        "ops": sum(r["ops"] for r in results),
        "bytes": sum(r["bytes"] for r in results),
        "time": sum(r["time"] for r in results),
        "throughput": sum(r["throughput"] for r in results),
        "latency_min": min(r["latency_min"] for r in results),
        "latency_avg": sum(r["latency_avg"] for r in results) / num_workers,
        "latency_max": max(r["latency_max"] for r in results),
    }
    return result


def print_results(result, title=None):
    if title is not None:
        print(underline(bold(f"Benchmark Results ({title}):")))

    headers_values = [
        ("# Ops Completed", result["ops"]),
        ("Total Size", format_size(result["bytes"])),
        ("Throughput", f"{format_size(result['throughput'])}/s"),
        ("Latency Min", format_time(result["latency_min"])),
        ("Latency Avg", format_time(result["latency_avg"])),
        ("Latency Max", format_time(result["latency_max"])),
    ]
    table = [
        [f"{BOLD}{name}{END}" for name, _ in headers_values],
        [value for _, value in headers_values],
    ]
    print("\n" + tabulate(table, tablefmt="simple_grid") + "\n")
