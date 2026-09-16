from tabulate import tabulate

from pyaisloader.const import BOLD, END
from pyaisloader.utils.parse_utils import format_size, format_time

from pyaisloader.utils.cli_utils import bold, underline


def combine_results(results):
    """
    Combine the results of multiple workers into a single result.

    If some workers have no operations, they are ignored.
    """
    total_ops = sum(r["ops"] for r in results)
    total_time = sum(r["time"] for r in results)
    nonempty_results = [r for r in results if r["ops"] > 0]

    result = {
        "ops": total_ops,
        "bytes": sum(r["bytes"] for r in results),
        "time": total_time,
        "throughput": sum(r["throughput"] for r in results),
        "latency_min": (
            min(r["latency_min"] for r in nonempty_results) if nonempty_results else 0
        ),
        "latency_avg": total_time / total_ops if total_ops else 0,
        "latency_max": (
            max(r["latency_max"] for r in nonempty_results) if nonempty_results else 0
        ),
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
