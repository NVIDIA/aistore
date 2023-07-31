import yaml
from pathlib import Path
import json
import humanize


def load_hosts(hosts_file):
    with open(hosts_file, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)
    dgx_nodes = data.get("aisloader_hosts").get("hosts")
    return dgx_nodes.keys()


def read_results(host_list, bench_type, bench_size):
    directory = f"output/{bench_type}/{bench_size}"
    base_name = f"bench-{bench_size}-{bench_type}-"

    host_results = {}
    # load all the output files
    for host in host_list:
        filename = f"{base_name}aistore{host}.json"
        outfile = Path(directory).joinpath(filename)
        with open(str(outfile), "r", encoding="utf-8") as file:
            content = file.read()
            host_results[host] = {"results": json.loads(content)}
    return host_results


# min, avg, max
def get_latencies(bench_res, bench_type):
    final_stats = bench_res[-1].get(bench_type)
    return (
        final_stats.get("min_latency"),
        final_stats.get("latency"),
        final_stats.get("max_latency"),
    )


def get_final_throughput(bench_res, bench_type):
    final_stats = bench_res[-1].get(bench_type)
    return final_stats.get("throughput")


def combine_results(result_dict, bench_type):
    total_lat_min = 10000000000000000
    total_lat_max = 0
    lats = []
    tputs = []
    for host_values in result_dict.values():
        host_res = host_values.get("results")
        min_lat, avg_lat, max_lat = get_latencies(host_res, bench_type)
        if min_lat < total_lat_min:
            total_lat_min = min_lat
        if max_lat > total_lat_max:
            total_lat_max = max_lat
        lats.append(int(avg_lat))
        tputs.append(int(get_final_throughput(host_res, bench_type)))
    avg_lat = sum(lats) / len(lats)
    total_tput = sum(tputs)
    return total_lat_min, avg_lat, total_lat_max, total_tput


def get_natural_time(raw_time):
    units = ["ns", "µs", "ms", "s"]
    unit_index = 0

    while raw_time >= 1000 and unit_index < len(units) - 1:
        raw_time /= 1000
        unit_index += 1

    return f"{raw_time:.2f} {units[unit_index]}"


def pretty_print_res(bench_type, bench_size, res, total_drives):
    lat_min, avg_lat, lat_max, total_tput = res
    print(f"Benchmark results for type {bench_type} with size {bench_size}")
    print("Latencies: ")
    print(
        f"min: {get_natural_time(lat_min)}, avg: {get_natural_time(avg_lat)}, max: {get_natural_time(lat_max)}"
    )
    print(
        f"Cluster average throughput: {humanize.naturalsize(total_tput, binary=True)}/s ({humanize.naturalsize(total_tput/total_drives, binary=True)}/s per drive)"
    )
    print()


def main(configs, args):
    for config in configs:
        bench_type, bench_size = config
        # load hosts from ansible yaml file
        host_list = load_hosts(args.host_file)
        results = read_results(host_list, bench_type, bench_size)
        combined_results = combine_results(results, bench_type)
        pretty_print_res(
            bench_type, bench_size, combined_results, total_drives=args.total_drives
        )


if __name__ == "__main__":
    bench_runs = [
        ("get", "100MB"),
        ("put", "100MB"),
        ("put", "10MB"),
        ("get", "10MB"),
        ("put", "1MB"),
        ("get", "1MB"),
    ]
    import argparse

    parser = argparse.ArgumentParser(
        description="Parses and combines results from multiple aisloader outputs"
    )

    parser.add_argument(
        "--host_file",
        default="inventory.yaml",
        help="Filename containing ansible hosts",
    )
    parser.add_argument(
        "--total_drives",
        type=int,
        default=30,
        help="Number of drives on the AIS cluster being tested",
    )
    parser.add_argument(
        "--aisloader_hosts",
        default="dgx_nodes",
        help="Name of hosts running the aisloader benchmark",
    )

    run_args = parser.parse_args()
    main(bench_runs, run_args)
