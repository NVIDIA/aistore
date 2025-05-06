import argparse
import pkg_resources

from pyaisloader.benchmark import PutGetMixedBenchmark, ListBenchmark
from pyaisloader.pytorch_benchmark import AISDatasetBenchmark, AISIterDatasetBenchmark
from pyaisloader.client_config import client

from pyaisloader.utils.parse_utils import parse_size, parse_time
from pyaisloader.utils.print_utils import bold
from pyaisloader.utils.etl_utils import init_etl, cleanup_etls

from aistore.sdk.provider import Provider


VERSION = pkg_resources.require("pyaisloader")[0].version


def prepend_default_arguments(parser):
    parser.add_argument(
        "-b",
        "--bucket",
        type=str,
        required=True,
        help="Bucket (e.g. ais://mybck, s3://mybck, gs://mybck)",
    )

    return parser


def append_default_arguments(parser):
    parser.add_argument(
        "-c",
        "--cleanup",
        action="store_true",
        default=False,
        help="Whether bucket should be destroyed or not upon benchmark completion",
    )
    parser.add_argument(
        "-w", "--workers", type=int, required=True, help="Number of workers"
    )
    parser.add_argument(
        "-e",
        "--etl",
        # choices=["tar2tf", "md5", "echo"],
        required=False,
        help="The built-in ETL transformation (one of tar2tf, md5, or echo) that each object from aisloader GETs undergoes",
    )

    return parser


def main():
    """Parses the command line arguments and instantiates the correct benchmark."""

    parser = argparse.ArgumentParser(description="CLI for running benchmarks.")

    parser.add_argument(
        "--version",
        action="version",
        version=f"pyaisloader {VERSION}",
        help="Show version number and exit",
    )

    subparsers = parser.add_subparsers(
        dest="type",
        title="types",
        description=(
            'Choose a benchmark type. Type "PUT -h", "GET -h", '
            '"MIXED -h", or "LIST -h" for more information about the specific benchmark.'
        ),
    )

    put_parser = subparsers.add_parser(
        "PUT",
        aliases=["put", "P", "p"],
        help="100% PUT benchmark",
        description="This command runs a 100% PUT benchmark.",
    )
    get_parser = subparsers.add_parser(
        "GET",
        aliases=["get", "G", "g"],
        help="100% GET benchmark",
        description="This command runs a 100% GET benchmark.",
    )
    mixed_parser = subparsers.add_parser(
        "MIXED",
        aliases=["mixed", "M", "m"],
        help="MIXED benchmark",
        description="This command runs a MIXED benchmark, with a customizable balance of PUT and GET operations.",
    )
    list_parser = subparsers.add_parser(
        "LIST",
        aliases=["list", "L", "l"],
        help="LIST objects benchmark",
        description="This command runs a LIST benchmark.",
    )
    ais_dataset_parser = subparsers.add_parser(
        "AIS_DATASET",
        aliases=["ais_dataset"],
        help="Map-style AISDataset benchmark",
        description="This command runs an AISDataset benchmark",
    )
    ais_iter_dataset_parser = subparsers.add_parser(
        "AIS_ITER_DATASET",
        aliases=["ais_iter_dataset"],
        help="Iteratable-style AISIterDataset benchmark",
        description="This command runs an AISDataset benchmark",
    )

    put_parser = prepend_default_arguments(put_parser)
    get_parser = prepend_default_arguments(get_parser)
    mixed_parser = prepend_default_arguments(mixed_parser)
    list_parser = prepend_default_arguments(list_parser)
    ais_dataset_parser = prepend_default_arguments(ais_dataset_parser)
    ais_iter_dataset_parser = prepend_default_arguments(ais_iter_dataset_parser)

    put_parser.add_argument(
        "-min",
        "--minsize",
        type=parse_size,
        required=True,
        help="Minimum size of objects to be PUT in bucket during the benchmark",
    )
    put_parser.add_argument(
        "-max",
        "--maxsize",
        type=parse_size,
        required=True,
        help="Maximum size of objects to be PUT in bucket during the benchmark",
    )
    put_parser.add_argument(
        "-s",
        "--totalsize",
        type=parse_size,
        required=False,
        help=(
            "Total size to PUT during the benchmark "
            "(if duration is not satisfied first)"
        ),
    )
    put_parser.add_argument(
        "-d",
        "--duration",
        type=parse_time,
        required=False,
        help="Duration for which benchmark should be run",
    )

    get_parser.add_argument(
        "-min",
        "--minsize",
        type=parse_size,
        required=False,
        help="Minimum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    get_parser.add_argument(
        "-max",
        "--maxsize",
        type=parse_size,
        required=False,
        help="Maximum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    get_parser.add_argument(
        "-s",
        "--totalsize",
        type=parse_size,
        required=False,
        help="Total size to which the bucket should be filled prior to start",
    )
    get_parser.add_argument(
        "-d",
        "--duration",
        type=parse_time,
        required=True,
        help="Duration for which benchmark should be run",
    )

    mixed_parser.add_argument(
        "-p",
        "--putpct",
        type=int,
        default=50,
        help="Percentage for PUT operations in MIXED benchmark",
    )
    mixed_parser.add_argument(
        "-min",
        "--minsize",
        type=parse_size,
        required=True,
        help=("Minimum size of objects to be PUT in bucket during the benchmark "),
    )
    mixed_parser.add_argument(
        "-max",
        "--maxsize",
        type=parse_size,
        required=True,
        help=("Maximum size of objects to be PUT in bucket during the benchmark "),
    )
    mixed_parser.add_argument(
        "-d",
        "--duration",
        type=parse_time,
        required=True,
        help="Duration for which benchmark should be run",
    )

    list_parser.add_argument(
        "-o",
        "--objects",
        type=int,
        help="Number of objects bucket should contain prior to benchmark start",
    )

    ais_dataset_parser.add_argument(
        "-min",
        "--minsize",
        type=parse_size,
        required=False,
        help="Minimum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    ais_dataset_parser.add_argument(
        "-max",
        "--maxsize",
        type=parse_size,
        required=False,
        help="Maximum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    ais_dataset_parser.add_argument(
        "-s",
        "--totalsize",
        type=parse_size,
        required=False,
        help="Total size to which the bucket should be filled prior to start",
    )
    ais_dataset_parser.add_argument(
        "-d",
        "--duration",
        type=parse_time,
        required=True,
        help="Duration for which benchmark should be run",
    )

    ais_iter_dataset_parser.add_argument(
        "-min",
        "--minsize",
        type=parse_size,
        required=False,
        help="Minimum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    ais_iter_dataset_parser.add_argument(
        "-max",
        "--maxsize",
        type=parse_size,
        required=False,
        help="Maximum size of objects to be PUT in bucket (if bucket is smaller than total size)",
    )
    ais_iter_dataset_parser.add_argument(
        "-s",
        "--totalsize",
        type=parse_size,
        required=False,
        help="Total size to which the bucket should be filled prior to start",
    )
    ais_iter_dataset_parser.add_argument(
        "-d",
        "--duration",
        type=parse_time,
        required=True,
        help="Duration for which benchmark should be run",
    )
    ais_iter_dataset_parser.add_argument(
        "-i",
        "--iterations",
        type=int,
        required=False,
        help="Iterations over the dataset for which benchmark should be run",
    )

    put_parser = append_default_arguments(put_parser)
    get_parser = append_default_arguments(get_parser)
    mixed_parser = append_default_arguments(mixed_parser)
    list_parser = append_default_arguments(list_parser)
    ais_dataset_parser = append_default_arguments(ais_dataset_parser)
    ais_iter_dataset_parser = append_default_arguments(ais_iter_dataset_parser)

    args = parser.parse_args()

    if args.type is None:
        print(
            f"\nWelcome to {bold('pyaisloader')}, a CLI for running benchmarks that leverage the AIStore Python SDK. \n\n"
            "Available benchmark types include: PUT, GET, MIXED, and LIST. \n\n"
            "For more details about each benchmark type, use 'pyaisloader [benchmark_type] -h' \nor 'pyaisloader [benchmark_type] --help' "
            "(e.g. for more information about the PUT \nbenchmark, run 'pyaisloader PUT -h' or 'pyaisloader PUT --help').\n"
        )
        return

    # Require that PUT benchmark specifies at least one of --totalsize or --duration
    if args.type.lower() in ["put", "p"]:
        if args.totalsize is None and args.duration is None:
            parser.error("At least one of --totalsize or --duration must be provided.")

    if args.type.lower() in ["get", "g"]:
        if args.totalsize:
            if args.minsize is None or args.maxsize is None:
                parser.error(
                    "If pre-populating bucket, --totalsize, --minsize, and --maxsize are all required."
                )

    # Instantiate bucket and etl objects for the benchmark
    provider, bck_name = args.bucket.split("://")
    bucket_model = client.bucket(bck_name, provider=Provider.parse(provider)).as_model()
    etl = init_etl(client=client, spec_type=args.etl)
    etl_name = etl.name if etl else None
    etl_spec_type = etl.spec_type if etl else None

    benchmark_type = args.type.lower()

    if benchmark_type in [
        "put",
        "get",
        "mixed",
        "p",
        "g",
        "m",
        "ais_dataset",
        "ais_iter_dataset",
    ]:
        if benchmark_type in ["put", "p"]:
            benchmark = PutGetMixedBenchmark(
                put_pct=100,
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                etl_name=etl_name,
                etl_spec_type=etl_spec_type,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        elif benchmark_type in ["get", "g"]:
            benchmark = PutGetMixedBenchmark(
                put_pct=0,
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                etl_name=etl_name,
                etl_spec_type=etl_spec_type,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        elif benchmark_type == "ais_dataset":
            benchmark = AISDatasetBenchmark(
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        elif benchmark_type == "ais_iter_dataset":
            benchmark = AISIterDatasetBenchmark(
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                iterations=args.iterations,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        elif benchmark_type == "ais_dataset":
            benchmark = AISDatasetBenchmark(
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        elif benchmark_type == "ais_iter_dataset":
            benchmark = AISIterDatasetBenchmark(
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                iterations=args.iterations,
                totalsize=args.totalsize,
                bucket_model=bucket_model,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        else:
            benchmark = PutGetMixedBenchmark(
                put_pct=args.putpct,
                minsize=args.minsize,
                maxsize=args.maxsize,
                duration=args.duration,
                bucket_model=bucket_model,
                etl_name=etl_name,
                etl_spec_type=etl_spec_type,
                workers=args.workers,
                cleanup=args.cleanup,
            )
        benchmark.run()
    elif benchmark_type in ["list", "l"]:
        benchmark = ListBenchmark(
            num_objects=args.objects,
            bucket_model=bucket_model,
            workers=args.workers,
            cleanup=args.cleanup,
        )
        benchmark.run()

    cleanup_etls()


if __name__ == "__main__":
    main()
