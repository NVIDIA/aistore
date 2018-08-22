# coding: utf-8

from helpers.helpers import unify_metrics, update_progress, print_summary
import argparse
import openapi_client

parser = argparse.ArgumentParser(description='Reshard data with random shuffling.')
parser.add_argument('--ext', type=str, default='.tar', help='extension for tarballs (either `.tar` or `.tgz`)')
parser.add_argument('--bucket', type=str, default='dsort-testing', help='bucket where shards will be put')
parser.add_argument('--url', type=str, default='http://localhost:8080', help='proxy url to which requests will be made')
parser.add_argument('--outsize', type=int, default=1024*1024*10, help='size of output of shard')
parser.add_argument('--seed', type=str, default='', help='seed used for shuffle')
parser.add_argument('--input', type=str, default='shard-{0..10}', help='name template for input shard')
parser.add_argument('--output', type=str, default='new-shard-{0000..1000}', help='name template for output shard')
parser.add_argument('--elimit', type=int, default=20, help='limits number of concurrent shards extracted')
parser.add_argument('--climit', type=int, default=20, help='limits number of concurrent shards created')
parser.add_argument('--mem', type=str, default='60%', help='limits maximum of total memory until extraction starts spilling data to the disk, can be 60%% or 10GB')
parser.add_argument('--refresh', type=float, default=0.5, help='metric refresh time (in seconds)')
args = parser.parse_args()

configuration = openapi_client.Configuration()
configuration.debug = False
configuration.host = ('%s/v1' % args.url)
api_client = openapi_client.ApiClient(configuration)

sort_api = openapi_client.api.sort_api.SortApi(api_client)
algorithm = openapi_client.models.SortSpecAlgorithm(
    kind='shuffle',
    seed=args.seed,
)

spec = openapi_client.models.SortSpec(
    bucket=args.bucket,
    local=True,
    extension=args.ext,
    input_format=args.input,
    output_format=args.output,
    algorithm=algorithm,
    shard_size=args.outsize,
    # Below advanced usage
    max_mem_usage=args.mem,
    extract_concurrency_limit=args.elimit,
    create_concurrency_limit=args.climit,
)
sort_uuid = sort_api.start_sort(spec)

last_phase = ''
while True:
    time.sleep(args.refresh)
    metrics = sort_api.get_sort_metrics(sort_uuid)
    progress, phase, finished = unify_metrics(metrics)
    if finished:
        break

    if phase != last_phase:
        print('')
        last_phase = phase

    if phase != '':
        update_progress(phase, progress)

print('')
print('Distributed sort has finished!')
print('\n\n')

time.sleep(1)
print_summary(sort_api.get_sort_metrics(sort_uuid))
