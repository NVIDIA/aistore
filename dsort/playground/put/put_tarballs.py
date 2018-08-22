# coding: utf-8

import io, tarfile, argparse, os
import six
import openapi_client

parser = argparse.ArgumentParser(description='Generate and put tarballs.')
parser.add_argument('--ext', type=str, default='.tar', help='extension for tarballs (either `.tar` or `.tgz`)')
parser.add_argument('--bucket', type=str, default='dsort-testing', help='bucket where shards will be put')
parser.add_argument('--url', type=str, default='http://localhost:8080', help='proxy url to which requests will be made')
parser.add_argument('--shards', type=int, default=20, help='number of shards to create')
parser.add_argument('--iprefix', type=str, default='shard-', help='prefix of input shard')
parser.add_argument('--fsize', type=int, default=1024, help='single file size (in bytes) inside the shard')
parser.add_argument('--fcount', type=int, default=20, help='number of files inside single shar')
parser.add_argument('--cleanup', type=bool, default=False, help='when true the bucket will be deleted and created so all objects will be fresh')
args = parser.parse_args()

configuration = openapi_client.Configuration()
configuration.debug = False
configuration.host = ('%s/v1' % args.url)
api_client = openapi_client.ApiClient(configuration)

bucket_api = openapi_client.api.bucket_api.BucketApi(api_client)
object_api = openapi_client.api.object_api.ObjectApi(api_client)

# Create local bucket
bucket_names = bucket_api.list_names(loc=True)
if args.bucket in bucket_names.local and args.cleanup:
    input_params = openapi_client.models.InputParameters(openapi_client.models.Actions.DESTROYLB)
    bucket_api.delete(args.bucket, input_params)
    bucket_names.local.remove(args.bucket)

if not args.bucket in bucket_names.local:
    input_params = openapi_client.models.InputParameters(openapi_client.models.Actions.CREATELB)
    bucket_api.perform_operation(args.bucket, input_params)

# Create and send tars
for i in range(0, args.shards):
    out = io.BytesIO()
    object_name = "%s%d%s" % (args.iprefix, i, args.ext)
    with tarfile.open(mode="w", fileobj=out) as tar:
        for j in range(0, args.fcount):
            b = os.urandom(args.fsize)
            t = tarfile.TarInfo("%d.txt" % j)
            t.size = args.fsize
            tar.addfile(t, io.BytesIO(b))

    print('PUT: %s' % object_name)
    if six.PY2:
        object_api.put(args.bucket, object_name, body=out.getvalue())
    else:
        object_api.put(args.bucket, object_name, body=out.getvalue().decode('ISO-8859-1'))

