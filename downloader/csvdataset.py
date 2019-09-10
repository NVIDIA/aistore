from __future__ import print_function
import csv, os, sys, itertools, time
from pprint import pprint
import ais_client
from ais_client.rest import ApiException

bucket_name = raw_input("Which local bucket would you like to save these files to: ")
if not bucket_name:
    print("An empty bucket is not valid.")
    sys.exit()

bucket_api = ais_client.BucketApi()
ais_buckets = bucket_api.list_names().ais
if bucket_name not in ais_buckets:
    print('Bucket "%s" does not exists, creating...' % bucket_name)
    input_params = ais_client.InputParameters(ais_client.Actions.CREATELB)
    bucket_api.perform_operation(bucket_name, input_params)


file_name = raw_input("Please enter the path to the CSV file containing the dataset to download: ")
if not os.path.isfile(file_name):
    print("A valid file was not provided")
    sys.exit()

skip = False
response = raw_input("Is the first row of the csv file just column names? (yes/y or no/n) ").lower()
if response == "yes" or response == "y":
    skip = True

delim = raw_input("Please enter the delimiter used in the provided CSV file (default: \\t): ")
if not delim:
    delim = '\t'

start = int(raw_input("Enter start row (0 indexed): "))
end = int(raw_input("Enter end row (0 indexed): "))

if start > end or start < 0:
    print("Invalid range of rows for csv file")
    sys.exit()

if skip:
    start += 1
    end += 1

col = int(raw_input("Which column contains the download link (0 indexed)? "))
print("\n bucket: %s\n file_name: %s\n delim: \"%s\"\n col: %d\n start: %d\n end: %d\n" % (bucket_name, file_name, delim, col, start, end))

# create an instance of the API class
api_instance = ais_client.DownloadApi()
download_multi = ais_client.DownloadMulti(bucket_name)

objects = []
with open(file_name) as csv_file:
    csv_reader = csv.reader(itertools.islice(csv_file, start, end), delimiter=delim, skipinitialspace=True)
    line_count = start
    for row in csv_reader:
        objects.append(row[col])

download_multi.object_list = objects
resp = api_instance.download_multiple_objects(download_multi)

