from __future__ import print_function
import csv, os, sys, itertools, time
from pprint import pprint
import openapi_client
from openapi_client.rest import ApiException


# bucket = input("Which local bucket would you like to save these files to? ")
# if not bucket:
#     print("An empty bucket is not valid.")
#     sys.exit()

# fileName = input("Please enter the path to the CSV file containing the dataset to download: ")
# if not os.path.isfile(fileName):
#     print("A valid file was not provided")
#     sys.exit()

# skip = False
# yes = input("Is the first row of the csv file just column names? (yes/y or no/n) ").lower()
# if yes == "yes" or yes =="yes":
#     skip = True

# delim = input("Please enter the delimiter used in the provided CSV file. Enter nothing if the delimiter is a comma. ")
# if not delim:
#     delim = ','

# print("Now we'll narrow the range of rows to upload to DFC")
# start = int(input("Enter start row (0 indexed) "))
# end = int(input("Enter end row (0 indexed) "))

# if start > end or start < 0:
#     print("Invalid range of rows for csv file")
#     sys.exit()

# col = int(input("Which column contains the download link (0 indexed)? "))
# if skip:
#     start += 1
#     end += 1
# print("Bucket %s\n fileName %s\n delim \"%s\"\n col %d\n start %d\n end %d\n" % (bucket, fileName, delim, col, start, end))

#/home/dalam/Downloads/imagenet_fall11_urls/fall11_urls.txt
# note imagenet uses tab for delimter
skip = False
bucket = "Test123"
fileName = "/home/dalam/Downloads/imagenet_fall11_urls/fall11_urls.txt"
start = 0
end = 10
delim = '\t'
col = 1

# create an instance of the API class
api_instance = openapi_client.DownloadApi()
download_multi = openapi_client.DownloadMulti()
download_multi.bucket = bucket

# prints all 
with open(fileName) as csv_file:
    csv_reader = csv.reader(itertools.islice(csv_file, start, end + 1), delimiter=delim, skipinitialspace = True)
    line_count = start
    #print(list(csv_reader))
    for row in csv_reader:
        # if line_count >= start and line_count <= end:
        print("Link \"%s\"." % row[col])
        # line_count += 1
        # if line_count > end:
        #     break
    print(f'Processed {line_count} lines.')

