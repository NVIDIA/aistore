#!/bin/bash

set -e

usage() {
    echo
    echo "Usage: $0 proxy_endpoint  port test_bucket duration";
    echo " where :"
    echo "  proxy_endpoint  : endpoint of the proxy (primary proxy or not, or k8s svc) for client to talk to."
    echo "  port            : proxy port to connect "
    echo "  test_bucket     : the local bucket name to be used for testing"
    echo "  duration        : duration of the test"
    echo
    exit -1
}

if [ $# != 4 ]; then
    usage
fi

proxy_endpoint=$1
proxy_port=$2
bucket=$3
duration=$4


# Bash shell has return value 0-255, so we can't return http code, which can be bigger than 255
# So, we only return true 0 / false (non-zero)here
list_objects() {
    expected_http_code=$1
    # We use the k8s internal service name: devops-ais-proxy
    rc=$(curl -X POST -L  -o /dev/null -s -w "%{http_code}\n" -H 'Content-Type: application/json' \
	      -d '{"action": "listobjects", "value":{"props": "size, checksum"}}' http://$proxy_endpoint:$proxy_port/v1/buckets/$bucket)

    echo "http return code: $rc"
    if [[ $rc != $expected_http_code ]]; then
	return -1
    else
	return 0
    fi
}

echo "Generating a list of random objects to populate a test bucket: $bucket"
cd $AISSRC/../bench/aisloader;
upload_test=$(go run main.go worker.go -ip=$proxy_endpoint \
		 -port=$proxy_port -duration=${duration}s -numworkers=4 -pctput=10 \
		 -local=true -cleanup=false -maxsize=2048 -readertype=rand -bucket=$bucket)

test_status=$(echo $upload_test | grep 'Actual run duration')
rc=$?

# Show the output for debugging
echo "$upload_test"

if [[ $rc != 0 ]]; then
    echo "ERROR: UPLOAD TEST FAILED!!!!!!!!!!" >&2
    echo "$upload_test"
    exit 1
else
    echo "UPLOAD TEST SUCCEEDED!!!!!!!!!!!!!" 
fi
echo

echo "Verify if the list of objects in the bucket: $bucket"

list_objects 200
rc=$?;
if [[ $rc != 0 ]];then
    echo "ERROR: LIST OBJECTS TEST FAILED!!!!!!!!!!!!!!!!!!!!" >&2
    exit 1
else
    echo "LIST OBJECTS TEST SUCCEEDED!!!!!!!!!!!!!!!!!"
fi
echo 

echo "Destroy the bucket: $bucket"
rc=$(curl -X DELETE -s -w "%{http_code}\n" -H 'Content-Type: application/json' \
	  -d '{"action": "destroylb" }' http://$proxy_endpoint:$proxy_port/v1/buckets/$bucket)
if [[ $rc != 200 ]];then
    echo "ERROR: DELETE BUCKET TEST FAILED!!!!!!!!!!!!!!!!!!!!" >&2
    exit 1
else
    echo "DELETE BUCKET TEST SUCCEEDED!!!!!!!!!!!!!!!!!"
fi

echo 

echo "Get list of objects in bucket: $bucket.  Expect there is none"
list_objects 400
rc=$?
if [[ $rc != 0 ]];then
    echo "ERROR: LIST OBJECTS FROM DESTROYED BUCKET TEST FAILED!!!!!!!!!!!!!!!!!!!!" >&2
    exit 1
else
    echo "LIST OBJECTS FROM DESTROYED BUCKET TEST SUCCEEDED!!!!!!!!!!!!!!!!!"
fi
echo 

exit 0
