#!/bin/bash

while getopts n:p:b:f: flag
do
	case "$flag" in
		b) BUCKET=${OPTARG};;
	esac
done

print_usage() {
	echo "Usage:"
	echo "./list-bucket-inventory -b BUCKET_NAME"
	echo
	echo "The script displays all enabled inventory collectors for a bucket"
	echo
	echo "  BUCKET_NAME - AWS bucket name for which inventory will be enabled"
}

RED='\033[0;31m'
NC='\033[0m'

if [[ -z "${BUCKET}" ]]; then
	printf "${RED}Error${NC}: bucket name is not defined.\n"
	print_usage
	exit 1
fi

CFG=`aws s3api list-bucket-inventory-configurations --bucket ${BUCKET}`

printf "ID\tPREFIX\tFREQUENCY\n"
OUT=`echo $CFG | jq '"\(.InventoryConfigurationList[] .Id) \(.InventoryConfigurationList[] .Destination .S3BucketDestination .Prefix) \(.InventoryConfigurationList[] .Schedule .Frequency)"'`
OUT=`echo $OUT | tr -d '"'`
OUT=`echo $OUT | tr ' ' "\t"`
printf "$OUT\n"
