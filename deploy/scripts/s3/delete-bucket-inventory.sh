#!/bin/bash

while getopts n:b: flag
do
	case "$flag" in
		n) ID=${OPTARG};;
		b) BUCKET=${OPTARG};;
	esac
done

print_usage() {
	echo "Usage:"
	echo "./delete-bucket-inventory -b BUCKET_NAME [-n INVENTORY_NAME]"
	echo
	echo "The script disables periodical inventory generation for a bucket."
	echo
	echo "  BUCKET_NAME - AWS bucket name for which inventory will be enabled"
	echo "  INVENTORY_NAME - is AWS inventory ID"
}

RED='\033[0;31m'
NC='\033[0m'

if [[ -z "${BUCKET}" ]]; then
	printf "${RED}Error${NC}: bucket name is not defined.\n"
	print_usage
	exit 1
fi

if [[ -z "${ID}" ]]; then
	ID="${BUCKET}"
fi

aws s3api delete-bucket-inventory-configuration --bucket ${BUCKET} --id ${ID}
