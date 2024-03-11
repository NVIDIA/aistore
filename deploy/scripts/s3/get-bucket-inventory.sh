#!/bin/bash

while getopts n:p:b:f: flag
do
	case "$flag" in
		b) BUCKET=${OPTARG};;
		n) ID=${OPTARG};;
	esac
done

print_usage() {
	echo "Usage:"
	echo "./get-bucket-inventory -b BUCKET_NAME [-n INVENTORY_NAME]"
	echo
	echo "The script displays detailed information about the selected inventory"
	echo
	echo "  BUCKET_NAME - AWS bucket name for which inventory will be enabled"
	echo "  INVENTORY_NAME - is the inventory ID"
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

CFG=`aws s3api get-bucket-inventory-configuration --bucket ${BUCKET} --id ${ID}`

OUT=`echo $CFG | jq '"ID\t\(.InventoryConfiguration .Id)\nPrefix\t\(.InventoryConfiguration .Destination .S3BucketDestination .Prefix)\nFrequency\t\(.InventoryConfiguration .Schedule .Frequency)\nEnabled\t\(.InventoryConfiguration .IsEnabled)\nFormat\t\(.InventoryConfiguration .Destination .S3BucketDestination .Format)\nFields\t\(.InventoryConfiguration .OptionalFields)"'`
OUT="${OUT:1:${#OUT}-2}"
printf "$OUT\n"
