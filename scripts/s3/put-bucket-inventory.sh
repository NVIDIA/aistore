#!/bin/bash

FREQ="Weekly"
INV_JSON="inv-config.json"
PREFIX=".inventory"

while getopts n:p:b:f: flag
do
	case "$flag" in
		n) ID=${OPTARG};;
		p) PREFIX=${OPTARG};;
		b) BUCKET=${OPTARG};;
		f) FREQ=${OPTARG};;
	esac
done

print_usage() {
	echo "Usage:"
	echo "./put-bucket-inventory -b BUCKET_NAME [-n INVENTORY_NAME] [-p PREFIX] [-f FREQUENCY]"
	echo
	echo "The script enables periodical inventory generation for a bucket. The inventory is saved to a bucket into PREFIX directory daily or weekly depending on the command-line options. Note: it may take up to 48 hours to generate the first inventory"
	echo
	echo "  BUCKET_NAME - AWS bucket name for which inventory will be enabled"
	echo "  INVENTORY_NAME - is AWS inventory ID - arbitrary string that must be unique for a new inventory". Default value is ".inventory"
	echo "  PREFIX - all inventory objects will be placed under s3://BUCKET_NAME/PREFIX/ directory". Default value is the bucket name
	echo "  FREQUENCY - how often inventory is updated. Can be either 'Daily' or 'Weekly'. Default value is 'Weekly'"
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

if [[ -z "${PREFIX}" ]]; then
	printf "${RED}Error${NC}: prefix is not defined.\n"
	print_usage
	exit 1
fi

if [[ "${FREQ}" != "Daily" && "${FREQ}" != "Weekly" ]]; then
	printf "${RED}Error${NC}: invalid value for frequency. Must be one of: ['Daily', 'Weekly']\n"
	print_usage
	exit 1
fi

cat > "${INV_JSON}" <<EOL
{
	"Destination": {
		"S3BucketDestination": {
			"Bucket": "arn:aws:s3:::${BUCKET}",
			"Format": "CSV",
			"Prefix": "${PREFIX}"
		}
	},
	"IsEnabled": true,
	"Id": "${ID}",
	"IncludedObjectVersions": "Current",
	"OptionalFields": ["Size","ETag"],
	"Schedule":{
		"Frequency": "${FREQ}"
	}
}
EOL

aws s3api put-bucket-inventory-configuration --bucket ${BUCKET} --id ${ID} --inventory-configuration "file://${INV_JSON}"
