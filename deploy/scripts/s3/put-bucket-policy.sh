#!/bin/bash

POLICY_JSON="policy-config.json"

while getopts b: flag
do
	case "$flag" in
		b) BUCKET=${OPTARG};;
	esac
done

print_usage() {
	echo "Usage:"
	echo "./put-bucket-policy -b BUCKET_NAME"
	echo
	echo "The script grants AWS the write access(PUT operation only) to save generated inventory files to the bucket"
	echo
	echo "  BUCKET_NAME - AWS bucket name for which the policy will be enabled"
}

RED='\033[0;31m'
NC='\033[0m'

if [[ -z "${BUCKET}" ]]; then
	printf "${RED}Error${NC}: bucket name is not defined.\n"
	print_usage
	exit 1
fi

cat > "${POLICY_JSON}" <<EOL
{
      "Version": "2012-10-17",
      "Statement": [
        {
            "Sid": "InventoryAndAnalyticsExamplePolicy",
            "Effect": "Allow",
            "Principal": {
                "Service": "s3.amazonaws.com"
            },
            "Action": "s3:PutObject",
            "Resource": [
                "arn:aws:s3:::${BUCKET}/*"
            ],
            "Condition": {
                "ArnLike": {
                    "aws:SourceArn": "arn:aws:s3:::{$BUCKET}"
                }
            }
        }
    ]
}
EOL

aws s3api put-bucket-policy --bucket ${BUCKET} --policy "file://${POLICY_JSON}"
