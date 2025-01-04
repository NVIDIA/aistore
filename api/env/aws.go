// Package env contains environment variables
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package env

import "os"

// use S3_ENDPOINT to globally override the default 'https://s3.amazonaws.com' endpoint
// NOTE: the same can be done on a per-bucket basis, via bucket prop `Extra.AWS.Endpoint`
// (bucket override will always take precedence)

// ditto non-default profile via "AWS_PROFILE" (the default one is called [default])

const (
	AWSEndpoint = "S3_ENDPOINT"
	AWSRegion   = "AWS_REGION"
	AWSProfile  = "AWS_PROFILE"
)

func AwsDefaultRegion() (region string) {
	if region = os.Getenv(AWSRegion); region == "" {
		// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html:
		// "Buckets in region `us-east-1` have a LocationConstraint of null."
		region = "us-east-1"
	}
	return region
}
