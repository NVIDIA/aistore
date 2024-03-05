// Package env contains environment variables
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package env

import "os"

func AwsDefaultRegion() (region string) {
	if region = os.Getenv(AWS.Region); region == "" {
		// from https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html:
		// "Buckets in region `us-east-1` have a LocationConstraint of null."
		region = "us-east-1"
	}
	return region
}

func BucketInventory() (inv string) {
	if inv = os.Getenv(AWS.Inventory); inv == "" {
		inv = "inv-all" // TODO -- FIXME: change to ".inventory"
	}
	return inv
}

// use S3_ENDPOINT to globally override the default 'https://s3.amazonaws.com' endpoint
// NOTE: the same can be done on a per-bucket basis, via bucket prop `Extra.AWS.Endpoint`
// (bucket override will always take precedence)

// ditto non-default profile via "AWS_PROFILE" (the default one is called [default])

var (
	AWS = struct {
		Endpoint  string
		Region    string
		Profile   string
		Inventory string
	}{
		Endpoint:  "S3_ENDPOINT",
		Region:    "AWS_REGION",
		Profile:   "AWS_PROFILE",
		Inventory: "S3_BUCKET_INVENTORY",
	}
)
