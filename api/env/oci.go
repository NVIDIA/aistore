// Package env contains environment variables
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package env

// To populate OCI_PRIVATE_KEY with the contents of a PrivateKey .PEM file:
//
//   export OCI_PRIVATE_KEY=$(cat ~/.oci/prikey.pem)

var (
	OCI = struct {
		TenancyOCID                 string
		CompartmentOCID             string
		UserOCID                    string
		Region                      string
		Fingerprint                 string
		PrivateKey                  string
		MaxPageSize                 string
		MaxDownloadSegmentSize      string
		MultiPartDownloadThreshold  string
		MultiPartDownloadMaxThreads string
		MaxUploadSegmentSize        string
		MultiPartUploadThreshold    string
		MultiPartUploadMaxThreads   string
	}{
		TenancyOCID:                 "OCI_TENANCY_OCID",
		CompartmentOCID:             "OCI_COMPARTMENT_OCID",
		UserOCID:                    "OCI_USER_OCID",
		Region:                      "OCI_REGION",
		Fingerprint:                 "OCI_FINGERPRINT",
		PrivateKey:                  "OCI_PRIVATE_KEY",
		MaxPageSize:                 "OCI_MAX_PAGE_SIZE",
		MaxDownloadSegmentSize:      "OCI_MAX_DOWNLOAD_SEGMENT_SIZE",
		MultiPartDownloadThreshold:  "OCI_MULTI_PART_DOWNLOAD_THRESHOLD",
		MultiPartDownloadMaxThreads: "OCI_MULTI_PART_DOWNLOAD_MAX_THREADS",
		MaxUploadSegmentSize:        "OCI_MAX_UPLOAD_SEGMENT_SIZE",
		MultiPartUploadThreshold:    "OCI_MULTI_PART_UPLOAD_THRESHOLD",
		MultiPartUploadMaxThreads:   "OCI_MULTI_PART_UPLOAD_MAX_THREADS",
	}
)
