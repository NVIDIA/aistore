// Package env contains environment variables
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package env

// To populate OCI_PRIVATE_KEY with the contents of a PrivateKey .PEM file:
//
//   export OCI_PRIVATE_KEY=$(cat ~/.oci/prikey.pem)
//
// To support the OCI CLI's RC File, we would need to add:
//
//   const OCIRCFilePath = "OCI_CLI_RC_FILE" //  defaults to ~/.oci/oci_cli_rc

const (
	OCIConfigFilePath              = "OCI_CLI_CONFIG_FILE" // defaults to ~/.oci/config
	OCIProfile                     = "OCI_CLI_PROFILE"     // defaults to DEFAULT
	OCITenancyOCID                 = "OCI_TENANCY_OCID"
	OCICompartmentOCID             = "OCI_COMPARTMENT_OCID"
	OCIUserOCID                    = "OCI_USER_OCID"
	OCIRegion                      = "OCI_REGION"
	OCIFingerprint                 = "OCI_FINGERPRINT"
	OCIPrivateKey                  = "OCI_PRIVATE_KEY"
	OCIMaxPageSize                 = "OCI_MAX_PAGE_SIZE"
	OCIMaxDownloadSegmentSize      = "OCI_MAX_DOWNLOAD_SEGMENT_SIZE"
	OCIMultiPartDownloadThreshold  = "OCI_MULTI_PART_DOWNLOAD_THRESHOLD"
	OCIMultiPartDownloadMaxThreads = "OCI_MULTI_PART_DOWNLOAD_MAX_THREADS"
	OCIMaxUploadSegmentSize        = "OCI_MAX_UPLOAD_SEGMENT_SIZE"
	OCIMultiPartUploadThreshold    = "OCI_MULTI_PART_UPLOAD_THRESHOLD"
	OCIMultiPartUploadMaxThreads   = "OCI_MULTI_PART_UPLOAD_MAX_THREADS"
	OCIMultiPartThreadPoolSize     = "OCI_MULTI_PART_THREAD_POOL_SIZE"
)
