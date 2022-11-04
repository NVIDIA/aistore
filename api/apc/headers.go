// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// For standard and provider-specific HTTP headers, see cmn/cos/const_http.go

const HdrError = "Hdr-Error"

// Header Key conventions:
//   - starts with a prefix "ais-",
//   - all words separated with "-": no dots and underscores.
const (
	HeaderPrefix = "ais-"

	// Bucket props headers.
	HdrBucketProps      = HeaderPrefix + "bucket-props"       // => cmn.BucketProps
	HdrBucketSumm       = HeaderPrefix + "bucket-summ"        // => cmn.BsummResult (see also: QparamFltPresence)
	HdrBucketVerEnabled = HeaderPrefix + "versioning-enabled" // Enable/disable object versioning in a bucket.
	HdrBucketCreated    = HeaderPrefix + "created"            // Bucket creation time.
	HdrBackendProvider  = HeaderPrefix + "provider"           // ProviderAmazon et al. - see cmn/bck.go.

	// including BucketProps.Extra.AWS
	HdrS3Region   = HeaderPrefix + "cloud_region"
	HdrS3Endpoint = HeaderPrefix + "endpoint"

	// including BucketProps.Extra.HTTP
	HdrOrigURLBck = HeaderPrefix + "original-url"

	// remote AIS
	HdrRemAisUUID  = HeaderPrefix + "remote-ais-uuid"
	HdrRemAisAlias = HeaderPrefix + "remote-ais-alias"
	HdrRemAisURL   = HeaderPrefix + "remote-ais-url"

	HdrRemoteOffline = HeaderPrefix + "remote-offline" // When accessing cached remote bucket with no backend connectivity.

	// Object props headers
	HdrObjCksumType = HeaderPrefix + "checksum-type"  // Checksum type, one of SupportedChecksums().
	HdrObjCksumVal  = HeaderPrefix + "checksum-value" // Checksum value.
	HdrObjAtime     = HeaderPrefix + "atime"          // Object access time.
	HdrObjCustomMD  = HeaderPrefix + "custom-md"      // Object custom metadata.
	HdrObjVersion   = HeaderPrefix + "version"        // Object version/generation - ais or cloud.

	// Archive filename and format (mime type)
	HdrArchpath = HeaderPrefix + "archpath"
	HdrArchmime = HeaderPrefix + "archmime"

	// Append object header.
	HdrAppendHandle = HeaderPrefix + "append-handle"

	// Query objects handle header.
	HdrHandle = HeaderPrefix + "query-handle"

	// Reverse proxy headers.
	HdrNodeID  = HeaderPrefix + "node-id"
	HdrNodeURL = HeaderPrefix + "node-url"

	// uptimes, respectively
	HdrNodeUptime    = HeaderPrefix + "node-uptime"
	HdrClusterUptime = HeaderPrefix + "cluster-uptime"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internally used headers
const (
	HdrCallerID          = HeaderPrefix + "caller-id" // Marker of intra-cluster request.
	HdrT2TPutterID       = HeaderPrefix + "putter-id" // DaemonID of the target that performs intra-cluster PUT
	HdrCallerName        = HeaderPrefix + "caller-name"
	HdrCallerSmapVersion = HeaderPrefix + "caller-smap-ver"

	HdrXactionID = HeaderPrefix + "xaction-id"

	// Stream related headers.
	HdrSessID   = HeaderPrefix + "session-id"
	HdrCompress = HeaderPrefix + "compress" // LZ4Compression, etc.

	// Promote(dir)
	HdrPromoteNamesHash = HeaderPrefix + "promote-names-hash"
	HdrPromoteNamesNum  = HeaderPrefix + "promote-names-num"
)
