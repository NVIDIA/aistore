// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "strings"

// AIS http header conventions:
//   - always starts with the prefix "ais-"
//   - all words separated with "-"
//   - no '.' periods, no underscores.
// For standard and provider-specific HTTP headers, see cmn/cos/const_http.go

const HdrError = "Hdr-Error"

const (
	HeaderPrefix = "ais-"

	// bucket inventory - an alternative way to list (very large) buckets
	HdrInventory = HeaderPrefix + "bucket-inventory" // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrInvName   = HeaderPrefix + "inv-name"         // optional; name of the inventory (to override the system default)
	HdrInvID     = HeaderPrefix + "inv-id"           // optional; inventory ID (ditto)

	// GET via x-blob-download
	HdrBlobDownload = HeaderPrefix + ActBlobDl      // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrBlobChunk    = HeaderPrefix + "blob-chunk"   // optional; e.g., 1mb, 2MIB, 3m, or 1234567 (bytes)
	HdrBlobWorkers  = HeaderPrefix + "blob-workers" // optional; the default number of workers is dfltNumWorkers in xs/blob_download.go

	// Bucket props headers
	HdrBucketProps      = HeaderPrefix + "bucket-props"       // => cmn.Bprops
	HdrBucketSumm       = HeaderPrefix + "bucket-summ"        // => cmn.BsummResult (see also: QparamFltPresence)
	HdrBucketVerEnabled = HeaderPrefix + "versioning-enabled" // Enable/disable object versioning in a bucket.
	HdrBackendProvider  = HeaderPrefix + "provider"           // ProviderAmazon et al. - see cmn/bck.go.

	// including BucketProps.Extra.AWS
	HdrS3Region   = HeaderPrefix + "cloud_region"
	HdrS3Endpoint = HeaderPrefix + "endpoint"
	HdrS3Profile  = HeaderPrefix + "profile"

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

	// Append object header.
	HdrAppendHandle = HeaderPrefix + "append-handle"

	// api.PutApndArchArgs message flags
	HdrPutApndArchFlags = HeaderPrefix + "pine"

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
	HdrCallerID        = HeaderPrefix + "caller-id" // Marker of intra-cluster request.
	HdrT2TPutterID     = HeaderPrefix + "putter-id" // DaemonID of the target that performs intra-cluster PUT
	HdrCallerName      = HeaderPrefix + "caller-name"
	HdrCallerIsPrimary = HeaderPrefix + "caller-is-primary"
	HdrCallerSmapVer   = HeaderPrefix + "caller-smap-ver"

	HdrXactionID = HeaderPrefix + "xaction-id"

	// Stream related headers.
	HdrSessID   = HeaderPrefix + "session-id"
	HdrCompress = HeaderPrefix + "compress" // LZ4Compression, etc.

	// Promote(dir)
	HdrPromoteNamesHash = HeaderPrefix + "promote-names-hash"
	HdrPromoteNamesNum  = HeaderPrefix + "promote-names-num"
)

//
// convert prop name to HTTP header tag name
//

func PropToHeader(prop string) string {
	if strings.HasPrefix(prop, HeaderPrefix) {
		return prop
	}
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}
	prop = strings.ReplaceAll(prop, ".", "-")
	prop = strings.ReplaceAll(prop, "_", "-")
	return HeaderPrefix + prop
}
