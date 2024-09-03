// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// AIS http header conventions:
//   - always starts with the prefix "ais-"
//   - all words separated with "-"
//   - no '.' periods, no underscores.
// For standard and provider-specific HTTP headers, see cmn/cos/const_http.go

const HdrError = "Hdr-Error"

const (
	aisPrefix = "Ais-"

	// bucket inventory - an alternative way to list (very large) buckets
	HdrInventory = aisPrefix + "Bucket-Inventory" // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrInvName   = aisPrefix + "Inv-Name"         // optional; name of the inventory (to override the system default)
	HdrInvID     = aisPrefix + "Inv-Id"           // optional; inventory ID (ditto)

	// GET via x-blob-download
	HdrBlobDownload = aisPrefix + "Blob-Download" // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrBlobChunk    = aisPrefix + "Blob-Chunk"    // optional; e.g., 1mb, 2MIB, 3m, or 1234567 (bytes)
	HdrBlobWorkers  = aisPrefix + "Blob-Workers"  // optional; the default number of workers is dfltNumWorkers in xs/blob_download.go

	// Bucket props headers
	HdrBucketProps      = aisPrefix + "Bucket-Props"       // => cmn.Bprops
	HdrBucketSumm       = aisPrefix + "Bucket-Summ"        // => cmn.BsummResult (see also: QparamFltPresence)
	HdrBucketVerEnabled = aisPrefix + "Versioning-Enabled" // Enable/disable object versioning in a bucket.
	HdrBackendProvider  = aisPrefix + "Provider"           // ProviderAmazon et al. - see cmn/bck.go.

	// including BucketProps.Extra.AWS
	HdrS3Region   = aisPrefix + "Cloud_region"
	HdrS3Endpoint = aisPrefix + "Endpoint"
	HdrS3Profile  = aisPrefix + "Profile"

	// including BucketProps.Extra.HTTP
	HdrOrigURLBck = aisPrefix + "Original-Url"

	// remote AIS
	HdrRemAisUUID  = aisPrefix + "Remote-Ais-Uuid"
	HdrRemAisAlias = aisPrefix + "Remote-Ais-Alias"
	HdrRemAisURL   = aisPrefix + "Remote-Ais-Url"

	HdrRemoteOffline = aisPrefix + "Remote-Offline" // When accessing cached remote bucket with no backend connectivity.

	// Object props headers
	HdrObjCksumType = aisPrefix + "Checksum-Type"  // Checksum type, one of SupportedChecksums().
	HdrObjCksumVal  = aisPrefix + "Checksum-Value" // Checksum value.
	HdrObjAtime     = aisPrefix + "Atime"          // Object access time.
	HdrObjCustomMD  = aisPrefix + "Custom-Md"      // Object custom metadata.
	HdrObjVersion   = aisPrefix + "Version"        // Object version/generation - ais or cloud.

	// Append object header
	HdrAppendHandle = aisPrefix + "Append-Handle"

	// api.PutApndArchArgs message flags
	HdrPutApndArchFlags = aisPrefix + "Pine"

	// Query objects handle header
	HdrHandle = aisPrefix + "Query-Handle"

	// Reverse proxy header
	HdrNodeID = aisPrefix + "Node-Id"

	// uptimes, respectively
	HdrNodeUptime    = aisPrefix + "Node-Uptime"
	HdrClusterUptime = aisPrefix + "Cluster-Uptime"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internally used headers
const (
	HdrCallerID        = aisPrefix + "Caller-Id" // Marker of intra-cluster request.
	HdrT2TPutterID     = aisPrefix + "Putter-Id" // DaemonID of the target that performs intra-cluster PUT
	HdrCallerName      = aisPrefix + "Caller-Name"
	HdrCallerIsPrimary = aisPrefix + "Caller-Is-Primary"
	HdrCallerSmapVer   = aisPrefix + "Caller-Smap-Ver"

	HdrXactionID = aisPrefix + "Xaction-Id"

	// intra-cluster streams
	HdrSessID   = aisPrefix + "Session-Id"
	HdrCompress = aisPrefix + "Compress" // LZ4

	// Promote(dir)
	HdrPromoteNamesHash = aisPrefix + "Promote-Names-Hash"
	HdrPromoteNamesNum  = aisPrefix + "Promote-Names-Num"
)

// (compare with cmn.PropToHeader)
func PropToHeader(prop string) string {
	debug.Assert(!strings.HasPrefix(prop, aisPrefix), "already converted: ", prop)
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}
	prop = strings.ReplaceAll(prop, ".", "-")
	prop = strings.ReplaceAll(prop, "_", "-")
	return aisPrefix + prop
}
