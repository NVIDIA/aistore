// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"
	"unicode"

	"github.com/NVIDIA/aistore/cmn/cos"
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
	HdrS3InvID   = aisPrefix + "Inv-Id"           // Deprecated: inventory ID

	// GET via x-blob-download
	HdrBlobDownload = aisPrefix + "Blob-Download" // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrBlobChunk    = aisPrefix + "Blob-Chunk"    // optional; e.g., 1mb, 2MIB, 3m, or 1234567 (bytes)
	HdrBlobWorkers  = aisPrefix + "Blob-Workers"  // optional: num concurrent downloading readers (see also: xs/nwp.go, "media type", load.Advice)

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

	HdrNodeURL   = aisPrefix + "Node-Url"
	HdrNodeFlags = aisPrefix + "Node-Flags"
)

// Custom S3 headers
const (
	// HdrSignedRequestStyle describes what type of request style was used to sign the request.
	// This is important because we don't really have way of knowing if the request
	// was signed with the style:
	//	* `virtual-hosted` - https://<bucket>.s3.<region>.amazonaws.com/<path_to_object> or,
	//	* `path`           - https://s3.<region>.amazonaws.com/<bucket>/<path_to_object>.
	// By default, (if the header is empty or not set) we use `virtual-hosted` style.
	// In case, the value of this header is not valid, the error will be thrown.
	HdrSignedRequestStyle = aisPrefix + "S3-Signed-Request-Style"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internal (intra-cluster) headers
const (
	//
	// TODO: update as aisPrefix + "Sender..."; keeping for now for backward compat.
	//
	HdrSenderID        = aisPrefix + "Caller-Id"
	HdrSenderName      = aisPrefix + "Caller-Name"
	HdrSenderIsPrimary = aisPrefix + "Caller-Is-Primary"
	HdrSenderSmapVer   = aisPrefix + "Caller-Smap-Ver"

	HdrT2TPutterID = aisPrefix + "Putter-Id" // DaemonID of the target that performs intra-cluster PUT

	HdrXactionID = aisPrefix + "Xaction-Id"

	// intra-cluster streams
	HdrSessID   = aisPrefix + "Session-Id"
	HdrCompress = aisPrefix + "Compress" // LZ4

	// Promote(dir)
	HdrPromoteNamesHash = aisPrefix + "Promote-Names-Hash"
	HdrPromoteNamesNum  = aisPrefix + "Promote-Names-Num"

	// ETL
	HdrETLPodInfo      = aisPrefix + "ETL-Pod-Info" // serialized etl.Info
	HdrDirectPutLength = aisPrefix + "Direct-Put-Length"

	// shared streams
	HdrActiveEC = aisPrefix + "Ec"
	HdrActiveDM = aisPrefix + "Dm"

	// (advanced use)
	HdrReadyToJoinClu = aisPrefix + "Ready-Join-Clu"
)

const lais = len(aisPrefix)

// internal (json) obj prop => canonical http header
// usage:
// - target InitObjProps2Hdr
// - api/object
func PropToHeader(prop string) string {
	debug.Assert(!strings.HasPrefix(prop, aisPrefix), "already converted: ", prop)
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}

	var (
		l   = len(prop)
		out = make([]byte, l+lais)
		o   = out[lais:]
		up  = true
	)
	copy(out, aisPrefix)
	for i := range l {
		c := prop[i]
		if c == '.' || c == '_' {
			c = '-'
		}
		switch {
		case up && 'a' <= c && c <= 'z':
			o[i] = byte(unicode.ToUpper(rune(c)))
		case !up && 'A' <= c && c <= 'Z':
			o[i] = byte(unicode.ToLower(rune(c)))
		default:
			o[i] = c
		}
		up = c == '-'
	}
	return cos.UnsafeS(out)
}
