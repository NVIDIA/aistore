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
	HdrPrefixAIS = "Ais-"

	// bucket inventory - request inventory-backed listing (implemented via NBI)
	HdrInventory = HdrPrefixAIS + "Bucket-Inventory" // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrInvName   = HdrPrefixAIS + "Inv-Name"         // optional; name of the inventory (to override the system default)

	// GET via x-blob-download
	HdrBlobDownload    = HdrPrefixAIS + "Blob-Download"     // must be present and must be "true" (or "y", "yes", "on" case-insensitive)
	HdrBlobChunk       = HdrPrefixAIS + "Blob-Chunk"        // optional; e.g., 1mb, 2MIB, 3m, or 1234567 (bytes)
	HdrBlobWorkers     = HdrPrefixAIS + "Blob-Workers"      // optional: num concurrent downloading readers (see also: xs/nwp.go, "media type", load.Advice)
	HdrBlobReadTimeout = HdrPrefixAIS + "Blob-Read-Timeout" // per-attempt timeout for backend range read; zero selects default
	HdrBlobThreshold   = HdrPrefixAIS + "Blob-Threshold"    // minimum remote object size (bytes) to use blob downloader

	// Bucket props headers
	HdrBucketProps      = HdrPrefixAIS + "Bucket-Props"       // => cmn.Bprops
	HdrBucketSumm       = HdrPrefixAIS + "Bucket-Summ"        // => cmn.BsummResult (see also: QparamFltPresence)
	HdrBucketVerEnabled = HdrPrefixAIS + "Versioning-Enabled" // Enable/disable object versioning in a bucket.
	HdrBackendProvider  = HdrPrefixAIS + "Provider"           // ProviderAmazon et al. - see cmn/bck.go.

	// including BucketProps.Extra.AWS
	HdrS3Region   = HdrPrefixAIS + "Cloud_region"
	HdrS3Endpoint = HdrPrefixAIS + "Endpoint"
	HdrS3Profile  = HdrPrefixAIS + "Profile"

	// including BucketProps.Extra.OCI
	HdrOCIRegion = HdrPrefixAIS + "Oci-Region"

	// remote AIS
	HdrRemAisUUID  = HdrPrefixAIS + "Remote-Ais-Uuid"
	HdrRemAisAlias = HdrPrefixAIS + "Remote-Ais-Alias"
	HdrRemAisURL   = HdrPrefixAIS + "Remote-Ais-Url"

	HdrRemoteOffline = HdrPrefixAIS + "Remote-Offline" // When accessing cached remote bucket with no backend connectivity.

	// Object props headers
	HdrObjCksumType = HdrPrefixAIS + "Checksum-Type"  // Checksum type, one of SupportedChecksums().
	HdrObjCksumVal  = HdrPrefixAIS + "Checksum-Value" // Checksum value.
	HdrObjAtime     = HdrPrefixAIS + "Atime"          // Object access time.
	HdrObjCustomMD  = HdrPrefixAIS + "Custom-Md"      // Object custom metadata.
	HdrObjVersion   = HdrPrefixAIS + "Version"        // Object version/generation - ais or cloud.

	// Append object header
	HdrAppendHandle = HdrPrefixAIS + "Append-Handle"

	// api.PutApndArchArgs message flags
	HdrPutApndArchFlags = HdrPrefixAIS + "Pine"

	// Query objects handle header
	HdrHandle = HdrPrefixAIS + "Query-Handle"

	// Reverse proxy header
	HdrNodeID = HdrPrefixAIS + "Node-Id"

	// uptimes, respectively
	HdrNodeUptime    = HdrPrefixAIS + "Node-Uptime"
	HdrClusterUptime = HdrPrefixAIS + "Cluster-Uptime"

	HdrNodeURL   = HdrPrefixAIS + "Node-Url"
	HdrNodeFlags = HdrPrefixAIS + "Node-Flags"

	// Software version (`aisnode --version`)
	HdrNodeVersion = HdrPrefixAIS + "Node-Version"
)

// the value for cos.HdrUserAgent header (internal usage)
const HdrUA = HdrPrefixAIS + "Node"

// Custom S3 headers
const (
	// HdrSignedRequestStyle describes what type of request style was used to sign the request.
	// This is important because we don't really have way of knowing if the request
	// was signed with the style:
	//	* `virtual-hosted` - https://<bucket>.s3.<region>.amazonaws.com/<path_to_object> or,
	//	* `path`           - https://s3.<region>.amazonaws.com/<bucket>/<path_to_object>.
	// By default, (if the header is empty or not set) we use `virtual-hosted` style.
	// In case, the value of this header is not valid, the error will be thrown.
	HdrSignedRequestStyle = HdrPrefixAIS + "S3-Signed-Request-Style"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internal (intra-cluster) headers
const (
	HdrSenderID        = HdrPrefixAIS + "Caller-Id"
	HdrSenderName      = HdrPrefixAIS + "Caller-Name"
	HdrSenderIsPrimary = HdrPrefixAIS + "Caller-Is-Primary"
	HdrSenderSmapVer   = HdrPrefixAIS + "Caller-Smap-Ver"

	HdrXactionID = HdrPrefixAIS + "Xaction-Id"

	// intra-cluster streams
	HdrSessID   = HdrPrefixAIS + "Session-Id"
	HdrCompress = HdrPrefixAIS + "Compress" // LZ4

	// Promote(dir)
	HdrPromoteNamesHash = HdrPrefixAIS + "Promote-Names-Hash"
	HdrPromoteNamesNum  = HdrPrefixAIS + "Promote-Names-Num"

	// ETL
	HdrETLPodInfo      = HdrPrefixAIS + "ETL-Pod-Info" // serialized etl.Info
	HdrDirectPutLength = HdrPrefixAIS + "Direct-Put-Length"
	// ETL → AIS retry contract: emitted by the ETL webserver alongside HTTP 503
	// to signal that the ETL bailed on a transient direct-put failure without
	// trying locally (one-shot body case). AIS retries the whole PUT against
	// the replayable LOM-backed source. See ext/etl/communicator.go.
	HdrETLRetryReason                = HdrPrefixAIS + "Etl-Retry-Reason"
	ETLRetryReasonDirectPutTransient = "direct-put-transient"

	// shared streams
	HdrActiveEC = HdrPrefixAIS + "Ec"

	// (ais/psetforce; advanced use)
	HdrReadyToJoinClu = HdrPrefixAIS + "Ready-Join-Clu"

	// cluster key (csk)
	HdrSenderSig   = HdrPrefixAIS + "Caller-Sig"   // base64 RawURL HMAC-SHA256 over the intra request
	HdrSenderNonce = HdrPrefixAIS + "Caller-Nonce" // monotonic nonce bound into the signature

	// node-join shared secret (request and response)
	HdrJoinTime = HdrPrefixAIS + "Join-Time"
	HdrJoinSig  = HdrPrefixAIS + "Join-Sig"
)

const lais = len(HdrPrefixAIS)

// internal (json) obj prop => canonical http header
// usage:
// - target InitObjProps2Hdr
// - api/object
func PropToHeader(prop string) string {
	debug.AssertFunc(func() bool { return !strings.HasPrefix(prop, HdrPrefixAIS) }, "already converted: ", prop)
	if prop[0] == '.' || prop[0] == '_' {
		prop = prop[1:]
	}

	var (
		l   = len(prop)
		out = make([]byte, l+lais)
		o   = out[lais:]
		up  = true
	)
	copy(out, HdrPrefixAIS)
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
