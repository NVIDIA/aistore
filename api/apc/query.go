// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

//
// NOTE: when adding new (or changing existing) query parameters, see ais/dpq.go comment on _categories_.
//

const (
	// see related "GET(what)" set of APIs: api/cluster and api/daemon
	QparamWhat = "what" // "smap" | "bmd" | "config" | "stats" | "xaction" ... (enum below)

	QparamProps = "props" // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...

	QparamTransient = "transient" // transient - in-memory only

	QparamUUID  = "uuid"  // Transaction/xaction UUID identifier
	QparamJobID = "jobid" // Job identifier

	// etl
	QparamETLName          = "etl_name"
	QparamETLPipeline      = "etl_pipeline"
	QparamETLTransformArgs = "etl_args"
	QparamETLFQN           = "etl_fqn"
	QparamETLSecret        = "etl_secret" // secret generated during ETL init to validate directly target access from trusted ETL

	QparamRegex      = "regex"       // dsort: list regex
	QparamOnlyActive = "only_active" // dsort: list only active

	// remove existing custom keys and store new custom metadata
	// NOTE: making an s/_/-/ naming exception because of the namesake CLI usage
	QparamNewCustom = "set-new-custom"

	// Main bucket query params.
	QparamProvider  = "provider"  // Backend provider type (ais, s3, gcp, azure, etc.)
	QparamNamespace = "namespace" // Namespace for remote buckets and cross-cluster operations

	// e.g., usage: copy bucket, copy object
	QparamBckTo = "bck_to"    // Destination bucket for copy/move operations
	QparamObjTo = "object_to" // Destination object name for copy/move operations

	// Do not add remote bucket to cluster's BMD e.g. when checking existence
	// via api.HeadBucket
	// By default, when existence of a remote buckets is confirmed the bucket's
	// metadata gets automatically (and transactionally) added to the cluster's BMD.
	// This query parameter can be used to override the default behavior.
	QparamDontAddRemote = "dont_add_remote_bck_md" // Don't add remote bucket to cluster BMD

	// Add remote bucket to BMD _unconditionally_ and without executing HEAD request
	// (to check access and load the bucket's properties)
	// NOTE: usage is limited to setting up bucket properties with alternative
	// profile and/or endpoint
	// See also:
	// - `LsDontHeadRemote`
	// - docs/bucket.md
	// - docs/cli/aws_profile_endpoint.md
	QparamDontHeadRemote = "dont_head_remote_bck" // Add remote bucket without HEAD request validation

	// When evicting, keep remote bucket in BMD (i.e., evict data only)
	QparamKeepRemote = "keep_bck_md" // Keep bucket metadata when evicting remote bucket data

	// (api.GetBucketInfo)
	// NOTE: non-empty value indicates api.GetBucketInfo; "true" value further requires "with remote obj-s"
	QparamBinfoWithOrWithoutRemote = "bsumm_remote" // Include remote objects in bucket summary/info

	// "presence" in a given cluster shall not be confused with "existence" (possibly, remote).
	// See also:
	// - Flt* enum below
	// - ListObjsMsg flags, docs/providers.md (for terminology)
	QparamFltPresence = "presence" // Filter bucket/object by presence state (FltExists, FltPresent, etc.)

	// APPEND(object) operation - QparamAppendType enum below
	QparamAppendType   = "append_type"   // Type of append operation (append, flush)
	QparamAppendHandle = "append_handle" // Handle for ongoing append operations

	// HTTP bucket support.
	QparamOrigURL = "original_url" // Original URL for HTTP bucket objects

	// Get logs
	QparamLogSev  = "severity" // see { LogInfo, ...} enum
	QparamLogOff  = "offset"
	QparamAllLogs = "all"

	// The following 4 (four) QparamArch* parameters are all intended for usage with sharded datasets,
	// whereby the shards are (.tar, .tgz (or .tar.gz), .zip, and/or .tar.lz4) formatted objects.
	//
	// For the most recently updated list of supported serialization formats, please see cmn/archive package.
	//
	// "archpath" and "archmime", respectively, specify archived pathname and expected format (mime type)
	// of the containing shard; the latter is especially usable with non-standard shard name extensions;
	QparamArchpath = "archpath"
	QparamArchmime = "archmime"

	// In addition, the following two closely related parameters can be used to select multiple matching files
	// from a given shard.
	//
	// In particular, "archregx" specifies prefix, suffix, WebDataset key, _or_ general-purpose regular expression
	// that can be used to match archived filenames, and select possibly multiple files
	// (that will be then archived as a TAR and returned in one shot);
	QparamArchregx = "archregx"

	// "archmode", on the other hand, tells aistore whether to interpret "archregx" (above) as a
	// general-purpose regular expression or (alternativelyr) use it for simple and fast string comparison;
	// the latter is further formalized as `MatchMode` enum in the cmn/archive package,
	// with enumerated values including: "regexp", "prefix", "suffix", "substr", "wdskey".
	//
	// for example:
	// - given a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)
	//   and "wdskey" = "subdir/aaa", aistore will match and return (subdir/aaa.jpg, subdir/aaa.json).
	QparamArchmode = "archmode" // see `MatchMode` enum in cmn/archive/read

	// See also:
	// - https://github.com/webdataset/webdataset                     - for WebDataset
	// - docs/cli/archive.md                                          - for usage and examples
	// - docs/cli/archive.md#get-archived-content-multiple-selection  - multi-selection usage and examples
	// - cmn/archive                                                  - the most recently updated "archmode" enumeration

	// Skip loading existing object's metadata, in part to
	// compare its Checksum and update its existing Version (if exists).
	// Can be used to reduce PUT latency when:
	// - we massively write new content into a bucket, and/or
	// - we simply don't care.
	QparamSkipVC = "skip_vc"

	// force operation
	// used to overcome certain restrictions, e.g.:
	// - shutdown the primary and the entire cluster
	// - attach invalid mountpath
	QparamForce = "frc" // Force operation to override restrictions

	// same as `Versioning.ValidateWarmGet` (cluster config and bucket props)
	// - usage: GET and (copy|transform) x (bucket|multi-object) operations
	// - implies remote backend
	QparamLatestVer = "latest-ver" // Get latest version of objects from remote backend

	// in addition to the latest-ver (above), also entails removing remotely
	// deleted objects
	QparamSync = "synchronize"

	// validate (ie., recompute and check) in-cluster object's checksums
	QparamValidateCksum = "validate-checksum"

	// when true, skip nlog.Error and friends
	// (to opt-out logging too many messages and/or benign warnings)
	QparamSilent = "sln" // Suppress error logging and warnings

	// (see api.AttachMountpath vs. LocalConfig.FSP)
	QparamMpathLabel = "mountpath_label"

	// Request to restore an object
	QparamECObject = "object"

	QparamMptUploads  = "uploads"    // Start multipart upload
	QparamMptUploadID = "uploadId"   // Complete, abort, or list parts of specific multipart upload
	QparamMptPartNo   = "partNumber" // Part number for multipart upload
)

// QparamFltPresence enum.
//
// Describes both buckets and objects with respect to their existence/presence (or non-existence/non-presence)
// in AIS cluster.
//
// "FltPresent*" refers to availability ("presence") in the cluster. For details, see the values and comments below.
//
// Remote object or bucket that is currently not present can still be accessed with
// the very first access making it "present", etc.
const (
	FltExists         = iota // (object | bucket) exists inside and/or outside cluster
	FltExistsNoProps         // same as above but no need to return props/info
	FltPresent               // bucket: is present | object: present and properly located
	FltPresentNoProps        // same as above but no need to return props/info

	// Objects are present on any target, any disk inside the cluster (including replicas, EC slices, misplaced, or rebalancing).
	// TODO: Currently, `FltPresentCluster` only checks whether the LOM can be loaded, and tries to find/restore it on the same target if loading fails.
	// This should be extended to check whether the object is present on any target or any disk inside the cluster (see also: `t.headObjBcast()`).
	FltPresentCluster
	FltExistsOutside // not present - exists _outside_ cluster (NOTE: currently, only list-buckets)
)

func IsFltPresent(v int) bool {
	return v == FltPresent || v == FltPresentNoProps || v == FltPresentCluster
}

func IsFltNoProps(v int) bool {
	return v == FltExistsNoProps || v == FltPresentNoProps
}

// QparamAppendType enum.
const (
	AppendOp = "append"
	FlushOp  = "flush"
)

// health
const (
	QparamHealthReadiness = "readiness" // used by external watchdogs (K8s)
	QparamHealthReady     = QparamHealthReadiness + "=true"

	QparamAskPrimary      = "apr" // true: the caller is directing health request to primary
	QparamPrimaryReadyReb = "prr" // true: check whether primary is ready to start rebalancing cluster
)

// Internal query params.
const (
	QparamPID              = "pid" // ID of a redirecting proxy.
	QparamPrimaryCandidate = "can" // candidate for the primary proxy (voting ID, force URL)
	QparamPrepare          = "prp" // 2-phase commit where 'true' corresponds to 'begin'; usage: (primary election; set-primary)
	QparamUnixTime         = "utm" // Unix time since 01/01/70 UTC (nanoseconds)
	QparamIsGFNRequest     = "gfn" // true if the request is a Get-From-Neighbor
	QparamRebStatus        = "rbs" // true: get detailed rebalancing status
	QparamRebData          = "rbd" // true: get EC rebalance data (pulling data if push way fails)
	QparamClusterInfo      = "cii" // true: /Health to return `cos.NodeStateInfo` including cluster metadata versions and state flags
	QparamOWT              = "owt" // object write transaction enum { OwtPut, ..., OwtGet* }

	QparamDontResilver = "dntres" // true: do not resilver data off of mountpaths that are being disabled/detached

	// dsort
	QparamTotalCompressedSize       = "tcs"
	QparamTotalInputShardsExtracted = "tise"
	QparamTotalUncompressedSize     = "tunc"

	// 2PC transactions - control plane
	QparamNetwTimeout  = "xnt" // [begin, start-commit] timeout
	QparamHostTimeout  = "xht" // [begin, txn-done] timeout
	QparamWaitMetasync = "xwm" // true: wait for metasync (used only when there's an alternative)

	// promote
	QparamConfirmFshare = "confirm-fshr" // confirm file share
	QparamActNoXact     = "act-no-xact"  // execute synchronously, i.e. without xaction

	// Notification target's node ID (usually, the node that initiates the operation).
	QparamNotifyMe = "nft"

	// added in v4.1
	QparamSmapVer = "vpams"
	QparamNonce   = "x"
	QparamHMAC    = "u"

	// GetBatch
	QparamTID   = "tid"   // designated target
	QparamColoc = "coloc" // colocation hint: enum { 0=ColocNone, 1=ColocOne, 2=ColocTwo }

	// System
	QparamSystem = "sys"
)

// QparamWhat enum.
const (
	// cluster metadata
	WhatSmap = "smap"
	WhatBMD  = "bmd"

	// config
	WhatNodeConfig    = "config"         // query specific node for (cluster config + overrides, local config)
	WhatClusterConfig = "cluster_config" // as the name implies; identical (compressed, checksummed, versioned) copy on each node

	// configured backends
	WhatBackends = "backends"

	// stats and status
	WhatNodeStats          = "node_stats"  // redundant
	WhatNodeStatsAndStatus = "node_status" // current

	WhatDiskRWUtilCap = "disk" // read/write stats, disk utilization, capacity

	WhatMetricNames = "metrics"

	// assorted
	WhatMountpaths = "mountpaths"
	WhatRemoteAIS  = "remote"
	WhatSmapVote   = "smapvote"
	WhatSysInfo    = "sysinfo"
	WhatTargetIPs  = "target_ips" // comma-separated list of all target IPs (compare w/ GetWhatSnode)

	// log
	WhatLog = "log"

	// xactions
	WhatOneXactStatus   = "status"      // IC status by uuid (returns a single matching xaction or none)
	WhatAllXactStatus   = "status_all"  // ditto - all matching xactions
	WhatXactStats       = "getxstats"   // stats: xaction by uuid
	WhatQueryXactStats  = "qryxstats"   // stats: all matching xactions
	WhatAllRunningXacts = "running_all" // e.g. e.g.: put-copies[D-ViE6HEL_j] list[H96Y7bhR2s] ...

	// internal
	WhatSnode    = "snode"
	WhatICBundle = "ic_bundle"

	// tls
	WhatCertificate = "tls_certificate"
)

// QparamLogSev enum.
const (
	LogInfo = "info"
	LogWarn = "warning"
	LogErr  = "error"
)
