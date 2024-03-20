// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// URL Query "?name1=val1&name2=..."
// User query params.
const (
	QparamWhat = "what" // "smap" | "bmd" | "config" | "stats" | "xaction" ... (enum below)

	QparamProps = "props" // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...

	QparamUUID    = "uuid"     // xaction
	QparamJobID   = "jobid"    // job
	QparamETLName = "etl_name" // etl

	QparamRegex      = "regex"       // dsort: list regex
	QparamOnlyActive = "only_active" // dsort: list only active

	// remove existing custom keys and store new custom metadata
	// NOTE: making an s/_/-/ naming exception because of the namesake CLI usage
	QparamNewCustom = "set-new-custom"

	// Main bucket query params.
	QparamProvider  = "provider" // aka backend provider or, simply, backend
	QparamNamespace = "namespace"

	// e.g., usage: copy bucket
	QparamBckTo = "bck_to"

	// Do not add remote bucket to cluster's BMD e.g. when checking existence
	// via api.HeadBucket
	// By default, when existence of a remote buckets is confirmed the bucket's
	// metadata gets automatically (and transactionally) added to the cluster's BMD.
	// This query parameter can be used to override the default behavior.
	QparamDontAddRemote = "dont_add_remote_bck_md"

	// Add remote bucket to BMD _unconditionally_ and without executing HEAD request
	// (to check access and load the bucket's properties)
	// NOTE: usage is limited to setting up bucket properties with alternative
	// profile and/or endpoint
	// See also:
	// - `LsDontHeadRemote`
	// - docs/bucket.md
	// - docs/cli/aws_profile_endpoint.md
	QparamDontHeadRemote = "dont_head_remote_bck"

	// When evicting, keep remote bucket in BMD (i.e., evict data only)
	QparamKeepRemote = "keep_bck_md"

	// (api.GetBucketInfo)
	QparamBsummRemote = "bsumm_remote"

	// "presence" in a given cluster shall not be be confused with "existence" (possibly, remote).
	// See also:
	// - Flt* enum below
	// - ListObjsMsg flags, docs/providers.md (for terminology)
	QparamFltPresence = "presence"

	// APPEND(object) operation - QparamAppendType enum below
	QparamAppendType   = "append_type"
	QparamAppendHandle = "append_handle"

	// HTTP bucket support.
	QparamOrigURL = "original_url"

	// Get logs
	QparamLogSev  = "severity" // see { LogInfo, ...} enum
	QparamLogOff  = "offset"
	QparamAllLogs = "all"

	// Archive filename and format (mime type)
	QparamArchpath = "archpath"
	QparamArchmime = "archmime"

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
	QparamForce = "frc"

	// same as `Versioning.ValidateWarmGet` (cluster config and bucket props)
	// - usage: GET and (copy|transform) x (bucket|multi-object) operations
	// - implies remote backend
	QparamLatestVer = "latest-ver"

	// in addition to the latest-ver (above), also entails removing remotely
	// deleted objects
	QparamSync = "synchronize"

	// when true, skip nlog.Error and friends
	// (to opt-out logging too many messages and/or benign warnings)
	QparamSilent = "sln"

	// (see api.AttachMountpath vs. LocalConfig.FSP)
	QparamDiskLabel = "disk_label"
)

// QparamFltPresence enum.
//
// Descibes both buckets and objects with respect to their existence/presence (or non-existence/non-presence)
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
	FltPresentCluster        // objects: present anywhere/anyhow _in_ the cluster as: replica, ec-slices, misplaced
	FltExistsOutside         // not present - exists _outside_ cluster (NOTE: currently, only list-buckets)
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
	QparamHealthReadiness = "readiness" // to be used by external watchdogs (e.g. K8s)
	QparamAskPrimary      = "apr"       // true: the caller is directing health request to primary
	QparamPrimaryReadyReb = "prr"       // true: check whether primary is ready to start rebalancing cluster
)

// Internal query params.
const (
	QparamProxyID          = "pid" // ID of the redirecting proxy.
	QparamPrimaryCandidate = "can" // ID of the candidate for the primary proxy.
	QparamPrepare          = "prp" // true: request belongs to the "prepare" phase of the primary proxy election
	QparamNonElectable     = "nel" // true: proxy is non-electable for the primary role
	QparamUnixTime         = "utm" // Unix time since 01/01/70 UTC (nanoseconds)
	QparamIsGFNRequest     = "gfn" // true if the request is a Get-From-Neighbor
	QparamRebStatus        = "rbs" // true: get detailed rebalancing status
	QparamRebData          = "rbd" // true: get EC rebalance data (pulling data if push way fails)
	QparamClusterInfo      = "cii" // true: /Health to return cluster info and status
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
)

// QparamWhat enum.
const (
	// cluster meta
	WhatSmap = "smap"
	WhatBMD  = "bmd"
	// config
	WhatNodeConfig    = "config" // query specific node for (cluster config + overrides, local config)
	WhatClusterConfig = "cluster_config"
	// stats
	WhatNodeStats          = "stats"
	WhatNodeStatsAndStatus = "status"
	WhatMetricNames        = "metrics"
	WhatDiskStats          = "disk"
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
)

// QparamLogSev enum.
const (
	LogInfo = "info"
	LogWarn = "warning"
	LogErr  = "error"
)
