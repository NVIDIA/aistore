// Package apc: API constants and message types
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// URL Query "?name1=val1&name2=..."
// User query params.
const (
	QparamWhat = "what" // "smap" | "bmd" | "config" | "stats" | "xaction" ... (enum below)

	QparamProps = "props" // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...
	QparamUUID  = "uuid"  // xaction
	QparamRegex = "regex" // dsort/downloader regex

	// remove existing custom keys and store new custom metadata
	// NOTE: making an s/_/-/ naming exception because of the namesake CLI usage
	QparamNewCustom = "set-new-custom"

	// Bucket related query params.
	QparamProvider  = "provider" // aka backend provider or, simply, backend
	QparamNamespace = "namespace"
	QparamBckTo     = "bck_to"
	QparamKeepBckMD = "keep_bck_md"

	// Do not add remote bucket to cluster's BMD e.g. when checking existence
	// via api.HeadBucket
	// By default, when existence of a remote buckets is confirmed the bucket's
	// metadata gets automatically (and transactionally) added to the cluster's BMD.
	// This query parameter can be used to override the default behavior.
	QparamDontAddBckMD = "dont_add_remote_bck_md"

	// NOTE: "presence" in a given cluster shall not be be confused with "existence" (possibly, remote).
	// See also:
	// - Flt* enum below
	// - ListObjsMsg flags, docs/providers.md (for terminology)
	QparamFltPresence = "presence"

	// Object related query params.
	QparamAppendType   = "append_type"
	QparamAppendHandle = "append_handle"

	// HTTP bucket support.
	QparamOrigURL = "original_url"

	// Log severity
	QparamSev = "severity" // see { LogInfo, ...} enum

	// Archive filename and format (mime type)
	QparamArchpath = "archpath"
	QparamArchmime = "archmime"

	// Skip loading existing object's metadata, in part to
	// compare its Checksum and update its existing Version (if exists).
	// Can be used to reduce PUT latency when:
	// - we massively write new content into a bucket, and/or
	// - we simply don't care.
	QparamSkipVC = "skip_vc"

	// force the operation; allows to overcome certain restrictions (e.g., shutdown primary and the entire cluster)
	// or errors (e.g., attach invalid mountpath)
	QparamForce = "frc"
)

// QparamFltPresence enum.
// Descibes both buckets and objects with respect to their existence/presence
// (or non-existence/non-presence) in a given AIS cluster.
//
// (Terminology: "FltPresent*" here refers to availability _in the cluster_.)
//
// Note that a remote object (or a bucket) that is currently _not_ present in the cluster can still
// be accessible with (the first) access resulting in this object (or bucket) becoming "present", etc.
const (
	FltExists           = iota // exists, as in: (FltPresentAnywhere | FltExistsOutside)
	FltPresent                 // bucket: is present; LOM: present and properly located
	FltPresentOmitProps        // establish presence = Yes/No, and that's it (same as above with no info returned)
	FltPresentAnywhere         // presence anywhere/anyhow _in_ the cluster (as a replica, ec-slices, temp misplaced)
	FltExistsOutside           // remote, e.g. cloud bucket
)

func IsFltPresent(v int) bool {
	return v == FltPresent || v == FltPresentOmitProps || v == FltPresentAnywhere
}

// QparamAppendType enum.
const (
	AppendOp = "append"
	FlushOp  = "flush"
)

// QparamTaskAction enum.
const (
	TaskStart  = Start
	TaskStatus = "status"
	TaskResult = "result"
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
	QparamSilent           = "sln" // true: destination should not log errors (HEAD request)
	QparamRebStatus        = "rbs" // true: get detailed rebalancing status
	QparamRebData          = "rbd" // true: get EC rebalance data (pulling data if push way fails)
	QparamTaskAction       = "tac" // "start", "status", "result"
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
	GetWhatBMD           = "bmd"
	GetWhatConfig        = "config"
	GetWhatClusterConfig = "cluster_config"
	GetWhatDaemonStatus  = "status"
	GetWhatDiskStats     = "disk"
	GetWhatMountpaths    = "mountpaths"
	GetWhatRemoteAIS     = "remote"
	GetWhatSmap          = "smap"
	GetWhatSmapVote      = "smapvote"
	GetWhatSnode         = "snode"
	GetWhatStats         = "stats"
	GetWhatXactStatus    = "status" // IC status by uuid.
	GetWhatSysInfo       = "sysinfo"
	GetWhatTargetIPs     = "target_ips"
	GetWhatLog           = "log"
)

// Internal "what" values.
const (
	GetWhatXactStats      = "getxstats" // stats: xaction by uuid
	GetWhatQueryXactStats = "qryxstats" // stats: all matching xactions
	GetWhatICBundle       = "ic_bundle"
)

// QparamSev enum.
const (
	LogInfo = "info"
	LogWarn = "warning"
	LogErr  = "error"
)
