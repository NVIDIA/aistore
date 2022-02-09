// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"
)

// module names
const (
	DSortName          = "dSort"
	DSortNameLowercase = "dsort"
)

// ActionMsg.Action
// includes Xaction.Kind == ActionMsg.Action (when the action is asynchronous)
const (
	ActAddRemoteBck    = "add-remote-bck" // register (existing) remote bucket into AIS
	ActArchive         = "archive"
	ActCopyBck         = "copy-bck"
	ActCopyObjects     = "copy-listrange"
	ActCreateBck       = "create-bck"
	ActDecommission    = "decommission" // decommission all nodes in the cluster (cleanup system data)
	ActDeleteObjects   = "delete-listrange"
	ActDestroyBck      = "destroy-bck" // destroy bucket data and metadata
	ActSummaryBck      = "summary-bck"
	ActDownload        = "download"
	ActECEncode        = "ec-encode" // erasure code a bucket
	ActECGet           = "ec-get"    // erasure decode objects
	ActECPut           = "ec-put"    // erasure encode objects
	ActECRespond       = "ec-resp"   // respond to other targets' EC requests
	ActETLInline       = "etl-inline"
	ActETLBck          = "etl-bck"
	ActETLObjects      = "etl-listrange"
	ActElection        = "election"
	ActEvictObjects    = "evict-listrange"
	ActEvictRemoteBck  = "evict-remote-bck" // evict remote bucket's data
	ActInvalListCache  = "inval-listobj-cache"
	ActLRU             = "lru"
	ActList            = "list"
	ActLoadLomCache    = "load-lom-cache"
	ActMakeNCopies     = "make-n-copies"
	ActMoveBck         = "move-bck"
	ActNewPrimary      = "new-primary"
	ActPrefetchObjects = "prefetch-listrange"
	ActPromote         = "promote"
	ActPutCopies       = "put-copies"
	ActRebalance       = "rebalance"
	ActRenameObject    = "rename-obj"
	ActResetBprops     = "reset-bprops"
	ActResetConfig     = "reset-config"
	ActResilver        = "resilver"
	ActResyncBprops    = "resync-bprops"
	ActSetBprops       = "set-bprops"
	ActSetConfig       = "set-config"
	ActShutdown        = "shutdown"
	ActStartGFN        = "start-gfn"
	ActStoreCleanup    = "cleanup-store"

	ActAttachRemote = "attach"
	ActDetachRemote = "detach"

	// Node maintenance & cluster membership (see the corresponding URL path words below)
	ActStartMaintenance   = "start-maintenance"     // put into maintenance state
	ActStopMaintenance    = "stop-maintenance"      // cancel maintenance state
	ActDecommissionNode   = "decommission-node"     // start rebalance and, when done, remove node from Smap
	ActShutdownNode       = "shutdown-node"         // shutdown node
	ActCallbackRmFromSmap = "callback-rm-from-smap" // set by primary when requested (internal use only)

	ActAdminJoinTarget = "admin-join-target"
	ActSelfJoinTarget  = "self-join-target"
	ActAdminJoinProxy  = "admin-join-proxy"
	ActSelfJoinProxy   = "self-join-proxy"
	ActKeepaliveUpdate = "keepalive-update"

	// IC
	ActSendOwnershipTbl  = "ic-send-own-tbl"
	ActListenToNotif     = "watch-xaction"
	ActMergeOwnershipTbl = "ic-merge-own-tbl"
	ActRegGlobalXaction  = "reg-global-xaction"
)

const (
	// Actions on mountpaths (/v1/daemon/mountpaths)
	ActMountpathAttach  = "attach-mp"
	ActMountpathEnable  = "enable-mp"
	ActMountpathDetach  = "detach-mp"
	ActMountpathDisable = "disable-mp"

	// Actions on xactions
	ActXactStop  = Stop
	ActXactStart = Start

	// auxiliary
	ActTransient = "transient" // transient - in-memory only
)

// xaction begin-commit phases
const (
	ActBegin  = "begin"
	ActCommit = "commit"
	ActAbort  = "abort"
)

// Header Key conventions:
//  - starts with a prefix "ais-",
//  - all words separated with "-": no dots and underscores.
const (
	headerPrefix = "ais-"

	// Bucket props headers.
	HdrBucketProps      = headerPrefix + "bucket-props"
	HdrOrigURLBck       = headerPrefix + "original-url"       // See: BucketProps.Extra.HTTP.OrigURLBck
	HdrCloudRegion      = headerPrefix + "cloud-region"       // See: BucketProps.Extra.AWS.CloudRegion
	HdrBucketVerEnabled = headerPrefix + "versioning-enabled" // Enable/disable object versioning in a bucket.
	HdrBucketCreated    = headerPrefix + "created"            // Bucket creation time.
	HdrBackendProvider  = headerPrefix + "provider"           // ProviderAmazon et al. - see cmn/bucket.go.

	HdrRemoteOffline = headerPrefix + "remote-offline" // When accessing cached remote bucket with no backend connectivity.

	// Object props headers.
	HdrObjCksumType = headerPrefix + "checksum-type"  // Checksum type, one of SupportedChecksums().
	HdrObjCksumVal  = headerPrefix + "checksum-value" // Checksum value.
	HdrObjAtime     = headerPrefix + "atime"          // Object access time.
	HdrObjCustomMD  = headerPrefix + "custom-md"      // Object custom metadata.
	HdrObjVersion   = headerPrefix + "version"        // Object version/generation - ais or cloud.

	// Append object header.
	HdrAppendHandle = headerPrefix + "append-handle"

	// Query objects handle header.
	HdrHandle = headerPrefix + "query-handle"

	// Reverse proxy headers.
	HdrNodeID  = headerPrefix + "node-id"
	HdrNodeURL = headerPrefix + "node-url"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internal header keys.
const (
	// Intra cluster headers.
	HdrCallerID          = headerPrefix + "caller-id" // Marker of intra-cluster request.
	HdrPutterID          = headerPrefix + "putter-id" // Marker of inter-cluster PUT object request.
	HdrCallerName        = headerPrefix + "caller-name"
	HdrCallerSmapVersion = headerPrefix + "caller-smap-ver"

	HdrXactionID = headerPrefix + "xaction-id"

	// Stream related headers.
	HdrSessID   = headerPrefix + "session-id"
	HdrCompress = headerPrefix + "compress" // LZ4Compression, etc.

	// Promote(dir)
	HdrPromoteNamesHash = headerPrefix + "promote-names-hash"
	HdrPromoteNamesNum  = headerPrefix + "promote-names-num"
)

// Configuration and bucket properties
const (
	PropBucketAccessAttrs  = "access"             // Bucket access attributes.
	PropBucketVerEnabled   = "versioning.enabled" // Enable/disable object versioning in a bucket.
	PropBucketCreated      = "created"            // Bucket creation time.
	PropBackendBck         = "backend_bck"
	PropBackendBckName     = PropBackendBck + ".name"
	PropBackendBckProvider = PropBackendBck + ".provider"
)

// HeaderCompress enum (supported compression algorithms)
const (
	LZ4Compression = "lz4"
)

// URL Query "?name1=val1&name2=..."
// Query parameter name conventions:
//  - contains only alpha-numeric characters,
//  - words must be separated with underscore "_".

// User/client query params.
const (
	URLParamWhat        = "what"           // "smap" | "bmd" | "config" | "stats" | "xaction" ...
	URLParamProps       = "props"          // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...
	URLParamNewCustom   = "set-new-custom" // remove existing custom keys (if any) and store new custom metadata
	URLParamCheckExists = "check_cached"   // true: check if object exists (aka "cached", "present")
	URLParamUUID        = "uuid"
	URLParamRegex       = "regex" // dsort/downloader regex

	// Bucket related query params.
	URLParamProvider  = "provider" // backend provider
	URLParamNamespace = "namespace"
	URLParamBucketTo  = "bck_to"
	URLParamKeepBckMD = "keep_md"

	// Object related query params.
	URLParamAppendType   = "append_type"
	URLParamAppendHandle = "append_handle"

	// HTTP bucket support.
	URLParamOrigURL = "original_url"

	// Log severity
	URLParamSev = "severity" // see { LogInfo, ...} enum

	// Archive filename and format (mime type)
	URLParamArchpath = "archpath"
	URLParamArchmime = "archmime"

	// Skip loading existing object's metadata in order to
	// compare its Checksum and update its existing Version (if exists);
	// can be used to reduce PUT latency when:
	// - we massively write a new content into a bucket, and/or
	// - we simply don't care.
	URLParamSkipVC = "skip_vc"
)

// health
const (
	URLParamHealthReadiness = "readiness" // to be used by external watchdogs (e.g. K8s)
	URLParamAskPrimary      = "apr"       // true: the caller is directing health request to primary
	URLParamPrimaryReadyReb = "prr"       // true: check whether primary is ready to start rebalancing cluster
)

// Internal query params.
const (
	URLParamCheckExistsAny   = "cea" // true: lookup object in all mountpaths (NOTE: compare with URLParamCheckExists)
	URLParamProxyID          = "pid" // ID of the redirecting proxy.
	URLParamPrimaryCandidate = "can" // ID of the candidate for the primary proxy.
	URLParamPrepare          = "prp" // true: request belongs to the "prepare" phase of the primary proxy election
	URLParamNonElectable     = "nel" // true: proxy is non-electable for the primary role
	URLParamUnixTime         = "utm" // Unix time since 01/01/70 UTC (nanoseconds)
	URLParamIsGFNRequest     = "gfn" // true if the request is a Get-From-Neighbor
	URLParamSilent           = "sln" // true: destination should not log errors (HEAD request)
	URLParamRebStatus        = "rbs" // true: get detailed rebalancing status
	URLParamRebData          = "rbd" // true: get EC rebalance data (pulling data if push way fails)
	URLParamTaskAction       = "tac" // "start", "status", "result"
	URLParamClusterInfo      = "cii" // true: /Health to return cluster info and status
	URLParamOWT              = "owt" // object write transaction enum { OwtPut, ..., OwtGet* }

	// force the operation; allows to overcome certain restrictions (e.g., shutdown primary and the entire cluster)
	// or errors (e.g., attach invalid mountpath)
	URLParamForce = "frc"

	URLParamDontLookupRemoteBck = "dntlrb" // true: do not try to lookup remote buckets on the fly (overrides the default)
	URLParamDontResilver        = "dntres" // true: do not resilver data off of mountpaths that are being disabled/detached

	// dsort
	URLParamTotalCompressedSize       = "tcs"
	URLParamTotalInputShardsExtracted = "tise"
	URLParamTotalUncompressedSize     = "tunc"

	// 2PC transactions - control plane
	URLParamNetwTimeout  = "xnt" // [begin, start-commit] timeout
	URLParamHostTimeout  = "xht" // [begin, txn-done] timeout
	URLParamWaitMetasync = "xwm" // true: wait for metasync (used only when there's an alternative)

	// promote(dir)
	URLParamPromoteFileShare = "prmshr"

	// Notification target's node ID (usually, the node that initiates the operation).
	URLParamNotifyMe = "nft"
)

// URLParamAppendType enum
const (
	AppendOp = "append"
	FlushOp  = "flush"
)

// URLParamTaskAction enum
const (
	TaskStart  = Start
	TaskStatus = "status"
	TaskResult = "result"
)

// URLParamWhat enum

// User/client "what" values.
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
	GetWhatStatus        = "status" // IC status by uuid.
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

// URLParamSev enum
const (
	LogInfo = "info"
	LogWarn = "warning"
	LogErr  = "error"
)

// BucketEntry.Flags field
const (
	EntryStatusBits = 5                          // N bits
	EntryStatusMask = (1 << EntryStatusBits) - 1 // mask for N low bits
)

const (
	// Status
	ObjStatusOK = iota
	ObjStatusMovedNode
	ObjStatusMovedMpath
	ObjStatusDeleted // TODO: reserved for future when we introduce delayed delete of the object/bucket

	// Flags
	EntryIsCached = 1 << (EntryStatusBits + 1)
	EntryInArch   = 1 << (EntryStatusBits + 2)
)

// List objects default page size
const (
	DefaultListPageSizeAIS = 10000
)

// RESTful URL path: l1/l2/l3
const (
	// l1
	Version = "v1"
	// l2
	Buckets   = "buckets"
	Objects   = "objects"
	EC        = "ec"
	Download  = "download"
	Daemon    = "daemon"
	Cluster   = "cluster"
	Tokens    = "tokens"
	Metasync  = "metasync"
	Health    = "health"
	Vote      = "vote"
	ObjStream = "objstream"
	MsgStream = "msgstream"
	Reverse   = "reverse"
	Rebalance = "rebalance"
	Xactions  = "xactions"
	S3        = "s3"
	Txn       = "txn"      // 2PC
	Notifs    = "notifs"   // intra-cluster notifications
	Users     = "users"    // AuthN
	Clusters  = "clusters" // AuthN
	Roles     = "roles"    // AuthN
	IC        = "ic"       // information center

	// l3
	SyncSmap = "syncsmap" // legacy

	Voteres    = "result"
	VoteInit   = "init"
	Mountpaths = "mountpaths"

	// (see the corresponding action messages above)
	Keepalive      = "keepalive"
	AdminJoin      = "join-by-admin"   // when node is joined by admin ("manual join")
	SelfJoin       = "autoreg"         // auto-join cluster at startup
	CallbackRmSelf = "cb-rm-from-smap" // set by primary to request that node calls back to request removal (internal use only!)

	// common
	Init     = "init"
	Start    = "start"
	Stop     = "stop"
	Abort    = "abort"
	Sort     = "sort"
	Finished = "finished"
	Progress = "progress"

	// dSort, downloader, query
	Metrics     = "metrics"
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished_ack"
	List        = "list"
	Remove      = "remove"
	Next        = "next"
	Peek        = "peek"
	Discard     = "discard"
	WorkerOwner = "worker" // TODO: it should be removed once get-next-bytes endpoint is ready

	// ETL
	ETL         = "etl"
	ETLInitSpec = "init_spec"
	ETLInitCode = "init_code"
	ETLInfo     = "info"
	ETLList     = List
	ETLLogs     = "logs"
	ETLObject   = "_object"
	ETLStop     = Stop
	ETLHealth   = "health"
)

const (
	Proxy  = "proxy"
	Target = "target"
)

// Compression enum
const (
	CompressAlways = "always"
	CompressNever  = "never"
)

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)

// metadata write policy
type MDWritePolicy string

func (mdw MDWritePolicy) IsImmediate() bool { return mdw == WriteDefault || mdw == WriteImmediate }

const (
	WriteImmediate = MDWritePolicy("immediate") // immediate write (default)
	WriteDelayed   = MDWritePolicy("delayed")   // cache and flush when not accessed for a while (lom_cache_hk.go)
	WriteNever     = MDWritePolicy("never")     // transient - in-memory only

	WriteDefault = MDWritePolicy("") // equivalent to immediate writing (WriteImmediate)
)

var (
	SupportedWritePolicy = []string{string(WriteImmediate), string(WriteDelayed), string(WriteNever)}
	SupportedCompression = []string{CompressNever, CompressAlways}
)
