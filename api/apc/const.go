// Package apc: API constants and message types
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"time"
)

// ActionMsg.Action
// includes Xaction.Kind == ActionMsg.Action (when the action is asynchronous)
const (
	ActAddRemoteBck   = "add-remote-bck" // register (existing) remote bucket into AIS
	ActCopyBck        = "copy-bck"
	ActCreateBck      = "create-bck"
	ActDecommission   = "decommission" // decommission all nodes in the cluster (cleanup system data)
	ActDestroyBck     = "destroy-bck"  // destroy bucket data and metadata
	ActSummaryBck     = "summary-bck"
	ActDownload       = "download"
	ActECEncode       = "ec-encode" // erasure code a bucket
	ActECGet          = "ec-get"    // erasure decode objects
	ActECPut          = "ec-put"    // erasure encode objects
	ActECRespond      = "ec-resp"   // respond to other targets' EC requests
	ActETLInline      = "etl-inline"
	ActETLBck         = "etl-bck"
	ActElection       = "election"
	ActEvictRemoteBck = "evict-remote-bck" // evict remote bucket's data
	ActInvalListCache = "inval-listobj-cache"
	ActLRU            = "lru"
	ActList           = "list"
	ActLoadLomCache   = "load-lom-cache"
	ActMakeNCopies    = "make-n-copies"
	ActMoveBck        = "move-bck"
	ActNewPrimary     = "new-primary"
	ActPromote        = "promote"
	ActPutCopies      = "put-copies"
	ActRebalance      = "rebalance"
	ActRenameObject   = "rename-obj"
	ActResetBprops    = "reset-bprops"
	ActResetConfig    = "reset-config"
	ActResilver       = "resilver"
	ActResyncBprops   = "resync-bprops"
	ActSetBprops      = "set-bprops"
	ActSetConfig      = "set-config"
	ActShutdown       = "shutdown"
	ActStartGFN       = "start-gfn"
	ActStoreCleanup   = "cleanup-store"

	// multi-object (via `SelectObjsMsg`)
	ActCopyObjects     = "copy-listrange"
	ActDeleteObjects   = "delete-listrange"
	ActETLObjects      = "etl-listrange"
	ActEvictObjects    = "evict-listrange"
	ActPrefetchObjects = "prefetch-listrange"
	ActArchive         = "archive" // see ArchiveMsg

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
	HeaderPrefix = "ais-"

	// Bucket props headers.
	HdrBucketProps      = HeaderPrefix + "bucket-props"
	HdrOrigURLBck       = HeaderPrefix + "original-url"       // See: BucketProps.Extra.HTTP.OrigURLBck
	HdrCloudRegion      = HeaderPrefix + "cloud-region"       // See: BucketProps.Extra.AWS.CloudRegion
	HdrBucketVerEnabled = HeaderPrefix + "versioning-enabled" // Enable/disable object versioning in a bucket.
	HdrBucketCreated    = HeaderPrefix + "created"            // Bucket creation time.
	HdrBackendProvider  = HeaderPrefix + "provider"           // ProviderAmazon et al. - see cmn/bucket.go.

	HdrRemoteOffline = HeaderPrefix + "remote-offline" // When accessing cached remote bucket with no backend connectivity.

	// Object props headers.
	HdrObjCksumType = HeaderPrefix + "checksum-type"  // Checksum type, one of SupportedChecksums().
	HdrObjCksumVal  = HeaderPrefix + "checksum-value" // Checksum value.
	HdrObjAtime     = HeaderPrefix + "atime"          // Object access time.
	HdrObjCustomMD  = HeaderPrefix + "custom-md"      // Object custom metadata.
	HdrObjVersion   = HeaderPrefix + "version"        // Object version/generation - ais or cloud.

	// Append object header.
	HdrAppendHandle = HeaderPrefix + "append-handle"

	// Query objects handle header.
	HdrHandle = HeaderPrefix + "query-handle"

	// Reverse proxy headers.
	HdrNodeID  = HeaderPrefix + "node-id"
	HdrNodeURL = HeaderPrefix + "node-url"
)

// AuthN consts
const (
	HdrAuthorization         = "Authorization" // https://developer.mozilla.org/en-US/docs/Web/HTTP/Hdrs/Authorization
	AuthenticationTypeBearer = "Bearer"
)

// Internal header keys.
const (
	// Intra cluster headers.
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

// Configuration and bucket properties
const (
	PropBucketAccessAttrs  = "access"             // Bucket access attributes.
	PropBucketVerEnabled   = "versioning.enabled" // Enable/disable object versioning in a bucket.
	PropBucketCreated      = "created"            // Bucket creation time.
	PropBackendBck         = "backend_bck"
	PropBackendBckName     = PropBackendBck + ".name"
	PropBackendBckProvider = PropBackendBck + ".provider"
)

// enum that may accompany HEAD(obj) to specify additional _nuances_:
// - object's in-cluster existence (and whether to avoid executing remote HEAD when not found)
// - thoroughness of local lookup (all mountpaths or just the designated one)
// In either case, we are not asking for the object's props.
const (
	HeadObjAvoidRemote = 1 << (iota + 2)
	HeadObjAvoidRemoteCheckAllMps
)

// URL Query "?name1=val1&name2=..."
// Query parameter name conventions:
//  - contains only alpha-numeric characters,
//  - words must be separated with underscore "_".

// User/client query params.
const (
	QparamWhat      = "what"           // "smap" | "bmd" | "config" | "stats" | "xaction" ...
	QparamProps     = "props"          // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...
	QparamNewCustom = "set-new-custom" // remove existing custom keys (if any) and store new custom metadata
	QparamUUID      = "uuid"           // xaction
	QparamRegex     = "regex"          // dsort/downloader regex

	QparamHeadObj = "head_obj" // enum { HeadObjAvoidRemote, ... } above

	// Bucket related query params.
	QparamProvider  = "provider" // backend provider
	QparamNamespace = "namespace"
	QparamBucketTo  = "bck_to"
	QparamKeepBckMD = "keep_md"

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

	// force the operation; allows to overcome certain restrictions (e.g., shutdown primary and the entire cluster)
	// or errors (e.g., attach invalid mountpath)
	QparamForce = "frc"

	QparamDontLookupRemoteBck = "dntlrb" // true: do not try to lookup remote buckets on the fly (overrides the default)
	QparamDontResilver        = "dntres" // true: do not resilver data off of mountpaths that are being disabled/detached

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

// QparamAppendType enum
const (
	AppendOp = "append"
	FlushOp  = "flush"
)

// QparamTaskAction enum
const (
	TaskStart  = Start
	TaskStatus = "status"
	TaskResult = "result"
)

// QparamWhat enum

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

// QparamSev enum
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
	ETLStart    = Start
	ETLHealth   = "health"
)

const (
	Proxy  = "proxy"
	Target = "target"
)

// deployment types
const (
	DeploymentK8s = "K8s"
	DeploymentDev = "dev"
)

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)
