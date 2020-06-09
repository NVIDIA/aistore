// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"
)

// checksums
const (
	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"
	ChecksumCRC32C = "crc32c"
	ChecksumSHA256 = "sha256" // crypto.SHA512_256 (SHA-2)
	ChecksumSHA512 = "sha512" // crypto.SHA512 (SHA-2)
)

// module names
const (
	DSortName          = "dSort"
	DSortNameLowercase = "dsort"
)

const (
	AppendOp = "append"
	FlushOp  = "flush"
)

// ActionMsg.Action enum (includes xactions)
const (
	ActShutdown      = "shutdown"
	ActRebalance     = "rebalance"
	ActResilver      = "resilver"
	ActLRU           = "lru"
	ActSyncLB        = "synclb"
	ActCreateLB      = "createlb"
	ActDestroyLB     = "destroylb"
	ActRenameLB      = "renamelb"
	ActCopyBucket    = "copybck"
	ActRegisterCB    = "registercb"
	ActEvictCB       = "evictcb"
	ActSetConfig     = "setconfig"
	ActSetBprops     = "setbprops"
	ActResetBprops   = "resetbprops"
	ActListObjects   = "listobj"
	ActSummaryBucket = "summarybck"
	ActRenameObject  = "renameobj"
	ActPromote       = "promote"
	ActEvictObjects  = "evictobj"
	ActDelete        = "delete"
	ActPrefetch      = "prefetch"
	ActDownload      = "download"
	ActRegTarget     = "regtarget"
	ActRegProxy      = "regproxy"
	ActUnregTarget   = "unregtarget"
	ActUnregProxy    = "unregproxy"
	ActNewPrimary    = "newprimary"
	ActRevokeToken   = "revoketoken"
	ActElection      = "election"
	ActPutCopies     = "putcopies"
	ActMakeNCopies   = "makencopies"
	ActLoadLomCache  = "loadlomcache"
	ActECGet         = "ecget"    // erasure decode objects
	ActECPut         = "ecput"    // erasure encode objects
	ActECRespond     = "ecresp"   // respond to other targets' EC requests
	ActECEncode      = "ecencode" // erasure code a bucket
	ActStartGFN      = "metasync-start-gfn"
	ActRecoverBck    = "recoverbck"
	ActAsyncTask     = "task"
	ActTar2Tf        = "tar2tf"
	ActAttach        = "attach"
	ActDetach        = "detach"
	ActQuery         = "query"

	// Actions to manipulate mountpaths (/v1/daemon/mountpaths)
	ActMountpathEnable  = "enable"
	ActMountpathDisable = "disable"
	ActMountpathAdd     = "add"
	ActMountpathRemove  = "remove"

	// Actions on xactions
	ActXactStop  = "stop"
	ActXactStart = "start"

	// auxiliary
	ActTransient = "transient" // do not save on the disk
)

// xaction begin-commit phases
const (
	ActBegin  = "begin"
	ActCommit = "commit"
	ActAbort  = "abort"
)

// Header Key enum - conventions:
// - the constant equals the path of a value in BucketProps structure
// - if a property is a root one, then the constant is just a lowercased property name
// - if a property is nested, then its value is property's parent and property name separated with a dot
const (
	HeaderBackendBck         = "backend_bck"
	HeaderBackendBckName     = HeaderBackendBck + ".name"
	HeaderBackendBckProvider = HeaderBackendBck + "." + HeaderCloudProvider

	HeaderCloudProvider    = "provider"           // ProviderAmazon et al. - see cmn/bucket.go
	HeaderCloudOffline     = "cloud.offline"      // when accessing cached cloud bucket with no Cloud connectivity
	HeaderRemoteAisOffline = "remote.ais.offline" // when accessing cached cloud bucket with no Cloud connectivity
	HeaderGuestAccess      = "guest"              // AuthN is enabled and proxy gets a request without token

	// bucket props
	HeaderBucketVerEnabled      = "versioning.enabled"           // Enable/disable object versioning in a bucket
	HeaderBucketVerValidateWarm = "versioning.validate_warm_get" // Validate version on warm GET
	HeaderBucketAccessAttrs     = "access"                       // Bucket access attributes
	HeaderBucketCreated         = "created"                      // Bucket creation time

	// object meta
	HeaderObjCksumType = "checksum.type"  // Checksum Type, one of SupportedChecksums()
	HeaderObjCksumVal  = "checksum.value" // Checksum Value
	HeaderObjAtime     = "atime"          // Object access time
	HeaderObjCustomMD  = "custom_md"      // Object custom metadata
	HeaderObjSize      = "size"           // Object size (bytes)
	HeaderObjVersion   = "version"        // Object version/generation - ais or Cloud
	HeaderObjECMeta    = "ec_meta"        // Info about EC object/slice/replica

	// intra-cluster: control
	HeaderCallerID          = "caller.id"
	HeaderCallerName        = "caller.name"
	HeaderCallerSmapVersion = "caller.smap.ver"

	HeaderNodeID  = "node.id"
	HeaderNodeURL = "node.url"

	// custom
	HeaderAppendHandle = "append.handle"

	// intra-cluster: streams
	HeaderSessID   = "session.id"
	HeaderCompress = "compress" // LZ4Compression, etc.
)

// supported compressions (alg-s)
const (
	LZ4Compression = "lz4"
)

// URL Query "?name1=val1&name2=..."
const (
	// user/app API
	URLParamWhat        = "what"         // "smap" | "bmd" | "config" | "stats" | "xaction" ...
	URLParamProps       = "props"        // e.g. "checksum, size"|"atime, size"|"cached"|"bucket, size"| ...
	URLParamCheckExists = "check_cached" // true: check if object exists
	URLParamProvider    = "provider"     // cloud provider
	URLParamNamespace   = "namespace"
	URLParamPrefix      = "prefix" // prefix for list objects in a bucket
	URLParamRegex       = "regex"  // dsort/downloader regex
	// internal use
	URLParamCheckExistsAny   = "cea" // true: lookup object in all mountpaths (NOTE: compare with URLParamCheckExists)
	URLParamProxyID          = "pid" // ID of the redirecting proxy
	URLParamPrimaryCandidate = "can" // ID of the candidate for the primary proxy
	URLParamForce            = "frc" // true: force the operation (e.g., shutdown primary and the entire cluster)
	URLParamPrepare          = "prp" // true: request belongs to the "prepare" phase of the primary proxy election
	URLParamNonElectable     = "nel" // true: proxy is non-electable for the primary role
	URLParamUnixTime         = "utm" // Unix time: number of nanoseconds elapsed since 01/01/70 UTC
	URLParamIsGFNRequest     = "gfn" // true if the request is a Get-From-Neighbor
	URLParamSilent           = "sln" // true: destination should not log errors (HEAD request)
	URLParamRebStatus        = "rbs" // true: get detailed rebalancing status
	URLParamRebData          = "rbd" // true: get EC rebalance data (pulling data if push way fails)
	URLParamTaskID           = "tsk" // ID of a task to return its state/result
	URLParamTaskAction       = "tac" // "start", "status", "result"
	URLParamECMeta           = "ecm" // true: EC metadata request
	URLParamClusterInfo      = "cii" // true: Health to return ais.clusterInfo

	URLParamAppendType   = "appendty"
	URLParamAppendHandle = "handle"

	// dsort
	URLParamTotalCompressedSize       = "tcs"
	URLParamTotalInputShardsExtracted = "tise"
	URLParamTotalUncompressedSize     = "tunc"

	// downloader - FIXME: name collisions, consistency, usage beyond downloader
	URLParamBucket            = "bucket" // FIXME
	URLParamID                = "id"     // FIXME
	URLParamLink              = "link"
	URLParamObjName           = "objname" // FIXME
	URLParamSuffix            = "suffix"
	URLParamTemplate          = "template"
	URLParamSubdir            = "subdir"
	URLParamTimeout           = "timeout" // FIXME
	URLParamDescription       = "description"
	URLParamLimitConnections  = "limit_conn"
	URLParamLimitBytesPerHour = "limit_bph"

	// 2PC (control plane)
	URLParamTxnTimeout = "txntout" // transaction timeout
	URLParamTxnEvent   = "txnevnt" // enum { txnCommitEvent* }
)

// enum: task action (cmn.URLParamTaskAction)
const (
	TaskStart  = "start"
	TaskStatus = "status"
	TaskResult = "result"
)

// URLParamWhat enum
const (
	GetWhatConfig        = "config"
	GetWhatSmap          = "smap"
	GetWhatBMD           = "bmd"
	GetWhatStats         = "stats"
	GetWhatXactStats     = "xstats"
	GetWhatXactRunStatus = "xrunstatus"
	GetWhatSmapVote      = "smapvote"
	GetWhatMountpaths    = "mountpaths"
	GetWhatSnode         = "snode"
	GetWhatSysInfo       = "sysinfo"
	GetWhatDiskStats     = "disk"
	GetWhatDaemonStatus  = "status"
	GetWhatRemoteAIS     = "remote"
)

// SelectMsg.TimeFormat enum
const (
	RFC822 = time.RFC822
)

// SelectMsg.Props enum
const (
	GetPropsName     = "name"
	GetPropsChecksum = "checksum"
	GetPropsSize     = "size"
	GetPropsAtime    = "atime"
	GetPropsCached   = "cached"
	GetPropsVersion  = "version"
	GetTargetURL     = "target_url"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
	GetPropsEC       = "ec"
)

// BucketEntry.Status
const (
	ObjStatusOK = iota
	ObjStatusMoved
	ObjStatusDeleted // TODO: reserved for future when we introduce delayed delete of the object/bucket
)

// BucketEntry Flags constants
const (
	EntryStatusBits = 5                          // N bits
	EntryStatusMask = (1 << EntryStatusBits) - 1 // mask for N low bits
	EntryIsCached   = 1 << (EntryStatusBits + 1) // StatusMaskBits + 1
)

// List objects default page size
const (
	DefaultListPageSize = uint(1000)
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
	Transport = "transport"
	Reverse   = "reverse"
	Rebalance = "rebalance"
	Txn       = "txn" // 2PC
	Xactions  = "xactions"
	Tar2Tf    = "tar2tf"
	S3        = "s3"
	Users     = "users"    // AuthN
	Clusters  = "clusters" // AuthN
	Roles     = "roles"    // AuthN

	// l3
	SyncSmap     = "syncsmap"
	Keepalive    = "keepalive"
	UserRegister = "register" // node register by admin (manual)
	AutoRegister = "autoreg"  // node register itself into the primary proxy (automatic)
	Unregister   = "unregister"
	Proxy        = "proxy"
	Voteres      = "result"
	VoteInit     = "init"
	Mountpaths   = "mountpaths"
	Summary      = "summary"
	AllBuckets   = "*"

	// dSort and downloader
	Init        = "init"
	Sort        = "sort"
	Start       = "start"
	Abort       = "abort"
	Metrics     = "metrics"
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished-ack"
	List        = "list"
	Remove      = "remove"

	// CLI
	Target = "target"

	// tar2tf
	GetTargetObjects = "objects"
)

// enum: compression
const (
	CompressAlways = "always"
	CompressNever  = "never"
	CompressRatio  = "ratio=%d" // adaptive: min ratio that warrants compression
)

// AuthN consts
const (
	HeaderAuthorization = "Authorization"
	HeaderBearer        = "Bearer"
)

// timeouts for intra-cluster requests
const (
	DefaultTimeout = time.Duration(-1)
	LongTimeout    = time.Duration(-2)
)
