// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"
)

// enum: http header, checksum, versioning
const (
	// FS extended attributes
	XattrLOM = "user.ais.lom"
	XattrBMD = "user.ais.bmd"

	// checksum hash function
	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"
	ChecksumCRC32C = "crc32c"
)

// module names
const (
	DSortName          = "dSort"
	DSortNameLowercase = "dsort"
)

// bucket property type (used by cksum, versioning)
const (
	PropInherit = "inherit" // inherit bucket property from global config
	PropOwn     = "own"     // bucket has its own configuration
)

// ActionMsg.Action enum (includes xactions)
const (
	ActShutdown     = "shutdown"
	ActGlobalReb    = "rebalance" // global cluster-wide rebalance
	ActLocalReb     = "resilver"  // local rebalance aka resilver
	ActLRU          = "lru"
	ActSyncLB       = "synclb"
	ActCreateLB     = "createlb"
	ActDestroyLB    = "destroylb"
	ActRenameLB     = "renamelb"
	ActCopyLB       = "copylb"
	ActRegisterCB   = "registercb"
	ActEvictCB      = "evictcb"
	ActResetProps   = "resetprops"
	ActSetConfig    = "setconfig"
	ActSetProps     = "setprops"
	ActListObjects  = "listobjects"
	ActRename       = "rename"
	ActReplicate    = "replicate"
	ActEvictObjects = "evictobjects"
	ActDelete       = "delete"
	ActPrefetch     = "prefetch"
	ActDownload     = "download"
	ActRegTarget    = "regtarget"
	ActRegProxy     = "regproxy"
	ActUnregTarget  = "unregtarget"
	ActUnregProxy   = "unregproxy"
	ActNewPrimary   = "newprimary"
	ActRevokeToken  = "revoketoken"
	ActElection     = "election"
	ActPutCopies    = "putcopies"
	ActMakeNCopies  = "makencopies"
	ActLoadLomCache = "loadlomcache"
	ActECGet        = "ecget"  // erasure decode objects
	ActECPut        = "ecput"  // erasure encode objects
	ActECRespond    = "ecresp" // respond to other targets' EC requests
	ActStartGFN     = "metasync-start-gfn"
	ActRecoverBck   = "recoverbck"
	ActAsyncTask    = "task"

	// Actions for manipulating mountpaths (/v1/daemon/mountpaths)
	ActMountpathEnable  = "enable"
	ActMountpathDisable = "disable"
	ActMountpathAdd     = "add"
	ActMountpathRemove  = "remove"

	// Actions for xactions
	ActXactStats = "stats"
	ActXactStop  = "stop"
	ActXactStart = "start"

	// auxiliary actions
	ActPersist = "persist" // store a piece of metadata or configuration
)

// xaction begin-commit phases
const (
	ActBegin  = "begin"
	ActCommit = "commit"
	ActAbort  = "abort"
)

// Header Key enum - conventions:
// - the constant equals the path of a value in BucketProps structure
// - if a property is a root one, then the constant is just a lowercased propery name
// - if a property is nested, then its value is propertie's parent and propery name separated with a dash
const (
	HeaderCloudProvider = "cloud_provider" // ProviderAmazon et al. - see provider.go

	// tiering
	HeaderNextTierURL = "tier.next_url"     // URL of the next tier in a AIStore multi-tier environment
	HeaderReadPolicy  = "tier.read_policy"  // Policy used for reading in a AIStore multi-tier environment
	HeaderWritePolicy = "tier.write_policy" // Policy used for writing in a AIStore multi-tier environment

	// bucket props
	HeaderBucketChecksumType    = "cksum.type"              // Checksum type used for objects in the bucket
	HeaderBucketValidateColdGet = "cksum.validate_cold_get" // Validate checksum on cold GET
	HeaderBucketValidateWarmGet = "cksum.validate_warm_get" // Validate checksum on warm GET
	HeaderBucketValidateObjMove = "cksum.validate_obj_move" // Validate checksum upon intra-cluster migration
	HeaderBucketEnableReadRange = "cksum.enable_read_range" // Return range checksum (otherwise, return the obj's)
	HeaderBucketVerEnabled      = "ver.enabled"             // Enable/disable object versioning in a bucket
	HeaderBucketVerValidateWarm = "ver.validate_warm_get"   // Validate version on warm GET
	HeaderBucketLRUEnabled      = "lru.enabled"             // LRU is run on a bucket only if this field is true
	HeaderBucketLRULowWM        = "lru.lowwm"               // Capacity usage low water mark
	HeaderBucketLRUHighWM       = "lru.highwm"              // Capacity usage high water mark
	HeaderBucketDontEvictTime   = "lru.dont_evict_time"     // eviction-free time period [atime, atime+dontevicttime]
	HeaderBucketCapUpdTime      = "lru.capacity_upd_time"   // Minimum time to update the capacity
	HeaderBucketMirrorEnabled   = "mirror.enabled"          // will only generate local copies when set to true
	HeaderBucketCopies          = "mirror.copies"           // # local copies
	HeaderBucketMirrorThresh    = "mirror.util_thresh"      // same util when below this threshold
	HeaderBucketECEnabled       = "ec.enabled"              // EC is on for a bucket
	HeaderBucketECMinSize       = "ec.objsize_limit"        // Objects under MinSize copied instead of being EC'ed
	HeaderBucketECData          = "ec.data_slices"          // number of data chunks for EC
	HeaderBucketECParity        = "ec.parity_slices"        // number of parity chunks for EC/copies for small files
	HeaderBucketECCompression   = "ec.compression"          // EC stream compression settings
	HeaderBucketAccessAttrs     = "aattrs"                  // Bucket access attributes

	// object meta
	HeaderObjCksumType  = "ObjCksumType"  // Checksum Type (xxhash, md5, none)
	HeaderObjCksumVal   = "ObjCksumVal"   // Checksum Value
	HeaderObjAtime      = "ObjAtime"      // Object access time
	HeaderObjReplicSrc  = "ObjReplicSrc"  // In replication PUT request specifies the source target
	HeaderObjSize       = "ObjSize"       // Object size (bytes)
	HeaderObjVersion    = "ObjVersion"    // Object version/generation - local or Cloud
	HeaderObjPresent    = "ObjPresent"    // Is object present in the cluster
	HeaderObjNumCopies  = "ObjNumCopies"  // Number of copies of the object
	HeaderObjIsBckLocal = "ObjIsBckLocal" // Is object from a local bucket

	// intra-cluster: control
	HeaderCallerID          = "caller.id"
	HeaderCallerName        = "caller.name"
	HeaderCallerSmapVersion = "caller.smap.ver"

	HeaderNodeID  = "node.id"
	HeaderNodeURL = "node.url"

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
	URLParamWhat        = "what"         // "smap" | "bucketmd" | "config" | "stats" | "xaction" ...
	URLParamProps       = "props"        // e.g. "checksum, size"|"atime, size"|"iscached"|"bucket, size"| ...
	URLParamCheckCached = "check_cached" // true: check if object is cached in AIStore
	URLParamOffset      = "offset"       // Offset from where the object should be read
	URLParamLength      = "length"       // the total number of bytes that need to be read from the offset
	URLParamBckProvider = "bprovider"    // "local" | "cloud"
	URLParamPrefix      = "prefix"       // prefix for list objects in a bucket
	URLParamRegex       = "regex"        // dsort/downloader regex
	// internal use
	URLParamProxyID          = "pid" // ID of the redirecting proxy
	URLParamPrimaryCandidate = "can" // ID of the candidate for the primary proxy
	URLParamCached           = "cho" // true: return cached objects (names & metadata); false: list Cloud bucket
	URLParamForce            = "frc" // true: force the operation (e.g., shutdown primary and the entire cluster)
	URLParamPrepare          = "prp" // true: request belongs to the "prepare" phase of the primary proxy election
	URLParamNonElectable     = "nel" // true: proxy is non-electable for the primary role
	URLParamSmapVersion      = "vsm" // Smap version
	URLParamBMDVersion       = "vbm" // version of the bucket-metadata
	URLParamUnixTime         = "utm" // Unix time: number of nanoseconds elapsed since 01/01/70 UTC
	URLParamReadahead        = "rah" // Proxy to target: readeahed
	URLParamIsGFNRequest     = "gfn" // true if the request is a Get-From-Neighbor
	URLParamSilent           = "sln" // true: destination should not log errors (HEAD request)
	URLParamRebStatus        = "rbs" // true: get detailed rebalancing status
	URLParamTaskID           = "tsk" // ID of a task to return its state/result
	URLParamTaskAction       = "tac" // "start", "status", "result"

	// dsort
	URLParamTotalCompressedSize       = "tcs"
	URLParamTotalInputShardsExtracted = "tise"
	URLParamTotalUncompressedSize     = "tunc"

	// downloader
	URLParamBucket      = "bucket"
	URLParamID          = "id"
	URLParamLink        = "link"
	URLParamObjName     = "objname"
	URLParamSuffix      = "suffix"
	URLParamTemplate    = "template"
	URLParamSubdir      = "subdir"
	URLParamTimeout     = "timeout"
	URLParamDescription = "description"
)

// enum: start and abort global (cluster) rebalancing
const (
	RebStart = "start"
	RebAbort = "abort"
)

// enum: task action (cmn.URLParamTaskAction)
const (
	ListTaskStart  = "start"
	ListTaskStatus = "status"
	ListTaskResult = "result"
)

// URLParamWhat enum
const (
	GetWhatConfig       = "config"
	GetWhatSmap         = "smap"
	GetWhatBucketMeta   = "bucketmd"
	GetWhatStats        = "stats"
	GetWhatXaction      = "xaction"
	GetWhatSmapVote     = "smapvote"
	GetWhatMountpaths   = "mountpaths"
	GetWhatSnode        = "snode"
	GetWhatSysInfo      = "sysinfo"
	GetWhatDiskStats    = "disk"
	GetWhatDaemonStatus = "status"
	GetWhatBucketMetaX  = "bucketmdxattr"
)

// SelectMsg.TimeFormat enum
const (
	RFC822     = time.RFC822
	Stamp      = time.Stamp      // e.g. "Jan _2 15:04:05"
	StampMilli = time.StampMilli // e.g. "Jan 12 15:04:05.000"
	StampMicro = time.StampMicro // e.g. "Jan _2 15:04:05.000000"
	RFC822Z    = time.RFC822Z
	RFC1123    = time.RFC1123
	RFC1123Z   = time.RFC1123Z
	RFC3339    = time.RFC3339
)

// SelectMsg.Props enum
const (
	GetPropsChecksum = "checksum"
	GetPropsSize     = "size"
	GetPropsAtime    = "atime"
	GetPropsIsCached = "iscached"
	GetPropsVersion  = "version"
	GetTargetURL     = "targetURL"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
)

// BucketEntry.Status
const (
	ObjStatusOK = iota
	ObjStatusMoved
	ObjStatusDeleted
)

// BucketEntry Flags constants
const (
	EntryStatusBits = 5                          // N bits
	EntryStatusMask = (1 << EntryStatusBits) - 1 // mask for N low bits
	EntryIsCached   = 1 << (EntryStatusBits + 1) // StatusMaskBits + 1
)

// list-bucket default page size
const (
	DefaultPageSize = 1000
)

// RESTful URL path: l1/l2/l3
const (
	// l1
	Version = "v1"
	// l2
	Buckets   = "buckets"
	Objects   = "objects"
	Download  = "download"
	Daemon    = "daemon"
	Cluster   = "cluster"
	Push      = "push"
	Tokens    = "tokens"
	Metasync  = "metasync"
	Health    = "health"
	Vote      = "vote"
	Transport = "transport"
	Reverse   = "reverse"

	// l3
	SyncSmap     = "syncsmap"
	Keepalive    = "keepalive"
	Register     = "register" // node register by admin (manual)
	AutoRegister = "autoreg"  // node register itself into the primary proxy (automatic)
	Unregister   = "unregister"
	Proxy        = "proxy"
	Voteres      = "result"
	VoteInit     = "init"
	Mountpaths   = "mountpaths"
	ListAll      = "*"

	// dSort and downloader
	Init        = "init"
	Sort        = "sort"
	Start       = "start"
	Abort       = "abort"
	Metrics     = "metrics"
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished-ack"
	List        = "list"   // lists running dsort processes
	Remove      = "remove" // removes a dsort process from history or downloader job form download list

	// CLI
	Target = "target"
)

const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

// bucket and object access attrs and [TODO] some of their popular combinations
const (
	AccessGET = 1 << iota
	AccessHEAD
	AccessPUT
	AccessColdGET
	AccessDELETE
	AccessRENAME

	AllowAnyAccess = 0
	AllowAllAccess = ^uint64(0)

	AllowAccess = "allow"
	DenyAccess  = "deny"
)

// enum: compression
const (
	CompressAlways = "always"
	CompressNever  = "never"
	CompressRatio  = "ratio=%d" // adaptive: min ratio that warrants compression
)
