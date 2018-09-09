/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package api

import (
	"time"
)

// ActionMsg is a JSON-formatted control structures
type ActionMsg struct {
	Action string      `json:"action"` // shutdown, restart, setconfig - the enum below
	Name   string      `json:"name"`   // action-specific params
	Value  interface{} `json:"value"`
}

// ActionMsg.Action enum
const (
	ActShutdown    = "shutdown"
	ActGlobalReb   = "rebalance"      // global rebalance between targets
	ActLocalReb    = "localrebalance" // local rebalance on single target
	ActRechecksum  = "rechecksum"
	ActLRU         = "lru"
	ActSyncLB      = "synclb"
	ActCreateLB    = "createlb"
	ActDestroyLB   = "destroylb"
	ActRenameLB    = "renamelb"
	ActSetConfig   = "setconfig"
	ActSetProps    = "setprops"
	ActListObjects = "listobjects"
	ActRename      = "rename"
	ActEvict       = "evict"
	ActDelete      = "delete"
	ActPrefetch    = "prefetch"
	ActRegTarget   = "regtarget"
	ActRegProxy    = "regproxy"
	ActUnregTarget = "unregtarget"
	ActUnregProxy  = "unregproxy"
	ActNewPrimary  = "newprimary"
	ActRevokeToken = "revoketoken"
	ActElection    = "election"

	// Actions for manipulating mountpaths (/v1/daemon/mountpaths)
	ActMountpathEnable  = "enable"
	ActMountpathDisable = "disable"
	ActMountpathAdd     = "add"
	ActMountpathRemove  = "remove"
)

// Cloud Provider enum
const (
	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderDFC    = "dfc"
)

// Header Key enum
const (
	HeaderCloudProvider         = "CloudProvider"         // from Cloud Provider enum
	HeaderVersioning            = "Versioning"            // Versioning state for a bucket: "enabled"/"disabled"
	HeaderNextTierURL           = "NextTierURL"           // URL of the next tier in a DFC multi-tier environment
	HeaderReadPolicy            = "ReadPolicy"            // Policy used for reading in a DFC multi-tier environment
	HeaderWritePolicy           = "WritePolicy"           // Policy used for writing in a DFC multi-tier environment
	HeaderBucketChecksumType    = "BucketChecksumType"    // Checksum type used for objects in the bucket
	HeaderBucketValidateColdGet = "BucketValidateColdGet" // Cold get validation policy used for objects in the bucket
	HeaderBucketValidateWarmGet = "BucketValidateWarmGet" // Warm get validation policy used for objects in the bucket
	HeaderBucketValidateRange   = "BucketValidateRange"   // Byte range validation policy used for objects in the bucket
	HeaderDFCChecksumType       = "HeaderDfcChecksumType" // Checksum Type (xxhash, md5, none)
	HeaderDFCChecksumVal        = "HeaderDfcChecksumVal"  // Checksum Value
	HeaderDFCObjVersion         = "HeaderDfcObjVersion"   // Object version/generation
	HeaderSize                  = "Size"                  // Size of object in bytes
	HeaderVersion               = "Version"               // Object version number
)

// URL Query "?name1=val1&name2=..."
const (
	// user/app API
	URLParamWhat        = "what"         // "smap" | "bucketmd" | "config" | "stats" | "xaction" ...
	URLParamProps       = "props"        // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size" | xaction type
	URLParamCheckCached = "check_cached" // true: check if object is cached in DFC
	URLParamOffset      = "offset"       // Offset from where the object should be read
	URLParamLength      = "length"       // the total number of bytes that need to be read from the offset
	// internal use
	URLParamLocal            = "loc" // true: bucket is local
	URLParamFromID           = "fid" // source target ID
	URLParamToID             = "tid" // destination target ID
	URLParamProxyID          = "pid" // ID of the redirecting proxy
	URLParamPrimaryCandidate = "can" // ID of the candidate for the primary proxy
	URLParamCached           = "cho" // true: return cached objects (names & metadata); false: list Cloud bucket
	URLParamForce            = "frc" // true: force the operation (e.g., shutdown primary proxy and the entire cluster)
	URLParamPrepare          = "prp" // true: request belongs to the "prepare" phase of the primary proxy election
	URLParamNonElectable     = "nel" // true: proxy is non-electable for the primary role
	URLParamSmapVersion      = "vsm" // Smap version
	URLParamBMDVersion       = "vbm" // version of the bucket-metadata
	URLParamUnixTime         = "utm" // Unix time: number of nanoseconds elapsed since 01/01/70 UTC
	URLParamReadahead        = "rah" // Proxy to target: readeahed
)

// TODO: sort and some props are TBD
// GetMsg represents properties and options for requests which fetch entities
type GetMsg struct {
	GetSort       string `json:"sort"`        // "ascending, atime" | "descending, name"
	GetProps      string `json:"props"`       // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size"
	GetTimeFormat string `json:"time_format"` // "RFC822" default - see the enum below
	GetPrefix     string `json:"prefix"`      // object name filter: return only objects which name starts with prefix
	GetPageMarker string `json:"pagemarker"`  // AWS/GCP: marker
	GetPageSize   int    `json:"pagesize"`    // maximum number of entries returned by list bucket call
}

// RangeListMsgBase contains fields common to Range and List operations
type RangeListMsgBase struct {
	Deadline time.Duration `json:"deadline,omitempty"`
	Wait     bool          `json:"wait,omitempty"`
}

// ListMsg contains a list of files and a duration within which to get them
type ListMsg struct {
	RangeListMsgBase
	Objnames []string `json:"objnames"`
}

// RangeMsg contains a Prefix, Regex, and Range for a Range Operation
type RangeMsg struct {
	RangeListMsgBase
	Prefix string `json:"prefix"`
	Regex  string `json:"regex"`
	Range  string `json:"range"`
}

// MountpathList contains two lists:
// * Available - the list of mountpaths that can be utilized by DFC
// * Disabled - the list of disabled mountpaths, mountpaths that triggered
//	            IO errors and after extra tests are found faulty
type MountpathList struct {
	Available []string `json:"available"`
	Disabled  []string `json:"disabled"`
}

//===================
//
// RESTful GET
//
//===================

// URLParamWhat enum
const (
	GetWhatConfig     = "config"
	GetWhatSmap       = "smap"
	GetWhatBucketMeta = "bucketmd"
	GetWhatStats      = "stats"
	GetWhatXaction    = "xaction"
	GetWhatSmapVote   = "smapvote"
	GetWhatMountpaths = "mountpaths"
	GetWhatDaemonInfo = "daemoninfo"
)

// GetMsg.GetSort enum
const (
	GetSortAsc = "ascending"
	GetSortDes = "descending"
)

// GetMsg.GetTimeFormat enum
const (
	RFC822     = time.RFC822     // default
	Stamp      = time.Stamp      // e.g. "Jan _2 15:04:05"
	StampMilli = time.StampMilli // e.g. "Jan 12 15:04:05.000"
	RFC822Z    = time.RFC822Z
	RFC1123    = time.RFC1123
	RFC1123Z   = time.RFC1123Z
	RFC3339    = time.RFC3339
)

// GetMsg.GetProps enum
const (
	GetPropsChecksum = "checksum"
	GetPropsSize     = "size"
	GetPropsAtime    = "atime"
	GetPropsCtime    = "ctime"
	GetPropsIsCached = "iscached"
	GetPropsBucket   = "bucket"
	GetPropsVersion  = "version"
	GetTargetURL     = "targetURL"
)

//===================
//
// Bucket Listing <= GET /bucket result set
//
//===================

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the GetMsg
type BucketEntry struct {
	Name      string `json:"name"`                // name of the object - note: does not include the bucket name
	Size      int64  `json:"size"`                // size in bytes
	Ctime     string `json:"ctime"`               // formatted as per GetMsg.GetTimeFormat
	Checksum  string `json:"checksum"`            // checksum
	Type      string `json:"type"`                // "file" OR "directory"
	Atime     string `json:"atime"`               // formatted as per GetMsg.GetTimeFormat
	Bucket    string `json:"bucket"`              // parent bucket name
	Version   string `json:"version"`             // version/generation ID. In GCP it is int64, in AWS it is a string
	IsCached  bool   `json:"iscached"`            // if the file is cached on one of targets
	TargetURL string `json:"targetURL,omitempty"` // URL of target which has the entry
}

// BucketList represents the contents of a given bucket - somewhat analogous to the 'ls <bucket-name>'
type BucketList struct {
	Entries    []*BucketEntry `json:"entries"`
	PageMarker string         `json:"pagemarker"`
}

// BucketNames is used to transfer all bucket names known to the system
type BucketNames struct {
	Cloud []string `json:"cloud"`
	Local []string `json:"local"`
}

const (
	// DefaultPageSize determines the number of cached file infos returned in one page
	DefaultPageSize = 1000
)

// RESTful URL path: /v1/....
const (
	Version    = "v1"
	Buckets    = "buckets"
	Objects    = "objects"
	Cluster    = "cluster"
	Daemon     = "daemon"
	SyncSmap   = "syncsmap"
	Push       = "push"
	Keepalive  = "keepalive"
	Register   = "register"
	Unregister = "unregister"
	Health     = "health"
	Vote       = "vote"
	Proxy      = "proxy"
	Voteres    = "result"
	VoteInit   = "init"
	Tokens     = "tokens"
	Metasync   = "metasync"
	Mountpaths = "mountpaths"
)

const (
	// Used by various Xaction APIs
	XactionRebalance = ActGlobalReb
	XactionPrefetch  = ActPrefetch

	// Denote the status of an Xaction
	XactionStatusInProgress = "InProgress"
	XactionStatusCompleted  = "Completed"
)
