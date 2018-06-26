// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import "time"

// ActionMsg is a JSON-formatted control structures
type ActionMsg struct {
	Action string      `json:"action"` // shutdown, restart, setconfig - the enum below
	Name   string      `json:"name"`   // action-specific params
	Value  interface{} `json:"value"`
}

// ActionMsg.Action enum
const (
	ActShutdown    = "shutdown"
	ActRebalance   = "rebalance"
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
)

// Cloud Provider enum
const (
	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderDfc    = "dfc"
)

// Header Key enum
const (
	CloudProvider         = "CloudProvider"         // from Cloud Provider enum
	Versioning            = "Versioning"            // Versioning state for a bucket: "enabled"/"disabled"
	NextTierURL           = "NextTierURL"           // URL of the next tier in a DFC multi-tier environment
	ReadPolicy            = "ReadPolicy"            // Policy used for reading in a DFC multi-tier environment
	WritePolicy           = "WritePolicy"           // Policy used for writing in a DFC multi-tier environment
	HeaderDfcChecksumType = "HeaderDfcChecksumType" // Checksum Type (xxhash, md5, none)
	HeaderDfcChecksumVal  = "HeaderDfcChecksumVal"  // Checksum Value
	HeaderDfcObjVersion   = "HeaderDfcObjVersion"   // Object version/generation
	HeaderPrimaryProxyURL = "PrimaryProxyURL"       // URL of Primary Proxy
	HeaderPrimaryProxyID  = "PrimaryProxyID"        // ID of Primary Proxy
	Size                  = "Size"                  // Size of object in bytes
	Version               = "Version"               // Object version number
)

// URL Query Parameter enum
const (
	URLParamLocal            = "local"        // true: bucket is expected to be local
	URLParamFromID           = "from_id"      // from_id=string - ID to copy from
	URLParamToID             = "to_id"        // to_id=string - ID to copy to
	URLParamFromName         = "from_name"    // rename from
	URLParamToName           = "to_name"      // rename to
	URLParamCached           = "cachedonly"   // true: return cached objects (names, metadata) instead of requesting the list from the cloud
	URLParamSuspectedTarget  = "suspect"      // suspect=string - ID of the target suspected of failure
	URLParamPrimaryCandidate = "candidate"    // candidate=string - ID of the candidate for primary proxy
	URLParamForce            = "force"        // true: shutdown the primary proxy
	URLParamPrepare          = "prepare"      // true: request is the prepare phase for primary proxy change
	URLParamDaemonID         = "daemon_id"    // daemon ID
	URLParamCheckCached      = "check_cached" // true: check if object is cached in DFC
	URLParamOffset           = "offset"       // Offset from where the object should be read
	URLParamLength           = "length"       // Length, the total number of bytes that need to be read from the offset
	URLParamWhat             = "what"         // "config" | "stats" | "xaction" ...
	URLParamProps            = "props"        // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size" | xaction type
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

// SmapVoteMsg contains the cluster map and a bool representing whether or not a vote is currently happening.
type SmapVoteMsg struct {
	VoteInProgress bool      `json:"vote_in_progress"`
	Smap           *Smap     `json:"smap"`
	BucketMD       *bucketMD `json:"bucketmd"`
}

//===================
//
// RESTful GET
//
//===================

// URLParamWhat enum
const (
	GetWhatFile     = "file" // { "what": "file" } is implied by default and can be omitted
	GetWhatConfig   = "config"
	GetWhatSmap     = "smap"
	GetWhatStats    = "stats"
	GetWhatXaction  = "xaction"
	GetWhatSmapVote = "smapvote"
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

// All bucket names known to the system
type BucketNames struct {
	Cloud []string `json:"cloud"`
	Local []string `json:"local"`
}

// RESTful URL path: /v1/....
const (
	Rversion   = "v1"
	Rbuckets   = "buckets"
	Robjects   = "objects"
	Rcluster   = "cluster"
	Rdaemon    = "daemon"
	Rsyncsmap  = "syncsmap"
	Rpush      = "push"
	Rkeepalive = "keepalive"
	Rregister  = "register"
	Rhealth    = "health"
	Rvote      = "vote"
	Rproxy     = "proxy"
	Rvoteres   = "result"
	Rvoteinit  = "init"
	Rtokens    = "tokens"
	Rmetasync  = "metasync"
)

const (
	// Used by various Xaction APIs
	XactionRebalance = ActRebalance
	XactionPrefetch  = ActPrefetch

	// Denote the status of an Xaction
	XactionStatusInProgress = "InProgress"
	XactionStatusCompleted  = "Completed"
)
