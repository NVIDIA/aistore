// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
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
	ActShutdown  = "shutdown"
	ActSyncSmap  = "syncsmap"  // synchronize cluster map aka Smap across all targets
	ActRebalance = "rebalance" // rebalance local caches upon target(s) joining and/or leaving the cluster
	ActLRU       = "lru"
	ActSyncLB    = "synclb"
	ActCreateLB  = "createlb"
	ActDestroyLB = "destroylb"
	ActRenameLB  = "renamelb"
	ActSetConfig = "setconfig"
	ActRename    = "rename"
	ActEvict     = "evict"
	ActDelete    = "delete"
	ActPrefetch  = "prefetch"
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
	HeaderDfcChecksumType = "HeaderDfcChecksumType" // Checksum Type (xxhash, md5, none)
	HeaderDfcChecksumVal  = "HeaderDfcChecksumVal"  // Checksum Value
	HeaderDfcObjVersion   = "HeaderDfcObjVersion"   // Object version/generation
	HeaderPrimaryProxyURL = "PrimaryProxyURL"       // URL of Primary Proxy
	HeaderPrimaryProxyID  = "PrimaryProxyID"        // ID of Primary Proxy
)

// URL Query Parameter enum
const (
	URLParamLocal            = "local"       // true: bucket is expected to be local
	URLParamFromID           = "from_id"     // from_id=string - ID to copy from
	URLParamToID             = "to_id"       // to_id=string - ID to copy to
	URLParamFromName         = "from_name"   // rename from
	URLParamToName           = "to_name"     // rename to
	URLParamCached           = "cachedonly"  // true: return cached objects (names, metadata) instead of requesting the list from the cloud
	URLParamNewTargetID      = "newtargetid" // ID of the new target joining the cluster
	URLParamSuspectedTarget  = "suspect"     // suspect=string - ID of the target suspected of failure
	URLParamPrimaryCandidate = "candidate"   // candidate=string - ID of the candidate for primary proxy
	URLParamForce            = "force"       // true: shutdown the primary proxy
	URLParamPrepare          = "prepare"     // true: request is the prepare phase for primary proxy change
)

// TODO: sort and some props are TBD
// GetMsg represents properties and options for get requests
type GetMsg struct {
	GetWhat       string `json:"what"`        // "config" | "stats" ...
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
	VoteInProgress bool  `json:"vote_in_progress"`
	Smap           *Smap `json:"smap"`
}

//===================
//
// RESTful GET
//
//===================

// GetMsg.GetWhat enum
const (
	GetWhatFile     = "file" // { "what": "file" } is implied by default and can be omitted
	GetWhatConfig   = "config"
	GetWhatSmap     = "smap"
	GetWhatStats    = "stats"
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
)

//===================
//
// Bucket Listing <= GET /bucket result set
//
//===================

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the GetMsg
type BucketEntry struct {
	Name     string `json:"name"`     // name of the object - note: does not include the bucket name
	Size     int64  `json:"size"`     // size in bytes
	Ctime    string `json:"ctime"`    // formatted as per GetMsg.GetTimeFormat
	Checksum string `json:"checksum"` // checksum
	Type     string `json:"type"`     // "file" OR "directory"
	Atime    string `json:"atime"`    // formatted as per GetMsg.GetTimeFormat
	Bucket   string `json:"bucket"`   // parent bucket name
	Version  string `json:"version"`  // version/generation ID. In GCP it is int64, in AWS it is a string
	IsCached bool   `json:"iscached"` // if the file is cached on one of targets
}

// BucketList represents the contents of a given bucket - somewhat analagous to the 'ls <bucket-name>'
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
	Rsyncsmap  = ActSyncSmap
	Rebalance  = ActRebalance
	Rsynclb    = ActSyncLB
	Rpush      = "push"
	Rkeepalive = "keepalive"
	Rhealth    = "health"
	Rvote      = "vote"
	Rtarget    = "target"
	Rproxy     = "proxy"
	Rvoteres   = "result"
	Rvoteinit  = "init"
	Rtokens    = "tokens"
)
