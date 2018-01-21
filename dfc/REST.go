/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

// JSON-formatted control structures
type ActionMsg struct {
	Action string `json:"action"` // shutdown, restart - see the const below
	Param1 string `json:"param1"` // action-specific params
	Param2 string `json:"param2"`
}

// ActionMsg.Action enum
const (
	ActionShutdown  = "shutdown"
	ActionSyncSmap  = "syncsmap"  // synchronize cluster map aka Smap across all targets
	ActionRebalance = "rebalance" // rebalance local caches upon target(s) joining and/or leaving the cluster
	ActionLRU       = "lru"
)

type GetMsg struct {
	GetWhat       string `json:"what"`        // "config" | "stats"
	GetSort       string `json:"sort"`        // "ascending, atime" | "descending, name"
	GetType       string `json:"type"`        // "file" | "directory"
	GetProps      string `json:"props"`       // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size"
	GetTimeFormat string `json:"time_format"` // "RFC822" default - see the enum below
}

//===================
//
// RESTful GET
//
//===================

// GetMsg.GetWhat enum
const (
	GetWhatFile   = "file" // { "what": "file" } is implied by default and can be omitted
	GetWhatConfig = "config"
	GetWhatStats  = "stats"
)

// GetMsg.GetSort enum
const (
	GetSortAsc = "ascending"
	GetSortDes = "descending"
)

// GetMsg.GetTimeFormat enum
const (
	RFC822     = "RFC822"     // default
	Stamp      = "Stamp"      // e.g. "Jan _2 15:04:05"
	StampMilli = "StampMilli" // e.g. "Jan 12 15:04:05.000"
	RFC822Z    = "RFC822Z"
	RFC1123    = "RFC1123"
	RFC1123Z   = "RFC1123Z"
	RFC3339    = "RFC3339"
)

// GetMsg.GetProps enum
const (
	GetPropsChecksum = "checksum"
	GetPropsSize     = "size"
	GetPropsAtime    = "atime"
	GetPropsCtime    = "ctime"
	GetPropsIsCached = "iscached"
	GetPropsBucket   = "bucket"
)

//===================
//
// Bucket Listing <= GET /bucket result set
//
//===================

// file and directory metadata in response to the GetMsg
type BucketEntry struct {
	Name     string `json:"name"`     // name of the object - note: does not include the bucket name
	Type     string `json:"type"`     // "file" | "directory"
	Checksum string `json:"checksum"` // checksum
	Size     int64  `json:"size"`     // size in bytes
	Ctime    string `json:"ctime"`    // formatted as per GetMsg.GetTimeFormat
	Atime    string `json:"atime"`    // ditto
}

type BucketList struct {
	Entries []*BucketEntry `json:"entries"`
}

// RESTful URL path: /v1/....
const (
	Rversion  = "v1"
	Rfiles    = "files"
	Rcluster  = "cluster"
	Rdaemon   = "daemon"
	Rsyncsmap = ActionSyncSmap
	Rebalance = ActionRebalance
	Rfrom     = "from_id"
	Rto       = "to_id"
)
