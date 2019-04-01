// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
)

// string enum: http header, checksum, versioning
const (
	// http header
	XattrXXHash  = "user.obj.xxhash"
	XattrVersion = "user.obj.version"
	XattrCopies  = "user.obj.copies"
	// checksum hash function
	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"
	ChecksumCRC32C = "crc32c"
	// buckets to inherit global checksum config
	ChecksumInherit = "inherit"
	// versioning
	VersionAll   = "all"
	VersionCloud = "cloud"
	VersionLocal = "local"
	VersionNone  = "none"
)

// ActionMsg is a JSON-formatted control structures for the REST API
type ActionMsg struct {
	Action string      `json:"action"` // shutdown, restart, setconfig - the enum below
	Name   string      `json:"name"`   // action-specific params
	Value  interface{} `json:"value"`
}

// ActionMsg.Action enum (includes xactions)
const (
	ActShutdown     = "shutdown"
	ActGlobalReb    = "rebalance"      // global cluster-wide rebalance
	ActLocalReb     = "localrebalance" // local rebalance
	ActRechecksum   = "rechecksum"
	ActLRU          = "lru"
	ActSyncLB       = "synclb"
	ActCreateLB     = "createlb"
	ActDestroyLB    = "destroylb"
	ActRenameLB     = "renamelb"
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
	ActEC           = "ec" // erasure (en)code objects
	ActStartGFN     = "metasync-start-gfn"

	// Actions for manipulating mountpaths (/v1/daemon/mountpaths)
	ActMountpathEnable  = "enable"
	ActMountpathDisable = "disable"
	ActMountpathAdd     = "add"
	ActMountpathRemove  = "remove"

	// auxiliary actions
	ActPersist = "persist" // store a piece of metadata or configuration
)

// Cloud Provider enum
const (
	ProviderAmazon = "aws"
	ProviderGoogle = "gcp"
	ProviderAIS    = "ais"
)

// Header Key enum
// Constant values conventions:
// - the constant equals the path of a value in BucketProps structure
// - if a property is a root one, then the constant is just a lowercased propery name
// - if a property is nested, then its value is propertie's parent and propery
//	 name separated with a dash
// Note: only constants from sections 'tiering' and 'bucket props' can used by
// in set single bucket property request.
const (
	HeaderCloudProvider = "cloud_provider" // from Cloud Provider enum
	HeaderVersioning    = "versioning"     // Versioning state for a bucket: "enabled"/"disabled"

	// tiering
	HeaderNextTierURL = "next_tier_url" // URL of the next tier in a AIStore multi-tier environment
	HeaderReadPolicy  = "read_policy"   // Policy used for reading in a AIStore multi-tier environment
	HeaderWritePolicy = "write_policy"  // Policy used for writing in a AIStore multi-tier environment

	// bucket props
	HeaderBucketChecksumType    = "cksum.type"              // Checksum type used for objects in the bucket
	HeaderBucketValidateColdGet = "cksum.validate_cold_get" // Cold get validation policy used for objects in the bucket
	HeaderBucketValidateWarmGet = "cksum.validate_warm_get" // Warm get validation policy used for objects in the bucket
	HeaderBucketValidateRange   = "cksum.enable_read_range" // Byte range validation policy used for objects in the bucket
	HeaderBucketLRUEnabled      = "lru.enabled"             // LRU is run on a bucket only if this field is true
	HeaderBucketLRULowWM        = "lru.lowwm"               // Capacity usage low water mark
	HeaderBucketLRUHighWM       = "lru.highwm"              // Capacity usage high water mark
	HeaderBucketAtimeCacheMax   = "lru.atime_cache_max"     // Maximum Number of Entires in the Cache
	HeaderBucketDontEvictTime   = "lru.dont_evict_time"     // Enforces an eviction-free time period between [atime, atime+dontevicttime]
	HeaderBucketCapUpdTime      = "lru.capacity_upd_time"   // Minimum time to update the capacity
	HeaderBucketMirrorEnabled   = "mirror.enabled"          // will only generate local copies when set to true
	HeaderBucketCopies          = "mirror.copies"           // # local copies
	HeaderBucketMirrorThresh    = "mirror.util_thresh"      // utilizations are considered equivalent when below this threshold
	HeaderBucketECEnabled       = "ec.enabled"              // EC is on for a bucket
	HeaderBucketECMinSize       = "ec.objsize_limit"        // Objects under MinSize copied instead of being EC'ed
	HeaderBucketECData          = "ec.data_slices"          // number of data chunks for EC
	HeaderBucketECParity        = "ec.parity_slices"        // number of parity chunks for EC/copies for small files

	// object meta
	HeaderObjCksumType = "ObjCksumType" // Checksum Type (xxhash, md5, none)
	HeaderObjCksumVal  = "ObjCksumVal"  // Checksum Value
	HeaderObjAtime     = "ObjAtime"     // Object access time
	HeaderObjReplicSrc = "ObjReplicSrc" // In replication PUT request specifies the source target
	HeaderObjSize      = "ObjSize"      // Object size (bytes)
	HeaderObjVersion   = "ObjVersion"   // Object version/generation - local or Cloud
)

// URL Query "?name1=val1&name2=..."
const (
	// user/app API
	URLParamWhat        = "what"         // "smap" | "bucketmd" | "config" | "stats" | "xaction" ...
	URLParamProps       = "props"        // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size" | xaction type
	URLParamCheckCached = "check_cached" // true: check if object is cached in AIStore
	URLParamOffset      = "offset"       // Offset from where the object should be read
	URLParamLength      = "length"       // the total number of bytes that need to be read from the offset
	URLParamBckProvider = "bprovider"    // "local" | "cloud"
	URLParamPrefix      = "prefix"       // prefix for list objects in a bucket
	URLParamRegex       = "regex"        // param for only returning dsort/downloader processes where the description contains regex
	// internal use
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
	URLParamIsGFNRequest     = "gfn" // true if the request is a Get From Neighbor request

	// dsort
	URLParamTotalCompressedSize   = "tcs"
	URLParamTotalInputShardsSeen  = "tiss"
	URLParamTotalUncompressedSize = "tunc"

	// downloader
	URLParamBase        = "base"
	URLParamBucket      = "bucket"
	URLParamID          = "id"
	URLParamLink        = "link"
	URLParamObjName     = "objname"
	URLParamSuffix      = "suffix"
	URLParamTemplate    = "template"
	URLParamTimeout     = "timeout"
	URLParamDescription = "description"
)

const (
	RebStart = "start"
	RebAbort = "abort"
)

// SelectMsg represents properties and options for requests which fetch entities
// Note: if Fast is `true` then paging is disabled - all items are returned
//       in one response. The result list is unsorted and contains only object
//       names: even field `Status` is filled with zero value
type SelectMsg struct {
	Props      string `json:"props"`       // e.g. "checksum, size" | "atime, size" | "ctime, iscached" | "bucket, size"
	TimeFormat string `json:"time_format"` // "RFC822" default - see the enum above
	Prefix     string `json:"prefix"`      // object name filter: return only objects which name starts with prefix
	PageMarker string `json:"pagemarker"`  // marker - the last object in previous page
	PageSize   int    `json:"pagesize"`    // maximum number of entries returned by list bucket call
	Fast       bool   `json:"fast"`        // performs a fast traversal of the bucket contents (returns only names)
}

// ListRangeMsgBase contains fields common to Range and List operations
type ListRangeMsgBase struct {
	Deadline time.Duration `json:"deadline,omitempty"`
	Wait     bool          `json:"wait,omitempty"`
}

// ListMsg contains a list of files and a duration within which to get them
type ListMsg struct {
	ListRangeMsgBase
	Objnames []string `json:"objnames"`
}

// RangeMsg contains a Prefix, Regex, and Range for a Range Operation
type RangeMsg struct {
	ListRangeMsgBase
	Prefix string `json:"prefix"`
	Regex  string `json:"regex"`
	Range  string `json:"range"`
}

// MountpathList contains two lists:
// * Available - list of local mountpaths available to the storage target
// * Disabled  - list of disabled mountpaths, the mountpaths that generated
//	         IO errors followed by (FSHC) health check, etc.
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
	GetWhatConfig       = "config"
	GetWhatSmap         = "smap"
	GetWhatBucketMeta   = "bucketmd"
	GetWhatStats        = "stats"
	GetWhatXaction      = "xaction"
	GetWhatSmapVote     = "smapvote"
	GetWhatMountpaths   = "mountpaths"
	GetWhatSnode        = "snode"
	GetWhatSysInfo      = "sysinfo"
	GetWhatDaemonStatus = "status"
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
	GetPropsCtime    = "ctime"
	GetPropsIsCached = "iscached"
	GetPropsBucket   = "bucket"
	GetPropsVersion  = "version"
	GetTargetURL     = "targetURL"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
)

// BucketEntry.Status
const (
	ObjStatusOK      = ""
	ObjStatusMoved   = "moved"
	ObjStatusDeleted = "deleted"
)

//===================
//
// Bucket Listing <= GET /bucket result set
//
//===================

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the SelectMsg
type BucketEntry struct {
	Name      string `json:"name"`                // name of the object - note: does not include the bucket name
	Size      int64  `json:"size,omitempty"`      // size in bytes
	Ctime     string `json:"ctime,omitempty"`     // formatted as per SelectMsg.TimeFormat
	Checksum  string `json:"checksum,omitempty"`  // checksum
	Type      string `json:"type,omitempty"`      // "file" OR "directory"
	Atime     string `json:"atime,omitempty"`     // formatted as per SelectMsg.TimeFormat
	Bucket    string `json:"bucket,omitempty"`    // parent bucket name
	Version   string `json:"version,omitempty"`   // version/generation ID. In GCP it is int64, in AWS it is a string
	TargetURL string `json:"targetURL,omitempty"` // URL of target which has the entry
	Status    string `json:"status,omitempty"`    // empty - normal object, it can be "moved", "deleted" etc
	Copies    int16  `json:"copies,omitempty"`    // ## copies (non-replicated = 1)
	IsCached  bool   `json:"iscached,omitempty"`  // if the file is cached on one of targets
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
	// l3
	SyncSmap   = "syncsmap"
	Keepalive  = "keepalive"
	Register   = "register"
	Unregister = "unregister"
	Proxy      = "proxy"
	Voteres    = "result"
	VoteInit   = "init"
	Mountpaths = "mountpaths"
	ListAll    = "*"

	// dSort
	Init        = "init"
	Sort        = "sort"
	Start       = "start"
	Abort       = "abort"
	Metrics     = "metrics"
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished-ack"
	List        = "list"   // lists running dsort processes
	Remove      = "remove" // removes a dsort process from history

	// downloader
	Cancel = "cancel"

	// CLI
	Target = "target"
)

// Download POST result returned to the user
type DlPostResp struct {
	ID string `json:"id"`
}

type DlStatusResp struct {
	Finished   int     `json:"finished"`
	Total      int     `json:"total"`
	Percentage float64 `json:"percentage"`

	Cancelled  bool `json:"cancelled"`
	NumPending int  `json:"num_pending"`

	CurrentTasks  []TaskDlInfo  `json:"current_tasks,omitempty"`
	FinishedTasks []TaskDlInfo  `json:"finished_tasks,omitempty"`
	Errs          []TaskErrInfo `json:"download_errors,omitempty"`
}

func (d *DlStatusResp) Print(verbose bool) string {
	if d.Cancelled {
		return "Download was cancelled."
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Download progress: %d/%d (%.0f%%)", d.Finished, d.Total, d.Percentage))
	if !verbose {
		return sb.String()
	}

	if len(d.CurrentTasks) != 0 {
		sb.WriteString("\n")
		sb.WriteString("Progress of files that are currently being downloaded:\n")
		for _, task := range d.CurrentTasks {
			sb.WriteString(fmt.Sprintf("\t%s: %s\n", task.Name, B2S(task.Downloaded, 2))) // TODO: print total too
		}
	}

	return sb.String()
}

// Summary info of the download job
// FIXME: add more stats
type DlJobInfo struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	NumPending  int    `json:"num_pending"`
	Cancelled   bool   `json:"cancelled"`
}

func (j *DlJobInfo) Aggregate(rhs DlJobInfo) {
	j.NumPending += rhs.NumPending
}

func (j *DlJobInfo) String() string {
	var sb strings.Builder

	sb.WriteString(j.ID)

	if j.Description != "" {
		sb.WriteString(fmt.Sprintf(" (%s)", j.Description))
	}

	sb.WriteString(": ")

	if j.NumPending == 0 {
		sb.WriteString("finished")
	} else {
		sb.WriteString(fmt.Sprintf("%d files still being downloaded", j.NumPending))
	}

	return sb.String()
}

type DlBase struct {
	Description string `json:"description"`
	Bucket      string `json:"bucket"`
	BckProvider string `json:"bprovider"`
	Timeout     string `json:"timeout"`
}

func (b *DlBase) InitWithQuery(query url.Values) {
	if b.Bucket == "" {
		b.Bucket = query.Get(URLParamBucket)
	}
	b.BckProvider = query.Get(URLParamBckProvider)
	b.Timeout = query.Get(URLParamTimeout)
	b.Description = query.Get(URLParamDescription)
}

func (b *DlBase) AsQuery() url.Values {
	query := url.Values{}
	if b.Bucket != "" {
		query.Add(URLParamBucket, b.Bucket)
	}
	if b.BckProvider != "" {
		query.Add(URLParamBckProvider, b.BckProvider)
	}
	if b.Timeout != "" {
		query.Add(URLParamTimeout, b.Timeout)
	}
	if b.Description != "" {
		query.Add(URLParamDescription, b.Description)
	}
	return query
}

func (b *DlBase) Validate() error {
	if b.Bucket == "" {
		return fmt.Errorf("missing the %q which is required", URLParamBucket)
	}
	if b.Timeout != "" {
		if _, err := time.ParseDuration(b.Timeout); err != nil {
			return fmt.Errorf("failed to parse timeout field: %v", err)
		}
	}
	return nil
}

type DlObj struct {
	Objname   string `json:"objname"`
	Link      string `json:"link"`
	FromCloud bool   `json:"from_cloud"`
}

func (b *DlObj) Validate() error {
	if b.Objname == "" {
		objName := path.Base(b.Link)
		if objName == "." || objName == "/" {
			return fmt.Errorf("can not extract a valid %q from the provided download link", URLParamObjName)
		}
		b.Objname = objName
	}
	if b.Link == "" && !b.FromCloud {
		return fmt.Errorf("missing the %q from the request body", URLParamLink)
	}
	if b.Objname == "" {
		return fmt.Errorf("missing the %q from the request body", URLParamObjName)
	}
	return nil
}

// Internal status/delete request body
type DlAdminBody struct {
	ID    string `json:"id"`
	Regex string `json:"regex"`
}

func (b *DlAdminBody) InitWithQuery(query url.Values) {
	b.ID = query.Get(URLParamID)
	b.Regex = query.Get(URLParamRegex)
}

func (b *DlAdminBody) AsQuery() url.Values {
	query := url.Values{}
	if b.ID != "" {
		query.Add(URLParamID, b.ID)
	}
	if b.Regex != "" {
		query.Add(URLParamRegex, b.Regex)
	}
	return query
}

func (b *DlAdminBody) Validate(requireID bool) error {
	if b.ID != "" && b.Regex != "" {
		return fmt.Errorf("regex %q defined at the same time as id %q", URLParamRegex, URLParamID)
	} else if b.Regex != "" {
		if _, err := regexp.CompilePOSIX(b.Regex); err != nil {
			return err
		}
	} else if b.ID == "" && requireID {
		return fmt.Errorf("ID not specified")
	}
	return nil
}

// Internal download request body
type DlBody struct {
	DlBase
	ID   string  `json:"id"`
	Objs []DlObj `json:"objs"`

	Cancelled bool `json:"cancelled"`
}

func (b *DlBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if b.ID == "" {
		return fmt.Errorf("missing %q, something went wrong", URLParamID)
	}
	if b.Cancelled {
		// used to store cancel status in db, should be unset
		return fmt.Errorf("invalid flag 'cancelled'")
	}
	for _, obj := range b.Objs {
		if err := obj.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (b *DlBody) String() string {
	return fmt.Sprintf("%s, id=%q", b.DlBase, b.ID)
}

type TaskInfoByName []TaskDlInfo

func (t TaskInfoByName) Len() int           { return len(t) }
func (t TaskInfoByName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TaskInfoByName) Less(i, j int) bool { return t[i].Name < t[j].Name }

// Info about a task that is currently or has been downloaded by one of the joggers
type TaskDlInfo struct {
	Name       string `json:"name"`
	Downloaded int64  `json:"downloaded"`
	Total      int64  `json:"total,omitempty"`

	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`

	Running bool `json:"running"`
}

type TaskErrByName []TaskErrInfo

func (t TaskErrByName) Len() int           { return len(t) }
func (t TaskErrByName) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t TaskErrByName) Less(i, j int) bool { return t[i].Name < t[j].Name }

type TaskErrInfo struct {
	Name string `json:"name"`
	Err  string `json:"error"`
}

// Single request
type DlSingleBody struct {
	DlBase
	DlObj
}

func (b *DlSingleBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Link = query.Get(URLParamLink)
	b.Objname = query.Get(URLParamObjName)
}

func (b *DlSingleBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamLink, b.Link)
	query.Add(URLParamObjName, b.Objname)
	return query
}

func (b *DlSingleBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if err := b.DlObj.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlSingleBody) ExtractPayload() (SimpleKVs, error) {
	objects := make(SimpleKVs, 1)
	objects[b.Objname] = b.Link
	return objects, nil
}

func (b *DlSingleBody) Describe() string {
	return fmt.Sprintf("%s -> %s/%s", b.Link, b.Bucket, b.Objname)
}

func (b *DlSingleBody) String() string {
	return fmt.Sprintf("Link: %q, Bucket: %q, Objname: %q.", b.Link, b.Bucket, b.Objname)
}

// Range request
type DlRangeBody struct {
	DlBase
	Base     string `json:"base"`
	Template string `json:"template"`
}

func (b *DlRangeBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Base = query.Get(URLParamBase)
	b.Template = query.Get(URLParamTemplate)
}

func (b *DlRangeBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamBase, b.Base)
	query.Add(URLParamTemplate, b.Template)
	return query
}

func (b *DlRangeBody) Validate() error {
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	if b.Base == "" {
		return fmt.Errorf("no %q for range found, %q is required", URLParamBase, URLParamBase)
	}
	if !strings.HasSuffix(b.Base, "/") {
		b.Base += "/"
	}
	if b.Template == "" {
		return fmt.Errorf("no %q for range found, %q is required", URLParamTemplate, URLParamTemplate)
	}
	return nil
}

func (b *DlRangeBody) ExtractPayload() (SimpleKVs, error) {
	prefix, suffix, start, end, step, digitCount, err := ParseBashTemplate(b.Template)
	if err != nil {
		return nil, err
	}

	objects := make(SimpleKVs, (end-start+1)/step)
	for i := start; i <= end; i += step {
		objname := fmt.Sprintf("%s%0*d%s", prefix, digitCount, i, suffix)
		objects[objname] = b.Base + objname
	}
	return objects, nil
}

func (b *DlRangeBody) Describe() string {
	return fmt.Sprintf("%s%s -> %s", b.Base, b.Template, b.Bucket)
}

func (b *DlRangeBody) String() string {
	return fmt.Sprintf("bucket: %q, base: %q, template: %q", b.Bucket, b.Base, b.Template)
}

// Multi request
type DlMultiBody struct {
	DlBase
}

func (b *DlMultiBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
}

func (b *DlMultiBody) Validate(body []byte) error {
	if len(body) <= 0 {
		return errors.New("body should not be empty")
	}

	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlMultiBody) ExtractPayload(objectsPayload interface{}) (SimpleKVs, error) {
	objects := make(SimpleKVs, 10)
	switch ty := objectsPayload.(type) {
	case map[string]interface{}:
		for key, val := range ty {
			switch v := val.(type) {
			case string:
				objects[key] = v
			default:
				return nil, fmt.Errorf("values in map should be strings, found: %T", v)
			}
		}
	case []interface{}:
		// process list of links
		for _, val := range ty {
			switch link := val.(type) {
			case string:
				objName := path.Base(link)
				if objName == "." || objName == "/" {
					// should we continue and let the use worry about this after?
					return nil, fmt.Errorf("can not extract a valid `object name` from the provided download link: %q", link)
				}
				objects[objName] = link
			default:
				return nil, fmt.Errorf("values in array should be strings, found: %T", link)
			}
		}
	default:
		return nil, fmt.Errorf("JSON body should be map (string -> string) or array of strings, found: %T", ty)
	}
	return objects, nil
}

func (b *DlMultiBody) Describe() string {
	return fmt.Sprintf("multi-download -> %s", b.Bucket)
}

func (b *DlMultiBody) String() string {
	return fmt.Sprintf("bucket: %q", b.Bucket)
}

// Cloud request
type DlCloudBody struct {
	DlBase
	Prefix string `json:"prefix"`
	Suffix string `json:"suffix"`
}

func (b *DlCloudBody) InitWithQuery(query url.Values) {
	b.DlBase.InitWithQuery(query)
	b.Prefix = query.Get(URLParamPrefix)
	b.Suffix = query.Get(URLParamSuffix)
}

func (b *DlCloudBody) Validate(bckIsLocal bool) error {
	if bckIsLocal {
		return errors.New("bucket download requires cloud bucket")
	}
	if err := b.DlBase.Validate(); err != nil {
		return err
	}
	return nil
}

func (b *DlCloudBody) AsQuery() url.Values {
	query := b.DlBase.AsQuery()
	query.Add(URLParamPrefix, b.Prefix)
	query.Add(URLParamSuffix, b.Suffix)
	return query
}

func (b *DlCloudBody) Describe() string {
	return fmt.Sprintf("cloud prefetch -> %s", b.Bucket)
}

const (
	// Used by various Xaction APIs
	XactionRebalance = ActGlobalReb
	XactionPrefetch  = ActPrefetch
	XactionDownload  = ActDownload

	// Denote the status of an Xaction
	XactionStatusInProgress = "InProgress"
	XactionStatusCompleted  = "Completed"
)
const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

// BucketProps defines the configuration of the bucket with regard to
// its type, checksum, and LRU. These characteristics determine its behaviour
// in response to operations on the bucket itself or the objects inside the bucket.
type BucketProps struct {
	// CloudProvider can be "aws", "gcp" (clouds) - or "ais".
	// If a bucket is local, CloudProvider must be "ais".
	// Otherwise, it must be "aws" or "gcp".
	CloudProvider string `json:"cloud_provider,omitempty"`

	// Versioning defines what kind of buckets should use versioning to
	// detect if the object must be redownloaded.
	// Values: "all", "cloud", "local" or "none".
	Versioning string

	// NextTierURL is an absolute URI corresponding to the primary proxy
	// of the next tier configured for the bucket specified
	NextTierURL string `json:"next_tier_url,omitempty"`

	// ReadPolicy determines if a read will be from cloud or next tier
	// specified by NextTierURL. Default: "next_tier"
	ReadPolicy string `json:"read_policy,omitempty"`

	// WritePolicy determines if a write will be to cloud or next tier
	// specified by NextTierURL. Default: "cloud"
	WritePolicy string `json:"write_policy,omitempty"`

	// Cksum is the embedded struct of the same name
	Cksum CksumConf `json:"cksum"`

	// LRU is the embedded struct of the same name
	LRU LRUConf `json:"lru"`

	// Mirror defines local-mirroring policy for the bucket
	Mirror MirrorConf `json:"mirror"`

	// EC defines erasure coding setting for the bucket
	EC ECConf `json:"ec"`
}

// ECConfig - per-bucket erasure coding configuration
type ECConf struct {
	ObjSizeLimit int64 `json:"objsize_limit"` // objects below this size are replicated instead of EC'ed
	DataSlices   int   `json:"data_slices"`   // number of data slices
	ParitySlices int   `json:"parity_slices"` // number of parity slices/replicas
	Enabled      bool  `json:"enabled"`       // EC is enabled
}

func (c *ECConf) Updatable(field string) bool {
	return (c.Enabled && !(field == HeaderBucketECData ||
		field == HeaderBucketECParity ||
		field == HeaderBucketECMinSize)) || !c.Enabled
}

// ObjectProps
type ObjectProps struct {
	Size    int
	Version string
}

func DefaultBucketProps() *BucketProps {
	c := GCO.Clone()

	c.Cksum.Type = ChecksumInherit
	return &BucketProps{
		Cksum:  c.Cksum,
		LRU:    c.LRU,
		Mirror: c.Mirror,
	}
}

func (to *BucketProps) CopyFrom(from *BucketProps) {
	to.NextTierURL = from.NextTierURL
	to.CloudProvider = from.CloudProvider
	if from.ReadPolicy != "" {
		to.ReadPolicy = from.ReadPolicy
	}
	if from.WritePolicy != "" {
		to.WritePolicy = from.WritePolicy
	}
	if from.Cksum.Type != "" {
		to.Cksum.Type = from.Cksum.Type
		if from.Cksum.Type != ChecksumInherit {
			to.Cksum = from.Cksum
		}
	}

	to.LRU = from.LRU
	to.Mirror = from.Mirror
	to.EC = from.EC
}

func (bp *BucketProps) Validate(bckIsLocal bool, targetCnt int, urlOutsideCluster func(string) bool) error {
	if bp.NextTierURL != "" {
		if _, err := url.ParseRequestURI(bp.NextTierURL); err != nil {
			return fmt.Errorf("invalid next tier URL: %s, err: %v", bp.NextTierURL, err)
		}
		if !urlOutsideCluster(bp.NextTierURL) {
			return fmt.Errorf("invalid next tier URL: %s, URL is in current cluster", bp.NextTierURL)
		}
	}
	if err := validateCloudProvider(bp.CloudProvider, bckIsLocal); err != nil {
		return err
	}
	if bp.ReadPolicy != "" && bp.ReadPolicy != RWPolicyCloud && bp.ReadPolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid read policy: %s", bp.ReadPolicy)
	}
	if bp.ReadPolicy == RWPolicyCloud && bckIsLocal {
		return fmt.Errorf("read policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if bp.WritePolicy != "" && bp.WritePolicy != RWPolicyCloud && bp.WritePolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid write policy: %s", bp.WritePolicy)
	}
	if bp.WritePolicy == RWPolicyCloud && bckIsLocal {
		return fmt.Errorf("write policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if bp.NextTierURL != "" {
		if bp.CloudProvider == "" {
			return fmt.Errorf("tiered bucket must use one of the supported cloud providers (%s | %s | %s)",
				ProviderAmazon, ProviderGoogle, ProviderAIS)
		}
		if bp.ReadPolicy == "" {
			bp.ReadPolicy = RWPolicyNextTier
		}
		if bp.WritePolicy == "" && !bckIsLocal {
			bp.WritePolicy = RWPolicyCloud
		} else if bp.WritePolicy == "" && bckIsLocal {
			bp.WritePolicy = RWPolicyNextTier
		}
	}

	validationArgs := &ValidationArgs{BckIsLocal: bckIsLocal, TargetCnt: targetCnt}
	validators := []PropsValidator{&bp.Cksum, &bp.LRU, &bp.Mirror, &bp.EC}
	for _, validator := range validators {
		if err := validator.ValidateAsProps(validationArgs); err != nil {
			return err
		}
	}
	return nil
}

func validateCloudProvider(provider string, bckIsLocal bool) error {
	if provider != "" && provider != ProviderAmazon && provider != ProviderGoogle && provider != ProviderAIS {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			ProviderAmazon, ProviderGoogle, ProviderAIS)
	} else if bckIsLocal && provider != ProviderAIS && provider != "" {
		return fmt.Errorf("local bucket can only have '%s' as the cloud provider", ProviderAIS)
	}
	return nil
}
