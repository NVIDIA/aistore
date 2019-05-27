// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// string enum: http header, checksum, versioning
const (
	// FS extended attributes
	XattrLOM = "user.ais.lom"
	XattrBMD = "user.ais.bmd"

	// checksum hash function
	ChecksumNone   = "none"
	ChecksumXXHash = "xxhash"
	ChecksumMD5    = "md5"
	ChecksumCRC32C = "crc32c"

	// EC default object size limit: smaller objects are replicated instead of EC'ing
	ECDefaultSizeLimit = 256 * KiB
)

// Module names

const (
	DSortName          = "dSort"
	DSortNameLowercase = "dsort"
)

// Bucket property type (used by cksum, versioning)
const (
	PropInherit = "inherit" // inherit bucket property from global config
	PropOwn     = "own"     // bucket has its own configuration
)

// ActionMsg is a JSON-formatted control structures for the REST API
type ActionMsg struct {
	Action string      `json:"action"` // shutdown, restart, setconfig - the enum below
	Name   string      `json:"name"`   // action-specific params
	Value  interface{} `json:"value"`
}

type XactKindMeta struct {
	IsGlobal bool
}

type XactKindType map[string]XactKindMeta

var XactKind = XactKindType{
	// global kinds
	ActLRU:          {true},
	ActElection:     {true},
	ActLocalReb:     {true},
	ActGlobalReb:    {true},
	ActPrefetch:     {true},
	ActDownload:     {true},
	ActEvictObjects: {true},
	ActDelete:       {true},

	// bucket's kinds
	ActECGet:       {},
	ActECPut:       {},
	ActECRespond:   {},
	ActMakeNCopies: {},
	ActPutCopies:   {},
}

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
	HeaderBucketMirrorThresh    = "mirror.util_thresh"      // utilizations are considered equivalent when below this threshold
	HeaderBucketECEnabled       = "ec.enabled"              // EC is on for a bucket
	HeaderBucketECMinSize       = "ec.objsize_limit"        // Objects under MinSize copied instead of being EC'ed
	HeaderBucketECData          = "ec.data_slices"          // number of data chunks for EC
	HeaderBucketECParity        = "ec.parity_slices"        // number of parity chunks for EC/copies for small files
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

	// intra-cluster comm
	HeaderCallerID   = "caller.id"
	HeaderCallerName = "caller.name"
)

// BucketPropList is a map bucket property <-> readonly, groupped by type
var BucketPropList = map[string]bool{
	HeaderCloudProvider: true,
	HeaderNextTierURL:   false, HeaderReadPolicy: false, HeaderWritePolicy: false,
	HeaderBucketChecksumType: true, HeaderBucketValidateColdGet: false, HeaderBucketValidateWarmGet: false,
	HeaderBucketEnableReadRange: false, HeaderBucketValidateObjMove: false,
	HeaderBucketVerEnabled: false, HeaderBucketVerValidateWarm: false,
	HeaderBucketLRUEnabled: false, HeaderBucketLRULowWM: false, HeaderBucketLRUHighWM: false, HeaderBucketDontEvictTime: true, HeaderBucketCapUpdTime: true,
	HeaderBucketMirrorEnabled: false, HeaderBucketMirrorThresh: false, HeaderBucketCopies: false,
	HeaderBucketECEnabled: false, HeaderBucketECData: false, HeaderBucketECParity: false, HeaderBucketECMinSize: false,
	HeaderBucketAccessAttrs: false,
}

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
	URLParamSilent           = "sln" // true - destination should not log errors(HEAD request)

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

type XactionExtMsg struct {
	Target string `json:"target,omitempty"`
	Bucket string `json:"bucket,omitempty"`
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

	AllowAnyAccess = 0
	AllowAllAccess = ^uint64(0)

	AllowAccess = "allow"
	DenyAccess  = "deny"
)

func MakeAccess(aattr uint64, action string, bits uint64) uint64 {
	if aattr == AllowAnyAccess {
		aattr = AllowAllAccess
	}
	if action == AllowAccess {
		return aattr | bits
	}
	Assert(action == DenyAccess)
	return aattr & (AllowAllAccess ^ bits)
}

// BucketProps defines the configuration of the bucket with regard to
// its type, checksum, and LRU. These characteristics determine its behavior
// in response to operations on the bucket itself or the objects inside the bucket.
type BucketProps struct {
	// CloudProvider can be "aws", "gcp" (clouds) - or "ais".
	// If a bucket is local, CloudProvider must be "ais".
	// Otherwise, it must be "aws" or "gcp".
	CloudProvider string `json:"cloud_provider,omitempty"`

	// Versioning defines what kind of buckets should use versioning to
	// detect if the object must be redownloaded.
	// Values: "all", "cloud", "local" or "none".
	Versioning VersionConf `json:"versioning,omitempty"`

	// tier location and tier/cloud policies
	Tiering TierConf `json:"tier,omitempty"`

	// Cksum is the embedded struct of the same name
	Cksum CksumConf `json:"cksum"`

	// LRU is the embedded struct of the same name
	LRU LRUConf `json:"lru"`

	// Mirror defines local-mirroring policy for the bucket
	Mirror MirrorConf `json:"mirror"`

	// EC defines erasure coding setting for the bucket
	EC ECConf `json:"ec"`

	// Bucket access attributes - see Allow* above
	AccessAttrs uint64 `json:"aattrs"`

	// unique bucket ID
	BID uint64
}

type TierConf struct {
	// NextTierURL is an absolute URI corresponding to the primary proxy
	// of the next tier configured for the bucket specified
	NextTierURL string `json:"next_url,omitempty"`

	// ReadPolicy determines if a read will be from cloud or next tier
	// specified by NextTierURL. Default: "next_tier"
	ReadPolicy string `json:"read_policy,omitempty"`

	// WritePolicy determines if a write will be to cloud or next tier
	// specified by NextTierURL. Default: "cloud"
	WritePolicy string `json:"write_policy,omitempty"`
}

// ECConfig - per-bucket erasure coding configuration
type ECConf struct {
	ObjSizeLimit int64 `json:"objsize_limit"` // objects below this size are replicated instead of EC'ed
	DataSlices   int   `json:"data_slices"`   // number of data slices
	ParitySlices int   `json:"parity_slices"` // number of parity slices/replicas
	Enabled      bool  `json:"enabled"`       // EC is enabled
}

func (c *VersionConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}

	text := "(validation: WarmGET="
	if c.ValidateWarmGet {
		text += "yes)"
	} else {
		text += "no)"
	}

	return text
}

func (c *CksumConf) String() string {
	if c.Type == ChecksumNone {
		return "Disabled"
	}

	yesProps := make([]string, 0)
	noProps := make([]string, 0)
	add := func(val bool, name string) {
		if val {
			yesProps = append(yesProps, name)
		} else {
			noProps = append(noProps, name)
		}
	}

	add(c.ValidateColdGet, "ColdGET")
	add(c.ValidateWarmGet, "WarmGET")
	add(c.ValidateObjMove, "ObjectMove")
	add(c.EnableReadRange, "ReadRange")

	props := make([]string, 0, 2)
	if len(yesProps) != 0 {
		props = append(props, strings.Join(yesProps, ",")+"=yes")
	}
	if len(noProps) != 0 {
		props = append(props, strings.Join(noProps, ",")+"=no")
	}

	return fmt.Sprintf("%s (validation: %s)", c.Type, strings.Join(props, ", "))
}

func (c *LRUConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}
	return fmt.Sprintf("Watermarks: %d/%d, do not evict time: %s",
		c.LowWM, c.HighWM, c.DontEvictTimeStr)
}

func (c *BucketProps) AccessToStr() string {
	aattrs := c.AccessAttrs
	if aattrs == 0 {
		return "No access"
	}
	accList := make([]string, 0, 8)
	if aattrs&AccessGET == AccessGET {
		accList = append(accList, "GET")
	}
	if aattrs&AccessPUT == AccessPUT {
		accList = append(accList, "PUT")
	}
	if aattrs&AccessDELETE == AccessDELETE {
		accList = append(accList, "DELETE")
	}
	if aattrs&AccessHEAD == AccessHEAD {
		accList = append(accList, "HEAD")
	}
	if aattrs&AccessColdGET == AccessColdGET {
		accList = append(accList, "ColdGET")
	}
	return strings.Join(accList, ",")
}

func (c *MirrorConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}

	return fmt.Sprintf("%d copies", c.Copies)
}

func (c *RebalanceConf) String() string {
	if c.Enabled {
		return "Enabled"
	}
	return "Disabled"
}

func (c *TierConf) String() string {
	if c.NextTierURL == "" {
		return "Disabled"
	}

	return c.NextTierURL
}

func (c *ECConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}
	objSizeLimit := c.ObjSizeLimit
	if objSizeLimit == 0 {
		objSizeLimit = ECDefaultSizeLimit
	}
	return fmt.Sprintf("%d:%d (%s)", c.DataSlices, c.ParitySlices, B2S(objSizeLimit, 0))
}

func (c *ECConf) Updatable(field string) bool {
	return (c.Enabled && !(field == HeaderBucketECData ||
		field == HeaderBucketECParity ||
		field == HeaderBucketECMinSize)) || !c.Enabled
}

func (c *ECConf) RequiredEncodeTargets() int {
	// data slices + parity slices + 1 target for original object
	return c.DataSlices + c.ParitySlices + 1
}

func (c *ECConf) RequiredRestoreTargets() int {
	// data slices + 1 target for original object
	return c.DataSlices + 1
}

// ObjectProps
type ObjectProps struct {
	Size        int64
	Version     string
	Atime       time.Time
	NumCopies   int
	Checksum    string
	Present     bool
	BucketLocal bool
}

func DefaultBucketProps(local bool) *BucketProps {
	c := GCO.Clone()
	c.Cksum.Type = PropInherit
	if local {
		if !c.LRU.LocalBuckets {
			c.LRU.Enabled = false
		}
	}
	return &BucketProps{
		Cksum:       c.Cksum,
		LRU:         c.LRU,
		Mirror:      c.Mirror,
		Versioning:  c.Ver,
		AccessAttrs: AllowAllAccess,
	}
}

// FIXME: *to = *from
func (to *BucketProps) CopyFrom(from *BucketProps) {
	to.Tiering.NextTierURL = from.Tiering.NextTierURL
	to.CloudProvider = from.CloudProvider
	if from.Tiering.ReadPolicy != "" {
		to.Tiering.ReadPolicy = from.Tiering.ReadPolicy
	}
	if from.Tiering.WritePolicy != "" {
		to.Tiering.WritePolicy = from.Tiering.WritePolicy
	}
	if from.Cksum.Type != "" {
		to.Cksum.Type = from.Cksum.Type
		if from.Cksum.Type != PropInherit {
			to.Cksum = from.Cksum
		}
	}
	if from.Versioning.Type != "" {
		if from.Versioning.Type != PropInherit {
			to.Versioning = from.Versioning
		} else {
			to.Versioning.Type = from.Versioning.Type
		}
	}
	to.LRU = from.LRU
	to.Mirror = from.Mirror
	to.EC = from.EC
	to.AccessAttrs = from.AccessAttrs
}

func (bp *BucketProps) Validate(bckIsLocal bool, targetCnt int, urlOutsideCluster func(string) bool) error {
	if bp.Tiering.NextTierURL != "" {
		if _, err := url.ParseRequestURI(bp.Tiering.NextTierURL); err != nil {
			return fmt.Errorf("invalid next tier URL: %s, err: %v", bp.Tiering.NextTierURL, err)
		}
		if !urlOutsideCluster(bp.Tiering.NextTierURL) {
			return fmt.Errorf("invalid next tier URL: %s, URL is in current cluster", bp.Tiering.NextTierURL)
		}
	}
	if err := validateCloudProvider(bp.CloudProvider, bckIsLocal); err != nil {
		return err
	}
	if bp.Tiering.ReadPolicy != "" && bp.Tiering.ReadPolicy != RWPolicyCloud && bp.Tiering.ReadPolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid read policy: %s", bp.Tiering.ReadPolicy)
	}
	if bp.Tiering.ReadPolicy == RWPolicyCloud && bckIsLocal {
		return fmt.Errorf("read policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if bp.Tiering.WritePolicy != "" && bp.Tiering.WritePolicy != RWPolicyCloud && bp.Tiering.WritePolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid write policy: %s", bp.Tiering.WritePolicy)
	}
	if bp.Tiering.WritePolicy == RWPolicyCloud && bckIsLocal {
		return fmt.Errorf("write policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if bp.Tiering.NextTierURL != "" {
		if bp.CloudProvider == "" {
			return fmt.Errorf("tiered bucket must use one of the supported cloud providers (%s | %s | %s)",
				ProviderAmazon, ProviderGoogle, ProviderAIS)
		}
		if bp.Tiering.ReadPolicy == "" {
			bp.Tiering.ReadPolicy = RWPolicyNextTier
		}
		if bp.Tiering.WritePolicy == "" {
			bp.Tiering.WritePolicy = RWPolicyNextTier
			if !bckIsLocal {
				bp.Tiering.WritePolicy = RWPolicyCloud
			}
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

func ReadXactionRequestMessage(actionMsg *ActionMsg) (*XactionExtMsg, error) {
	xactMsg := &XactionExtMsg{}
	xactMsgJSON, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal action message: %v. error: %v", actionMsg, err)
	}
	if err := jsoniter.Unmarshal(xactMsgJSON, xactMsg); err != nil {
		return nil, err
	}

	return xactMsg, nil
}

func (k XactKindType) IsGlobalKind(kind string) (bool, error) {
	kindMeta, ok := k[kind]

	if !ok {
		return false, fmt.Errorf("xaction kind %s not recognized", kind)
	}

	return kindMeta.IsGlobal, nil
}

// Common errors
var (
	ErrorBucketAlreadyExists     = errors.New("bucket already exists")
	ErrorCloudBucketDoesNotExist = errors.New("cloud bucket does not exist")
)

type errAccessDenied struct {
	entity      string
	operation   string
	accessAttrs uint64
}

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}

type BucketAccessDenied struct{ errAccessDenied }
type ObjectAccessDenied struct{ errAccessDenied }

func (e *BucketAccessDenied) Error() string { return "bucket " + e.String() }
func (e *ObjectAccessDenied) Error() string { return "object " + e.String() }

func NewBucketAccessDenied(bucket, oper string, aattrs uint64) *BucketAccessDenied {
	return &BucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}
func NewObjectAccessDenied(name, oper string, aattrs uint64) *ObjectAccessDenied {
	return &ObjectAccessDenied{errAccessDenied{name, oper, aattrs}}
}
