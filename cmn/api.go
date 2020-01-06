// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// ActionMsg is a JSON-formatted control structures for the REST API
type ActionMsg struct {
	Action string      `json:"action"` // shutdown, restart, setconfig - the enum below
	Name   string      `json:"name"`   // action-specific params
	Value  interface{} `json:"value"`
}

type ActValPromote struct {
	Target    string `json:"target"`
	Objname   string `json:"objname"`
	OmitBase  string `json:"omit_base"`
	Recurs    bool   `json:"recurs"`
	Overwrite bool   `json:"overwrite"`
	Verbose   bool   `json:"verbose"`
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
	ActAsyncTask:   {},
	ActRenameLB:    {},
	ActCopyBucket:  {},
	ActECEncode:    {},
}

// SelectMsg represents properties and options for requests which fetch entities
// Note: if Fast is `true` then paging is disabled - all items are returned
//       in one response. The result list is unsorted and contains only object
//       names: even field `Status` is filled with zero value
type SelectMsg struct {
	Props      string `json:"props"`         // e.g. "checksum, size"|"atime, size"|"iscached"|"bucket, size"
	TimeFormat string `json:"time_format"`   // "RFC822" default - see the enum above
	Prefix     string `json:"prefix"`        // object name filter: return only objects which name starts with prefix
	PageMarker string `json:"pagemarker"`    // marker - the last object in previous page
	PageSize   int    `json:"pagesize"`      // maximum number of entries returned by list bucket call
	TaskID     int64  `json:"taskid,string"` // task ID for long running requests
	Fast       bool   `json:"fast"`          // performs a fast traversal of the bucket contents (returns only names)
	Cached     bool   `json:"cached"`        // for cloud buckets - list only cached objects
}

// ListRangeMsgBase contains fields common to Range and List operations
type ListRangeMsgBase struct {
	Deadline time.Duration `json:"deadline,omitempty"`
	Wait     bool          `json:"wait,omitempty"`
}

// ListMsg contains a list of files and a duration within which to get them
type ListMsg struct {
	ListRangeMsgBase
	Objnames []string `json:"objname"`
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
	All    bool   `json:"all,omitempty"`
}

// GetPropsAll is a list of all GetProps* options
var GetPropsAll = []string{
	GetPropsChecksum, GetPropsSize, GetPropsAtime,
	GetPropsIsCached, GetPropsVersion,
	GetTargetURL, GetPropsStatus, GetPropsCopies,
}

// NeedLocalData returns true if ListBucket for a cloud bucket needs
// to return object properties that can be retrieved only from local caches
func (msg *SelectMsg) NeedLocalData() bool {
	return strings.Contains(msg.Props, GetPropsAtime) ||
		strings.Contains(msg.Props, GetPropsStatus) ||
		strings.Contains(msg.Props, GetPropsCopies) ||
		strings.Contains(msg.Props, GetPropsIsCached)
}

// WantProp returns true if msg request requires to return propName property
func (msg *SelectMsg) WantProp(propName string) bool {
	return strings.Contains(msg.Props, propName)
}

func (msg *SelectMsg) AddProps(propNames ...string) {
	var props strings.Builder
	props.WriteString(msg.Props)
	for _, propName := range propNames {
		if msg.WantProp(propName) {
			continue
		}
		if props.Len() > 0 {
			props.WriteString(",")
		}
		props.WriteString(propName)
	}

	msg.Props = props.String()
}

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the SelectMsg
// Flags is a bit field:
// 0-2: objects status, all statuses are mutually exclusive, so it can hold up
//      to 8 different statuses. Now only OK=0, Moved=1, Deleted=2 are supported
// 3:   CheckExists (for cloud bucket it shows if the object in local cache)
type BucketEntry struct {
	Name      string `json:"name"`                  // name of the object - note: does not include the bucket name
	Size      int64  `json:"size,string,omitempty"` // size in bytes
	Checksum  string `json:"checksum,omitempty"`    // checksum
	Atime     string `json:"atime,omitempty"`       // formatted as per SelectMsg.TimeFormat
	Version   string `json:"version,omitempty"`     // version/generation ID. In GCP it is int64, in AWS it is a string
	TargetURL string `json:"targetURL,omitempty"`   // URL of target which has the entry
	Copies    int16  `json:"copies,omitempty"`      // ## copies (non-replicated = 1)
	Flags     uint16 `json:"flags,omitempty"`       // object flags, like CheckExists, IsMoved etc
}

func (be *BucketEntry) CheckExists() bool {
	return be.Flags&EntryIsCached != 0
}
func (be *BucketEntry) SetExists() {
	be.Flags |= EntryIsCached
}

func (be *BucketEntry) IsStatusOK() bool {
	return be.Flags&EntryStatusMask == 0
}

// BucketList represents the contents of a given bucket - somewhat analogous to the 'ls <bucket-name>'
type BucketList struct {
	Entries    []*BucketEntry `json:"entries"`
	PageMarker string         `json:"pagemarker"`
}

type BucketSummary struct {
	Name           string `json:"name"`
	ObjCount       uint64 `json:"count,string"`
	Size           uint64 `json:"size,string"`
	TotalDisksSize uint64 `json:"disks_size,string"`
	UsedPct        uint64 `json:"used_pct"`
	Provider       string `json:"provider"`
}

func (bs *BucketSummary) Aggregate(bckSummary BucketSummary) {
	bs.ObjCount += bckSummary.ObjCount
	bs.Size += bckSummary.Size
	bs.TotalDisksSize += bckSummary.TotalDisksSize
	bs.UsedPct = bs.Size * 100 / bs.TotalDisksSize
}

type BucketsSummaries map[string]BucketSummary

// BucketNames is used to transfer all bucket names known to the system
type BucketNames struct {
	Cloud []string `json:"cloud"`
	AIS   []string `json:"ais"`
}

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
//
// Naming convention for setting/getting the particular props is defined as
// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
// one would need to write `ec.enabled`. For more info refer to `IterFields`.
//
// nolint:maligned // no performance critical code
type BucketProps struct {
	// CloudProvider can be "aws", "gcp" (clouds) - or "ais".
	// If a bucket is local, CloudProvider must be "ais".
	// Otherwise, it must be "aws" or "gcp".
	CloudProvider string `json:"cloud_provider" list:"readonly"`

	// Versioning can be enabled or disabled on a per-bucket basis
	Versioning VersionConf `json:"versioning"`

	// Tier location and tier/cloud policies
	Tiering TierConf `json:"tier"`

	// Cksum is the embedded struct of the same name
	Cksum CksumConf `json:"cksum"`

	// LRU is the embedded struct of the same name
	LRU LRUConf `json:"lru"`

	// Mirror defines local-mirroring policy for the bucket
	Mirror MirrorConf `json:"mirror"`

	// EC defines erasure coding setting for the bucket
	EC ECConf `json:"ec"`

	// Bucket access attributes - see Allow* above
	AccessAttrs uint64 `json:"aattrs,string"`

	// unique bucket ID
	BID uint64 `json:"bid,string" list:"readonly"`

	// non-empty when the bucket has been renamed (TODO: delayed deletion likewise)
	Renamed string `list:"omit"`

	// Determines if the bucket has been binded to some action and currently
	// cannot be updated or changed in anyway until the action finishes.
	InProgress bool `json:"in_progress,omitempty" list:"omit"`
}

type BucketPropsToUpdate struct {
	Versioning  *VersionConfToUpdate `json:"versioning"`
	Cksum       *CksumConfToUpdate   `json:"cksum"`
	LRU         *LRUConfToUpdate     `json:"lru"`
	Mirror      *MirrorConfToUpdate  `json:"mirror"`
	EC          *ECConfToUpdate      `json:"ec"`
	AccessAttrs *uint64              `json:"attrs,string"`
}

type TierConf struct {
	// NextTierURL is an absolute URI corresponding to the primary proxy
	// of the next tier configured for the bucket specified
	NextTierURL string `json:"next_url"`

	// ReadPolicy determines if a read will be from cloud or next tier
	// specified by NextTierURL. Default: "next_tier"
	ReadPolicy string `json:"read_policy"`

	// WritePolicy determines if a write will be to cloud or next tier
	// specified by NextTierURL. Default: "cloud"
	WritePolicy string `json:"write_policy"`
}

// ECConfig - per-bucket erasure coding configuration
type ECConf struct {
	ObjSizeLimit int64  `json:"objsize_limit"` // objects below this size are replicated instead of EC'ed
	DataSlices   int    `json:"data_slices"`   // number of data slices
	ParitySlices int    `json:"parity_slices"` // number of parity slices/replicas
	Compression  string `json:"compression"`   // see CompressAlways, etc. enum
	Enabled      bool   `json:"enabled"`       // EC is enabled
}

type ECConfToUpdate struct {
	Enabled      *bool   `json:"enabled"`
	ObjSizeLimit *int64  `json:"objsize_limit"`
	DataSlices   *int    `json:"data_slices"`
	ParitySlices *int    `json:"parity_slices"`
	Compression  *string `json:"compression"`
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

	toValidate := make([]string, 0)
	add := func(val bool, name string) {
		if val {
			toValidate = append(toValidate, name)
		}
	}
	add(c.ValidateColdGet, "ColdGET")
	add(c.ValidateWarmGet, "WarmGET")
	add(c.ValidateObjMove, "ObjectMove")
	add(c.EnableReadRange, "ReadRange")

	toValidateStr := "Nothing"
	if len(toValidate) > 0 {
		toValidateStr = strings.Join(toValidate, ",")
	}

	return fmt.Sprintf("Type: %s | Validate: %s", c.Type, toValidateStr)
}

func (c *LRUConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}
	return fmt.Sprintf("Watermarks: %d%%/%d%% | Do not evict time: %s | OOS: %v%%",
		c.LowWM, c.HighWM, c.DontEvictTimeStr, c.OOS)
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
	return fmt.Sprintf("%d:%d (%s)", c.DataSlices, c.ParitySlices, B2S(objSizeLimit, 0))
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
	Size         int64
	Version      string
	Atime        time.Time
	Checksum     string
	NumCopies    int
	DataSlices   int
	ParitySlices int
	IsECCopy     bool
	Present      bool
	BckIsAIS     bool
}

func DefaultBucketProps() *BucketProps {
	c := GCO.Clone()
	c.Cksum.Type = PropInherit
	return &BucketProps{
		Cksum:       c.Cksum,
		LRU:         c.LRU,
		Mirror:      c.Mirror,
		Versioning:  c.Versioning,
		AccessAttrs: AllowAllAccess,
		EC:          c.EC,
	}
}

func (to *BucketProps) CopyFrom(from *BucketProps) {
	src, err := jsoniter.Marshal(from)
	AssertNoErr(err)
	err = jsoniter.Unmarshal(src, to)
	AssertNoErr(err)
}

func (from *BucketProps) Clone() *BucketProps {
	to := &BucketProps{}
	to.CopyFrom(from)
	return to
}

func (p1 *BucketProps) Equal(p2 *BucketProps) bool {
	var (
		jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary
		p11        = p1.Clone()
	)
	p11.BID = p2.BID

	s1, _ := jsonCompat.Marshal(p11)
	s2, _ := jsonCompat.Marshal(p2)
	return string(s1) == string(s2)
}

func (bp *BucketProps) Validate(bckIsAIS bool, targetCnt int, urlOutsideCluster func(string) bool) error {
	if bp.Tiering.NextTierURL != "" {
		if _, err := url.ParseRequestURI(bp.Tiering.NextTierURL); err != nil {
			return fmt.Errorf("invalid next tier URL: %s, err: %v", bp.Tiering.NextTierURL, err)
		}
		if !urlOutsideCluster(bp.Tiering.NextTierURL) {
			return fmt.Errorf("invalid next tier URL: %s, URL is in current cluster", bp.Tiering.NextTierURL)
		}
	}
	if err := validateCloudProvider(bp.CloudProvider, bckIsAIS); err != nil {
		return err
	}
	if bp.Tiering.ReadPolicy != "" && bp.Tiering.ReadPolicy != RWPolicyCloud && bp.Tiering.ReadPolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid read policy: %s", bp.Tiering.ReadPolicy)
	}
	if bp.Tiering.ReadPolicy == RWPolicyCloud && bckIsAIS {
		return fmt.Errorf("read policy for ais bucket cannot be '%s'", RWPolicyCloud)
	}
	if bp.Tiering.WritePolicy != "" && bp.Tiering.WritePolicy != RWPolicyCloud && bp.Tiering.WritePolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid write policy: %s", bp.Tiering.WritePolicy)
	}
	if bp.Tiering.WritePolicy == RWPolicyCloud && bckIsAIS {
		return fmt.Errorf("write policy for ais bucket cannot be '%s'", RWPolicyCloud)
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
			if !bckIsAIS {
				bp.Tiering.WritePolicy = RWPolicyCloud
			}
		}
	}
	validationArgs := &ValidationArgs{BckIsAIS: bckIsAIS, TargetCnt: targetCnt}
	validators := []PropsValidator{&bp.Cksum, &bp.LRU, &bp.Mirror, &bp.EC}
	for _, validator := range validators {
		if err := validator.ValidateAsProps(validationArgs); err != nil {
			return err
		}
	}

	if bp.Mirror.Enabled && bp.EC.Enabled {
		return fmt.Errorf("cannot enable mirroring and ec at the same time for the same bucket")
	}
	return nil
}

func (bp *BucketProps) Apply(propsToUpdate BucketPropsToUpdate) {
	copyProps(propsToUpdate, bp)
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

func NewBucketPropsToUpdate(nvs SimpleKVs) (props BucketPropsToUpdate, err error) {
	props = BucketPropsToUpdate{
		Versioning: &VersionConfToUpdate{},
		Cksum:      &CksumConfToUpdate{},
		LRU:        &LRUConfToUpdate{},
		Mirror:     &MirrorConfToUpdate{},
		EC:         &ECConfToUpdate{},
	}

	for key, val := range nvs {
		name, value := strings.ToLower(key), val

		if err := UpdateFieldValue(&props, name, value); err != nil {
			return props, fmt.Errorf("unknown property '%s'", name)
		}
	}
	return
}
