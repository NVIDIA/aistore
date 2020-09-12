// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// SelectMsg extended flags
const (
	SelectCached    = 1 << iota // list only cached (Cloud buckets only)
	SelectMisplaced             // Include misplaced
	SelectDeleted               // Include marked for deletion
)

// ActionMsg is a JSON-formatted control structures for the REST API
type (
	ActionMsg struct {
		Action string      `json:"action"` // ActShutdown, ActRebalance, and many more (see api_const.go)
		Name   string      `json:"name"`   // action-specific (e.g., bucket name)
		Value  interface{} `json:"value"`  // ditto
	}
	ActValPromote struct {
		Target    string `json:"target"`
		ObjName   string `json:"objname"`
		Recurs    bool   `json:"recurs"`
		Overwrite bool   `json:"overwrite"`
		KeepOrig  bool   `json:"keep_original"`
		Verbose   bool   `json:"verbose"`
	}

	// TODO: `UUID` should be merged into `ContinuationToken`.
	// SelectMsg represents properties and options for listing objects.
	SelectMsg struct {
		UUID              string `json:"uuid"`               // ID to identify a single multi-page request
		Props             string `json:"props"`              // e.g. "checksum,size"
		TimeFormat        string `json:"time_format"`        // "RFC822" default - see the enum above
		Prefix            string `json:"prefix"`             // objname filter: return names starting with prefix
		PageSize          uint   `json:"pagesize"`           // max entries returned by list objects call
		StartAfter        string `json:"start_after"`        // start listing after (AIS buckets only)
		ContinuationToken string `json:"continuation_token"` // `BucketList.ContinuationToken`
		Flags             uint64 `json:"flags,string"`       // advanced filtering (SelectMsg extended flags)
		UseCache          bool   `json:"use_cache"`          // use proxy cache to speed up listing objects
	}

	BucketSummary struct {
		Bck
		ObjCount       uint64  `json:"count,string"`
		Size           uint64  `json:"size,string"`
		TotalDisksSize uint64  `json:"disks_size,string"`
		UsedPct        float64 `json:"used_pct"`
	}
	// BucketSummaryMsg represents options that can be set when asking for bucket summary.
	BucketSummaryMsg struct {
		UUID   string `json:"uuid"`
		Fast   bool   `json:"fast"`
		Cached bool   `json:"cached"`
	}
	BucketsSummaries []BucketSummary

	// ListMsg contains a list of files and a duration within which to get them
	ListMsg struct {
		ObjNames []string `json:"objnames"`
	}
	// RangeMsg contains a Prefix, Regex, and Range for a Range Operation
	RangeMsg struct {
		Template string `json:"template"`
	}

	// MountpathList contains two lists:
	// * Available - list of local mountpaths available to the storage target
	// * Disabled  - list of disabled mountpaths, the mountpaths that generated
	//	         IO errors followed by (FSHC) health check, etc.
	MountpathList struct {
		Available []string `json:"available"`
		Disabled  []string `json:"disabled"`
	}
)

// bucket properties
type (
	// BucketProps defines the configuration of the bucket with regard to
	// its type, checksum, and LRU. These characteristics determine its behavior
	// in response to operations on the bucket itself or the objects inside the bucket.
	//
	// Naming convention for setting/getting the particular props is defined as
	// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
	// one would need to write `ec.enabled`. For more info refer to `IterFields`.
	BucketProps struct {
		// Provider of the bucket. The value contains explicit provider
		// meaning that `` or `cloud` values are forbidden.
		Provider string `json:"provider" list:"readonly"`

		// if specified, creates association between an ais bucket and a given Cloud backend
		// in effect, making a Cloud bucket out of existing (and differently named) ais bucket.
		BackendBck Bck `json:"backend_bck,omitempty"`

		// Versioning can be enabled or disabled on a per-bucket basis
		Versioning VersionConf `json:"versioning"`

		// Cksum is the embedded struct of the same name
		Cksum CksumConf `json:"checksum"`

		// LRU is the embedded struct of the same name
		LRU LRUConf `json:"lru"`

		// Mirror defines local-mirroring policy for the bucket
		Mirror MirrorConf `json:"mirror"`

		// EC defines erasure coding setting for the bucket
		EC ECConf `json:"ec"`

		// Bucket access attributes - see Allow* above
		Access AccessAttrs `json:"access,string"`

		// Extra contains additional information which can depend on the provider.
		Extra struct {
			// [HTTP provider] Original URL prior to hashing.
			OrigURLBck string `json:"original_url,omitempty" list:"readonly"`

			// [AWS provider] Region where the cloud bucket is located.
			CloudRegion string `json:"cloud_region,omitempty" list:"readonly"`
		} `json:"extra,omitempty" list:"readonly"`

		// unique bucket ID
		BID uint64 `json:"bid,string" list:"omit"`

		// Bucket creation time
		Created int64 `json:"created,string" list:"readonly"`

		// non-empty when the bucket has been renamed (TODO: delayed deletion likewise)
		Renamed string `list:"omit"`
	}
	BucketPropsToUpdate struct {
		BackendBck *BckToUpdate         `json:"backend_bck"`
		Versioning *VersionConfToUpdate `json:"versioning"`
		Cksum      *CksumConfToUpdate   `json:"checksum"`
		LRU        *LRUConfToUpdate     `json:"lru"`
		Mirror     *MirrorConfToUpdate  `json:"mirror"`
		EC         *ECConfToUpdate      `json:"ec"`
		Access     *AccessAttrs         `json:"access,string"`
	}
	BckToUpdate struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

// object properties
type (
	ObjectProps struct {
		Name         string           `json:"name"`
		Bck          Bck              `json:"bucket"`
		Size         int64            `json:"size"`
		Version      string           `json:"version"`
		Atime        int64            `json:"atime"`
		Checksum     ObjectCksumProps `json:"checksum"`
		NumCopies    int              `json:"copies"`
		DataSlices   int              `list:"omit"`
		ParitySlices int              `list:"omit"`
		IsECCopy     bool             `list:"omit"`
		Present      bool             `json:"present"`
	}
	ObjectCksumProps struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
)

// GetPropsDefault is a list of default (most relevant) `GetProps*` options.
// NOTE: do **NOT** forget update this array when a prop is added/removed.
var GetPropsDefault = []string{
	GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime,
}

// GetPropsAll is a list of all `GetProps*` options.
// NOTE: do **NOT** forget update this array when a prop is added/removed.
var GetPropsAll = append(GetPropsDefault,
	GetPropsVersion, GetPropsCached, GetTargetURL, GetPropsStatus, GetPropsCopies, GetPropsEC,
)

///////////////
// SelectMsg //
///////////////

// NeedLocalData returns true if ListObjects for a cloud bucket needs
// to return object properties that can be retrieved only from local caches
func (msg *SelectMsg) NeedLocalData() bool {
	return msg.WantProp(GetPropsAtime) ||
		msg.WantProp(GetPropsStatus) ||
		msg.WantProp(GetPropsCopies) ||
		msg.WantProp(GetPropsCached)
}

// WantProp returns true if msg request requires to return propName property.
func (msg *SelectMsg) WantProp(propName string) bool {
	debug.Assert(!strings.ContainsRune(propName, ','))
	return strings.Contains(msg.Props, propName)
}

func (msg *SelectMsg) AddProps(propNames ...string) {
	for _, propName := range propNames {
		if msg.WantProp(propName) {
			continue
		}
		if msg.Props != "" {
			msg.Props += ","
		}
		msg.Props += propName
	}
}

func (msg *SelectMsg) PropsSet() (s StringSet) {
	props := strings.Split(msg.Props, ",")
	s = make(StringSet, len(props))
	for _, p := range props {
		s.Add(p)
	}
	return s
}

func (msg *SelectMsg) SetFlag(flag uint64) {
	msg.Flags |= flag
}

func (msg *SelectMsg) IsFlagSet(flags uint64) bool {
	return msg.Flags&flags == flags
}

// nolint:interfacer // the bucket is expected
func (msg *SelectMsg) ListObjectsCacheID(bck Bck) string {
	return fmt.Sprintf("%s/%s", bck.String(), msg.Prefix)
}

func (bs *BucketSummary) Aggregate(bckSummary BucketSummary) {
	bs.ObjCount += bckSummary.ObjCount
	bs.Size += bckSummary.Size
	bs.TotalDisksSize += bckSummary.TotalDisksSize
	bs.UsedPct = float64(bs.Size) * 100 / float64(bs.TotalDisksSize)
}

//////////////////////
// BucketsSummaries //
//////////////////////

func (s BucketsSummaries) Len() int           { return len(s) }
func (s BucketsSummaries) Less(i, j int) bool { return s[i].Bck.Less(s[j].Bck) }
func (s BucketsSummaries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BucketsSummaries) Aggregate(summary BucketSummary) BucketsSummaries {
	for idx, bckSummary := range s {
		if bckSummary.Bck.Equal(summary.Bck) {
			bckSummary.Aggregate(summary)
			s[idx] = bckSummary
			return s
		}
	}
	s = append(s, summary)
	return s
}

func (s BucketsSummaries) Get(bck Bck) (BucketSummary, bool) {
	for _, bckSummary := range s {
		if bckSummary.Bck.Equal(bck) {
			return bckSummary, true
		}
	}
	return BucketSummary{}, false
}

///////////////////////////
// bprops & config *Conf //
///////////////////////////

func (c *VersionConf) String() string {
	if !c.Enabled {
		return "Disabled"
	}

	text := "Enabled | Validate on WarmGET: "
	if c.ValidateWarmGet {
		text += "yes"
	} else {
		text += "no"
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

//
// by default, bucket props inherit global config
//
func DefaultAISBckProps() *BucketProps {
	c := GCO.Clone()
	if c.Cksum.Type == "" {
		c.Cksum.Type = ChecksumXXHash
	}
	return &BucketProps{
		Cksum:      c.Cksum,
		LRU:        c.LRU,
		Mirror:     c.Mirror,
		Versioning: c.Versioning,
		Access:     AllAccess(),
		EC:         c.EC,
	}
}

func DefaultCloudBckProps(header http.Header) (props *BucketProps) {
	props = DefaultAISBckProps()
	props.Versioning.Enabled = false
	return MergeCloudBckProps(props, header)
}

func MergeCloudBckProps(base *BucketProps, header http.Header) (props *BucketProps) {
	Assert(len(header) > 0)
	props = base.Clone()
	props.Provider = header.Get(HeaderCloudProvider)
	Assert(IsValidProvider(props.Provider))

	if props.Provider == ProviderHTTP {
		props.Extra.OrigURLBck = header.Get(HeaderOrigURLBck)
	}

	if region := header.Get(HeaderCloudRegion); region != "" {
		props.Extra.CloudRegion = region
	}

	if verStr := header.Get(HeaderBucketVerEnabled); verStr != "" {
		versioning, err := ParseBool(verStr)
		AssertNoErr(err)
		props.Versioning.Enabled = versioning
	}
	return props
}

/////////////////
// BucketProps //
/////////////////

func (bp *BucketProps) Clone() *BucketProps {
	to := *bp
	debug.Assert(bp.Equal(&to))
	return &to
}

func (bp *BucketProps) Equal(other *BucketProps) (eq bool) {
	src := *bp
	src.BID = other.BID
	src.Created = other.Created
	eq = reflect.DeepEqual(&src, other)
	return
}

func (bp *BucketProps) Validate(targetCnt int) error {
	if !IsValidProvider(bp.Provider) {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s)", bp.Provider, allProviders)
	}
	if !bp.BackendBck.IsEmpty() {
		if bp.BackendBck.Provider == "" {
			return fmt.Errorf("backend bucket (%q): provider is empty", bp.BackendBck)
		}
		if bp.BackendBck.Name == "" {
			return fmt.Errorf("backend bucket (%q) name is empty", bp.BackendBck)
		}
		if bp.BackendBck.IsHTTP() {
			return fmt.Errorf("backend bucket (%q) cannot be associated with %q provider",
				bp.BackendBck, ProviderHTTP)
		}
		if !bp.BackendBck.IsCloud() {
			return fmt.Errorf("backend bucket (%q) must be a Cloud bucket", bp.BackendBck)
		}
		if bp.Provider != ProviderAIS {
			return fmt.Errorf("backend bucket (%q) can only be set for AIS buckets", bp.BackendBck)
		}
	}

	validationArgs := &ValidationArgs{TargetCnt: targetCnt}
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

func NewBucketPropsToUpdate(nvs SimpleKVs) (props BucketPropsToUpdate, err error) {
	for key, val := range nvs {
		name, value := strings.ToLower(key), val

		// HACK: Some of the fields are present in `BucketProps` and not in
		// `BucketPropsToUpdate`. Therefore if a user would like to change such field,
		// `unknown field` would be returned in response. To make UX more pleasant we try
		// to set the value first in `BucketProps` which should report `readonly` in such case.
		if err := UpdateFieldValue(&BucketProps{}, name, value); err != nil {
			return props, err
		}

		if err := UpdateFieldValue(&props, name, value); err != nil {
			return props, err
		}
	}
	return
}
