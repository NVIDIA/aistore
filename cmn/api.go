// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		Recursive bool   `json:"recursive"`
		Overwrite bool   `json:"overwrite"`
		KeepOrig  bool   `json:"keep_original"`
	}
	ActValRmNode struct {
		DaemonID      string `json:"sid"`
		SkipRebalance bool   `json:"skip_rebalance"`
		RmUserData    bool   `json:"rm_user_data"` // remove user data (decommission-only)
		NoShutdown    bool   `json:"no_shutdown"`
	}
)

// SelectMsg extended flags
const (
	SelectCached    = 1 << iota // list only cached (Cloud buckets only)
	SelectMisplaced             // Include misplaced
	SelectDeleted               // Include marked for deletion
	SelectArchDir               // expand archives as directories
)

type (
	// options and flags to list objects
	SelectMsg struct {
		UUID              string `json:"uuid"`               // ID to identify a single multi-page request
		Props             string `json:"props"`              // e.g. "checksum,size"
		TimeFormat        string `json:"time_format"`        // "RFC822" default - see the enum above
		Prefix            string `json:"prefix"`             // objname filter: return names starting with prefix
		StartAfter        string `json:"start_after"`        // start listing after (AIS buckets only)
		ContinuationToken string `json:"continuation_token"` // `BucketList.ContinuationToken`
		Flags             uint64 `json:"flags,string"`       // enum {SelectCached, ..., SelectArchDir } - see above
		PageSize          uint   `json:"pagesize"`           // max entries returned by list objects call
		UseCache          bool   `json:"use_cache"`          // use proxy cache to speed up listing objects
	}

	// control message to generate bucket summary or summaries
	BucketSummaryMsg struct {
		UUID   string `json:"uuid"`
		Fast   bool   `json:"fast"`
		Cached bool   `json:"cached"`
	}
	// bucket summary (result) for a given bucket
	BucketSummary struct {
		Bck
		ObjCount       uint64  `json:"count,string"`
		Size           uint64  `json:"size,string"`
		TotalDisksSize uint64  `json:"disks_size,string"`
		UsedPct        float64 `json:"used_pct"`
	}
	BucketsSummaries []BucketSummary
)

// 1. copy & transform bucket to bucket
type (
	CopyBckMsg struct {
		Prefix string `json:"prefix"`  // Prefix added to each resulting object.
		DryRun bool   `json:"dry_run"` // Don't perform any PUT
	}
	TCBMsg struct {
		// Resulting objects names will have this extension. Warning: if in a source bucket exist two objects with the
		// same base name, but different extension, specifying this field might cause object overriding. This is because
		// of resulting name conflict.
		// TODO: this field might not be required when transformation on subset (template) of bucket is supported.
		Ext cos.SimpleKVs `json:"ext"`

		ID             string       `json:"id,omitempty"`              // optional, ETL only
		RequestTimeout cos.Duration `json:"request_timeout,omitempty"` // optional, ETL only

		CopyBckMsg
	}
)

// MountpathList contains two lists:
// * Available - list of local mountpaths available to the storage target
// * WaitingDD - waiting for resilvering completion to be detached or disabled (moved to `Disabled`)
// * Disabled  - list of disabled mountpaths, the mountpaths that generated
//	         IO errors followed by (FSHC) health check, etc.
type (
	MountpathList struct {
		Available []string `json:"available"`
		WaitingDD []string `json:"waiting_dd"`
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

		// Metadata write policy
		MDWrite MDWritePolicy `json:"md_write"`

		// EC defines erasure coding setting for the bucket
		EC ECConf `json:"ec"`

		// Bucket access attributes - see Allow* above
		Access AccessAttrs `json:"access,string"`

		// Extra contains additional information which can depend on the provider.
		Extra ExtraProps `json:"extra,omitempty" list:"omitempty"`

		// Unique bucket ID
		BID uint64 `json:"bid,string" list:"omit"`

		// Bucket creation time
		Created int64 `json:"created,string" list:"readonly"`

		// Non-empty when the bucket has been renamed.
		// TODO: Could be used for delayed deletion.
		Renamed string `list:"omit"`
	}

	ExtraProps struct {
		AWS  ExtraPropsAWS  `json:"aws,omitempty" list:"omitempty"`
		HTTP ExtraPropsHTTP `json:"http,omitempty" list:"omitempty"`
		HDFS ExtraPropsHDFS `json:"hdfs,omitempty" list:"omitempty"`
	}
	ExtraToUpdate struct {
		HDFS *ExtraPropsHDFSToUpdate `json:"hdfs"`
	}

	ExtraPropsAWS struct {
		// Region where AWS bucket is located.
		CloudRegion string `json:"cloud_region,omitempty" list:"readonly"`
	}

	ExtraPropsHTTP struct {
		// Original URL prior to hashing.
		OrigURLBck string `json:"original_url,omitempty" list:"readonly"`
	}

	ExtraPropsHDFS struct {
		// Reference directory.
		RefDirectory string `json:"ref_directory,omitempty"`
	}
	ExtraPropsHDFSToUpdate struct {
		RefDirectory *string `json:"ref_directory"`
	}

	// Once validated, BucketPropsToUpdate are copied to BucketProps.
	// The struct may have extra fields that do not exist in BucketProps.
	// Add tag 'copy:"skip"' to ignore those fields when copying values.
	BucketPropsToUpdate struct {
		BackendBck *BckToUpdate         `json:"backend_bck"`
		Versioning *VersionConfToUpdate `json:"versioning"`
		Cksum      *CksumConfToUpdate   `json:"checksum"`
		LRU        *LRUConfToUpdate     `json:"lru"`
		Mirror     *MirrorConfToUpdate  `json:"mirror"`
		EC         *ECConfToUpdate      `json:"ec"`
		Access     *AccessAttrs         `json:"access,string"`
		MDWrite    *MDWritePolicy       `json:"md_write"`
		Extra      *ExtraToUpdate       `json:"extra"`
		Force      bool                 `json:"force" copy:"skip" list:"omit"`
	}

	BckToUpdate struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

// sysinfo
type (
	CapacityInfo struct {
		Used    uint64  `json:"fs_used,string"`
		Total   uint64  `json:"fs_capacity,string"`
		PctUsed float64 `json:"pct_fs_used"`
	}
	TSysInfo struct {
		cos.SysInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*cos.SysInfo `json:"proxy"`
		Target map[string]*TSysInfo    `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  cos.JSONRawMsgs `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)

////////////////
// ArchiveMsg //
////////////////
func (msg *ArchiveMsg) FullName() string { return filepath.Join(msg.ToBck.Name, msg.ArchName) }

///////////////
// SelectMsg //
///////////////

// NeedLocalMD indicates that ListObjects for a remote bucket needs
// to include AIS-maintained metadata: access time, etc.
func (msg *SelectMsg) NeedLocalMD() bool {
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

func (msg *SelectMsg) PropsSet() (s cos.StringSet) {
	props := strings.Split(msg.Props, ",")
	s = make(cos.StringSet, len(props))
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

func (msg *SelectMsg) ListObjectsCacheID(bck Bck) string {
	return fmt.Sprintf("%s/%s", bck.String(), msg.Prefix)
}

func (msg *SelectMsg) Clone() *SelectMsg {
	c := &SelectMsg{}
	cos.CopyStruct(c, msg)
	return c
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
	if c.Type == cos.ChecksumNone {
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
	return fmt.Sprintf("Watermarks: %d%%/%d%% | Do not evict time: %v | OOS: %v%%",
		c.LowWM, c.HighWM, c.DontEvictTime, c.OOS)
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

func (c *ResilverConf) String() string {
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
	return fmt.Sprintf("%d:%d (%s)", c.DataSlices, c.ParitySlices, cos.B2S(objSizeLimit, 0))
}

func (c *ECConf) RequiredEncodeTargets() int {
	// data slices + parity slices + 1 target for original object
	return c.DataSlices + c.ParitySlices + 1
}

func (c *ECConf) RequiredRestoreTargets() int {
	return c.DataSlices
}

func (c *ExtraProps) ValidateAsProps(args *ValidationArgs) error {
	switch args.Provider {
	case ProviderHDFS:
		if c.HDFS.RefDirectory == "" {
			return fmt.Errorf("reference directory must be set for a bucket with HDFS provider")
		}
	case ProviderHTTP:
		if c.HTTP.OrigURLBck == "" {
			return fmt.Errorf("original bucket URL must be set for a bucket with HTTP provider")
		}
	}
	return nil
}

/////////////////
// BucketProps //
/////////////////

// By default, created buckets inherit their properties from the cluster (aka "global") configuration.
// Global configuration, in turn, is protected versioned, checksummed, and replicated across the entire cluster.
//
// NOTE:
// * Bucket properties can be changed at any time via `api.SetBucketProps`.
// * In addition, `api.CreateBucket` allows to specify (non-default) properties at bucket creation time.
// * Inherited defaults include checksum, LRU, etc. configurations - see below.
// * By default, LRU is disabled for AIS (`ais://`) buckets.
//
// See also:
//    * github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties
//    * BucketPropsToUpdate (above)
//    * ais.defaultBckProps()
func DefaultBckProps(bck Bck, cs ...*Config) *BucketProps {
	var c *Config
	if len(cs) > 0 {
		c = &Config{}
		cos.CopyStruct(c, cs[0])
	} else {
		c = GCO.Clone()
		c.Cksum.Type = cos.ChecksumXXHash
	}
	if bck.IsAIS() {
		c.LRU.Enabled = false
	}
	return &BucketProps{
		Cksum:      c.Cksum,
		LRU:        c.LRU,
		Mirror:     c.Mirror,
		Versioning: c.Versioning,
		Access:     AccessAll,
		EC:         c.EC,
		MDWrite:    c.MDWrite,
	}
}

func (bp *BucketProps) SetProvider(provider string) {
	debug.Assert(IsNormalizedProvider(provider))
	bp.Provider = provider
}

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
	debug.Assert(IsNormalizedProvider(bp.Provider))
	if !bp.BackendBck.IsEmpty() {
		if bp.Provider != ProviderAIS {
			return fmt.Errorf("wrong bucket provider %q: only AIS buckets can have remote backend (%q)",
				bp.Provider, bp.BackendBck)
		}
		if bp.BackendBck.Provider == "" {
			return fmt.Errorf("backend bucket %q: provider is empty", bp.BackendBck)
		}
		if bp.BackendBck.Name == "" {
			return fmt.Errorf("backend bucket %q name is empty", bp.BackendBck)
		}
		if !bp.BackendBck.IsRemote() {
			return fmt.Errorf("backend bucket %q must be remote", bp.BackendBck)
		}
	}

	var (
		softErr        error
		validationArgs = &ValidationArgs{Provider: bp.Provider, TargetCnt: targetCnt}
		validators     = []PropsValidator{&bp.Cksum, &bp.LRU, &bp.Mirror, &bp.EC, &bp.Extra, bp.MDWrite}
	)
	for _, validator := range validators {
		if err := validator.ValidateAsProps(validationArgs); err != nil {
			if !IsErrSoft(err) {
				return err
			}
			softErr = err
		}
	}

	if bp.Mirror.Enabled && bp.EC.Enabled {
		return fmt.Errorf("cannot enable mirroring and ec at the same time for the same bucket")
	}
	return softErr
}

func (bp *BucketProps) Apply(propsToUpdate *BucketPropsToUpdate) {
	err := copyProps(*propsToUpdate, bp, Daemon)
	debug.AssertNoErr(err)
}

func NewBucketPropsToUpdate(nvs cos.SimpleKVs) (props *BucketPropsToUpdate, err error) {
	props = &BucketPropsToUpdate{}
	for key, val := range nvs {
		name, value := strings.ToLower(key), val

		// HACK: Some of the fields are present in `BucketProps` and not in `BucketPropsToUpdate`.
		// Thus, if user wants to change such field, `unknown field` will be returned.
		// To make UX more friendly we attempt to set the value in an empty `BucketProps` first.
		if err := UpdateFieldValue(&BucketProps{}, name, value); err != nil {
			return props, err
		}

		if err := UpdateFieldValue(props, name, value); err != nil {
			return props, err
		}
	}
	return
}

func (msg *TCBMsg) Validate() error {
	if msg.ID == "" {
		return ErrETLMissingUUID
	}
	return nil
}

// Replace extension and add suffix if provided.
func (msg *TCBMsg) ToName(name string) string {
	if msg.Ext != nil {
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			ext := name[:idx]
			if replacement, exists := msg.Ext[ext]; exists {
				name = name[:idx+1] + strings.TrimLeft(replacement, ".")
			}
		}
	}
	if msg.Prefix != "" {
		name = msg.Prefix + name
	}
	return name
}
