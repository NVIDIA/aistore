// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Bucket properties - manageable, user-configurable, and inheritable (from cluster config)
type (
	// BucketProps includes per-bucket user-configurable checksum, version, LRU, erasure-coding,
	// and more.
	//
	// At creation time, unless specified via api.CreateBucket, new bucket by default
	// inherits its properties from the global configuration.
	// * see api.CreateBucket for details
	// * for all inheritable props, see DefaultProps below
	//
	// Naming convention for setting/getting the particular props is defined as
	// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
	// one would need to write `ec.enabled`. For more info refer to `IterFields`.
	BucketProps struct {
		BackendBck  Bck             `json:"backend_bck,omitempty"` // makes remote bucket out of a given ais bucket
		Extra       ExtraProps      `json:"extra,omitempty" list:"omitempty"`
		WritePolicy WritePolicyConf `json:"write_policy"`
		Provider    string          `json:"provider" list:"readonly"`       // backend provider
		Renamed     string          `list:"omit"`                           // non-empty if the bucket has been renamed
		Cksum       CksumConf       `json:"checksum"`                       // the bucket's checksum
		EC          ECConf          `json:"ec"`                             // erasure coding
		LRU         LRUConf         `json:"lru"`                            // LRU (watermarks and enabled/disabled)
		Mirror      MirrorConf      `json:"mirror"`                         // mirroring
		Access      apc.AccessAttrs `json:"access,string"`                  // access permissions
		BID         uint64          `json:"bid,string" list:"omit"`         // unique ID
		Created     int64           `json:"created,string" list:"readonly"` // creation timestamp
		Versioning  VersionConf     `json:"versioning"`                     // versioning (see "inherit" here and elsewhere)
	}

	ExtraProps struct {
		AWS  ExtraPropsAWS  `json:"aws,omitempty" list:"omitempty"`
		HTTP ExtraPropsHTTP `json:"http,omitempty" list:"omitempty"`
		HDFS ExtraPropsHDFS `json:"hdfs,omitempty" list:"omitempty"`
	}
	ExtraToUpdate struct { // ref. bpropsFilterExtra
		AWS  *ExtraPropsAWSToUpdate  `json:"aws"`
		HTTP *ExtraPropsHTTPToUpdate `json:"http"`
		HDFS *ExtraPropsHDFSToUpdate `json:"hdfs"`
	}

	ExtraPropsAWS struct {
		CloudRegion string `json:"cloud_region,omitempty" list:"readonly"`

		// from https://github.com/aws/aws-sdk-go/blob/main/aws/config.go:
		//   "An optional endpoint URL (hostname only or fully qualified URI)
		//    that overrides the default generated endpoint."
		Endpoint string `json:"endpoint,omitempty"`
	}
	ExtraPropsAWSToUpdate struct {
		CloudRegion *string `json:"cloud_region"`
		Endpoint    *string `json:"endpoint"`
	}

	ExtraPropsHTTP struct {
		// Original URL prior to hashing.
		OrigURLBck string `json:"original_url,omitempty" list:"readonly"`
	}
	ExtraPropsHTTPToUpdate struct {
		OrigURLBck *string `json:"original_url"`
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
		BackendBck  *BackendBckToUpdate      `json:"backend_bck,omitempty"`
		Versioning  *VersionConfToUpdate     `json:"versioning,omitempty"`
		Cksum       *CksumConfToUpdate       `json:"checksum,omitempty"`
		LRU         *LRUConfToUpdate         `json:"lru,omitempty"`
		Mirror      *MirrorConfToUpdate      `json:"mirror,omitempty"`
		EC          *ECConfToUpdate          `json:"ec,omitempty"`
		Access      *apc.AccessAttrs         `json:"access,string,omitempty"`
		WritePolicy *WritePolicyConfToUpdate `json:"write_policy,omitempty"`
		Extra       *ExtraToUpdate           `json:"extra,omitempty"`
		Force       bool                     `json:"force,omitempty" copy:"skip" list:"omit"`
	}

	BackendBckToUpdate struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

// Bucket summary (result) for a given bucket
type (
	BckSumm struct {
		Bck
		ObjCount uint64 `json:"obj_count,string"`
		ObjSize  struct {
			Min int64 `json:"obj_min_size"`
			Avg int64 `json:"obj_avg_size"`
			Max int64 `json:"obj_max_size"`
		}
		Size           uint64 `json:"size,string"` // TODO -- FIXME: several different sizes: on-disk, objects, ...
		TotalDisksSize uint64 `json:"total_disks_size,string"`
		UsedPct        uint64 `json:"used_pct"`
		// presence, as in: apc.Flt*
		Present bool `json:"present,omitempty"`
	}
	BckSummaries []*BckSumm
)

// Multi-object (list|range) operations
type (
	// List of object names _or_ a template specifying { Prefix, Regex, and/or Range }
	SelectObjsMsg struct {
		Template string   `json:"template"`
		ObjNames []string `json:"objnames"`
	}

	// ArchiveMsg is used in CreateArchMultiObj operations; the message contains parameters
	// for archiving mutiple (source) objects as one of the supported cos.ArchExtensions types
	// at the specified (bucket) destination
	ArchiveMsg struct {
		ToBck       Bck    `json:"tobck"`
		TxnUUID     string `json:"-"`
		FromBckName string `json:"-"`
		ArchName    string `json:"archname"` // must have one of the cos.ArchExtensions
		Mime        string `json:"mime"`     // user-specified mime type takes precedence if defined
		SelectObjsMsg
		InclSrcBname          bool `json:"isbn"` // include source bucket name into the names of archived objects
		AllowAppendToExisting bool `json:"aate"` // allow adding a list or a range of objects to an existing archive
		ContinueOnError       bool `json:"coer"` // on err, keep running arc xaction in a any given multi-object transaction
	}

	//  Multi-object copy & transform (see also: TCBMsg)
	TCObjsMsg struct {
		ToBck Bck `json:"tobck"`
		SelectObjsMsg
		TxnUUID string `json:"-"`
		apc.TCBMsg
		ContinueOnError bool `json:"coer"` // ditto
	}
)

/////////////////
// BucketProps //
/////////////////

// By default, created buckets inherit their properties from the cluster (global) configuration.
// Global configuration, in turn, is protected versioned, checksummed, and replicated across the entire cluster.
//
// * Bucket properties can be changed at any time via `api.SetBucketProps`.
// * In addition, `api.CreateBucket` allows to specify (non-default) properties at bucket creation time.
// * Inherited defaults include checksum, LRU, etc. configurations - see below.
// * By default, LRU is disabled for AIS (`ais://`) buckets.
//
// See also:
//   - github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties
//   - BucketPropsToUpdate (above)
//   - ais.defaultBckProps()
func (bck *Bck) DefaultProps(cs ...*Config) *BucketProps {
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
	wp := c.WritePolicy
	if wp.MD.IsImmediate() {
		wp.MD = apc.WriteImmediate
	}
	if wp.Data.IsImmediate() {
		wp.Data = apc.WriteImmediate
	}
	return &BucketProps{
		Cksum:       c.Cksum,
		LRU:         c.LRU,
		Mirror:      c.Mirror,
		Versioning:  c.Versioning,
		Access:      apc.AccessAll,
		EC:          c.EC,
		WritePolicy: wp,
	}
}

func (bp *BucketProps) SetProvider(provider string) {
	debug.Assert(apc.IsProvider(provider))
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
	debug.Assert(apc.IsProvider(bp.Provider))
	if !bp.BackendBck.IsEmpty() {
		if bp.Provider != apc.AIS {
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
	var softErr error
	for _, pv := range []PropsValidator{&bp.Cksum, &bp.Mirror, &bp.EC, &bp.Extra, &bp.WritePolicy} {
		var err error
		if pv == &bp.EC {
			err = bp.EC.ValidateAsProps(targetCnt)
		} else if pv == &bp.Extra {
			err = bp.Extra.ValidateAsProps(bp.Provider)
		} else {
			err = pv.ValidateAsProps()
		}
		if err != nil {
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
	err := copyProps(*propsToUpdate, bp, apc.Daemon)
	debug.AssertNoErr(err)
}

//
// BucketPropsToUpdate
//

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

func (c *ExtraProps) ValidateAsProps(arg ...any) error {
	provider, ok := arg[0].(string)
	debug.Assert(ok)
	switch provider {
	case apc.HDFS:
		if c.HDFS.RefDirectory == "" {
			return fmt.Errorf("reference directory must be set for a bucket with HDFS provider")
		}
	case apc.HTTP:
		if c.HTTP.OrigURLBck == "" {
			return fmt.Errorf("original bucket URL must be set for a bucket with HTTP provider")
		}
	}
	return nil
}

//
// bucket summary
//

// interface guard
var _ sort.Interface = (*BckSummaries)(nil)

func NewBckSumm(bck *Bck, totalDisksSize uint64) (bs *BckSumm) {
	bs = &BckSumm{Bck: *bck, TotalDisksSize: totalDisksSize}
	bs.ObjSize.Min = math.MaxInt64
	return
}

func (s BckSummaries) Len() int           { return len(s) }
func (s BckSummaries) Less(i, j int) bool { return s[i].Bck.Less(&s[j].Bck) }
func (s BckSummaries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s BckSummaries) Aggregate(from *BckSumm) BckSummaries {
	for _, to := range s {
		if to.Bck.Equal(&from.Bck) {
			aggr(from, to)
			return s
		}
	}
	s = append(s, from)
	return s
}

func aggr(from, to *BckSumm) {
	if from.ObjSize.Min < to.ObjSize.Min {
		to.ObjSize.Min = from.ObjSize.Min
	}
	if from.ObjSize.Max > to.ObjSize.Max {
		to.ObjSize.Max = from.ObjSize.Max
	}
	to.ObjCount += from.ObjCount
	to.Size += from.Size
}

func (s BckSummaries) Finalize(dsize map[string]uint64, testingEnv bool) {
	var totalDisksSize uint64
	for _, tsiz := range dsize {
		totalDisksSize += tsiz
		if testingEnv {
			break
		}
	}
	for _, summ := range s {
		if summ.ObjSize.Min == math.MaxInt64 {
			summ.ObjSize.Min = 0 // alternatively, CLI to show `-`
		}
		if summ.ObjCount > 0 {
			summ.ObjSize.Avg = int64(cos.DivRoundU64(summ.Size, summ.ObjCount))
		}
		summ.UsedPct = cos.DivRoundU64(summ.Size*100, totalDisksSize)
	}
}

///////////////////
// SelectObjsMsg //
///////////////////

// NOTE: to operate on an entire bucket, use empty SelectObjsMsg{}

func (lrm *SelectObjsMsg) IsList() bool      { return len(lrm.ObjNames) > 0 }
func (lrm *SelectObjsMsg) HasTemplate() bool { return lrm.Template != "" }

////////////////
// ArchiveMsg //
////////////////

func (msg *ArchiveMsg) FullName() string { return filepath.Join(msg.ToBck.Name, msg.ArchName) }
