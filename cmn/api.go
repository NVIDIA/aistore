// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Bprops - manageable, user-configurable, and inheritable (from cluster config).
// Includes per-bucket user-configurable checksum, version, LRU, erasure-coding, and more.
//
// At creation time, unless specified via api.CreateBucket, new bucket by default
// inherits its properties from the global configuration.
// * see api.CreateBucket for details
// * for all inheritable props, see DefaultProps below
//
// Naming convention for setting/getting the particular props is defined as
// joining the json tags with dot. Eg. when referring to `EC.Enabled` field
// one would need to write `ec.enabled`. For more info refer to `IterFields`.

const (
	PropBucketAccessAttrs  = "access"             // Bucket access attributes.
	PropBucketVerEnabled   = "versioning.enabled" // Enable/disable object versioning in a bucket.
	PropBucketCreated      = "created"            // Bucket creation time.
	PropBackendBck         = "backend_bck"
	PropBackendBckName     = PropBackendBck + ".name"
	PropBackendBckProvider = PropBackendBck + ".provider"
)

type (
	Bprops struct {
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
		Versioning  VersionConf     `json:"versioning"`                     // versioning (see "inherit")
	}

	ExtraProps struct {
		AWS  ExtraPropsAWS  `json:"aws,omitempty" list:"omitempty"`
		HTTP ExtraPropsHTTP `json:"http,omitempty" list:"omitempty"`
		HDFS ExtraPropsHDFS `json:"hdfs,omitempty" list:"omitempty"`
	}
	ExtraToSet struct { // ref. bpropsFilterExtra
		AWS  *ExtraPropsAWSToSet  `json:"aws"`
		HTTP *ExtraPropsHTTPToSet `json:"http"`
		HDFS *ExtraPropsHDFSToSet `json:"hdfs"`
	}

	ExtraPropsAWS struct {
		CloudRegion string `json:"cloud_region,omitempty" list:"readonly"`

		// from https://github.com/aws/aws-sdk-go/blob/main/aws/config.go:
		// - "An optional endpoint URL (hostname only or fully qualified URI)
		// that overrides the default generated endpoint."
		Endpoint string `json:"endpoint,omitempty"`

		// from https://github.com/aws/aws-sdk-go/blob/main/aws/session/session.go:
		// - "Overrides the config profile the Session should be created from. If not
		// set the value of the environment variable will be loaded (AWS_PROFILE,
		// or AWS_DEFAULT_PROFILE if the Shared Config is enabled)."
		Profile string `json:"profile,omitempty"`
	}
	ExtraPropsAWSToSet struct {
		CloudRegion *string `json:"cloud_region"`
		Endpoint    *string `json:"endpoint"`
		Profile     *string `json:"profile"`
	}

	ExtraPropsHTTP struct {
		// Original URL prior to hashing.
		OrigURLBck string `json:"original_url,omitempty" list:"readonly"`
	}
	ExtraPropsHTTPToSet struct {
		OrigURLBck *string `json:"original_url"`
	}

	ExtraPropsHDFS struct {
		// Reference directory.
		RefDirectory string `json:"ref_directory,omitempty"`
	}
	ExtraPropsHDFSToSet struct {
		RefDirectory *string `json:"ref_directory"`
	}

	// Once validated, BpropsToSet are copied to Bprops.
	// The struct may have extra fields that do not exist in Bprops.
	// Add tag 'copy:"skip"' to ignore those fields when copying values.
	BpropsToSet struct {
		BackendBck  *BackendBckToSet      `json:"backend_bck,omitempty"`
		Versioning  *VersionConfToSet     `json:"versioning,omitempty"`
		Cksum       *CksumConfToSet       `json:"checksum,omitempty"`
		LRU         *LRUConfToSet         `json:"lru,omitempty"`
		Mirror      *MirrorConfToSet      `json:"mirror,omitempty"`
		EC          *ECConfToSet          `json:"ec,omitempty"`
		Access      *apc.AccessAttrs      `json:"access,string,omitempty"`
		WritePolicy *WritePolicyConfToSet `json:"write_policy,omitempty"`
		Extra       *ExtraToSet           `json:"extra,omitempty"`
		Force       bool                  `json:"force,omitempty" copy:"skip" list:"omit"`
	}

	BackendBckToSet struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

/////////////////
// Bprops //
/////////////////

// By default, created buckets inherit their properties from the cluster (global) configuration.
// Global configuration, in turn, is protected versioned, checksummed, and replicated across the entire cluster.
//
// * Bucket properties can be changed at any time via `api.SetBprops`.
// * In addition, `api.CreateBucket` allows to specify (non-default) properties at bucket creation time.
// * Inherited defaults include checksum, LRU, etc. configurations - see below.
// * By default, LRU is disabled for AIS (`ais://`) buckets.
//
// See also:
//   - github.com/NVIDIA/aistore/blob/master/docs/bucket.md#default-bucket-properties
//   - BpropsToSet (above)
//   - ais.defaultBckProps()
func (bck *Bck) DefaultProps(c *ClusterConfig) *Bprops {
	lru := c.LRU
	if bck.IsAIS() {
		lru.Enabled = false
	}
	cksum := c.Cksum
	if cksum.Type == "" { // tests with empty cluster config
		cksum.Type = cos.ChecksumXXHash
	}
	wp := c.WritePolicy
	if wp.MD.IsImmediate() {
		wp.MD = apc.WriteImmediate
	}
	if wp.Data.IsImmediate() {
		wp.Data = apc.WriteImmediate
	}
	return &Bprops{
		Cksum:       cksum,
		LRU:         lru,
		Mirror:      c.Mirror,
		Versioning:  c.Versioning,
		Access:      apc.AccessAll,
		EC:          c.EC,
		WritePolicy: wp,
	}
}

func (bp *Bprops) SetProvider(provider string) {
	debug.Assert(apc.IsProvider(provider))
	bp.Provider = provider
}

func (bp *Bprops) Clone() *Bprops {
	to := *bp
	debug.Assert(bp.Equal(&to))
	return &to
}

func (bp *Bprops) Equal(other *Bprops) (eq bool) {
	src := *bp
	src.BID = other.BID
	src.Created = other.Created
	eq = reflect.DeepEqual(&src, other)
	return
}

func (bp *Bprops) Validate(targetCnt int) error {
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

func (bp *Bprops) Apply(propsToSet *BpropsToSet) {
	err := copyProps(propsToSet, bp, apc.Daemon)
	debug.AssertNoErr(err)
}

//
// BpropsToSet
//

func NewBpropsToSet(nvs cos.StrKVs) (props *BpropsToSet, err error) {
	props = &BpropsToSet{}
	for key, val := range nvs {
		name, value := strings.ToLower(key), val

		// HACK: Some of the fields are present in `Bprops` and not in `BpropsToSet`.
		// Thus, if user wants to change such field, `unknown field` will be returned.
		// To make UX more friendly we attempt to set the value in an empty `Bprops` first.
		if err := UpdateFieldValue(&Bprops{}, name, value); err != nil {
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
// Bucket Summary - result for a given bucket, and all results -------------------------------------------------
//

type (
	BsummResult struct {
		Bck
		apc.BsummResult
	}
	AllBsummResults []*BsummResult
)

// interface guard
var _ sort.Interface = (*AllBsummResults)(nil)

func (s AllBsummResults) Len() int           { return len(s) }
func (s AllBsummResults) Less(i, j int) bool { return s[i].Bck.Less(&s[j].Bck) }
func (s AllBsummResults) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s AllBsummResults) Aggregate(from *BsummResult) AllBsummResults {
	for _, to := range s {
		if to.Bck.Equal(&from.Bck) {
			aggr(from, to)
			return s
		}
	}
	s = append(s, from)
	return s
}

// across targets
func aggr(from, to *BsummResult) {
	if from.ObjSize.Min < to.ObjSize.Min {
		to.ObjSize.Min = from.ObjSize.Min
	}
	if from.ObjSize.Max > to.ObjSize.Max {
		to.ObjSize.Max = from.ObjSize.Max
	}
	to.ObjCount.Present += from.ObjCount.Present
	to.ObjCount.Remote += from.ObjCount.Remote
	to.TotalSize.OnDisk += from.TotalSize.OnDisk
	to.TotalSize.PresentObjs += from.TotalSize.PresentObjs
	to.TotalSize.RemoteObjs += from.TotalSize.RemoteObjs
}

func (s AllBsummResults) Finalize(dsize map[string]uint64, testingEnv bool) {
	var totalDisksSize uint64
	for _, tsiz := range dsize {
		totalDisksSize += tsiz
		if testingEnv {
			break
		}
	}
	for _, summ := range s {
		if summ.ObjCount.Present > 0 {
			summ.ObjSize.Avg = int64(cos.DivRoundU64(summ.TotalSize.PresentObjs, summ.ObjCount.Present))
		}
		summ.UsedPct = cos.DivRoundU64(summ.TotalSize.OnDisk*100, totalDisksSize)
	}
}

//
// Multi-object (list|range) operations source bucket => dest. bucket ---------------------------------------
//

type (
	// ArchiveBckMsg contains parameters to archive mutiple objects from the specified (source) bucket.
	// Destination bucket may the same as the source or a different one.
	// --------------------  NOTE on terminology:   ---------------------
	// "archive" is any (.tar, .tgz/.tar.gz, .zip, .tar.lz4) formatted object often also called "shard"
	//
	// See also: apc.PutApndArchArgs
	ArchiveBckMsg struct {
		ToBck Bck `json:"tobck"`
		apc.ArchiveMsg
	}

	//  Multi-object copy & transform (see also: TCBMsg)
	TCObjsMsg struct {
		ToBck Bck `json:"tobck"`
		apc.TCObjsMsg
	}
)

func (msg *ArchiveBckMsg) Cname() string { return msg.ToBck.Cname(msg.ArchName) }
