// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		MDWrite apc.MDWritePolicy `json:"md_write"`

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
		MDWrite    *apc.MDWritePolicy   `json:"md_write"`
		Extra      *ExtraToUpdate       `json:"extra"`
		Force      bool                 `json:"force" copy:"skip" list:"omit"`
	}

	BckToUpdate struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

// By default, created buckets inherit their properties from the cluster (global) configuration.
// Global configuration, in turn, is protected versioned, checksummed, and replicated across the entire cluster.
//
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
		if bp.Provider != apc.ProviderAIS {
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
	for _, pv := range []PropsValidator{&bp.Cksum, &bp.LRU, &bp.Mirror, &bp.EC, &bp.Extra, bp.MDWrite} {
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

/////////////////////////
// BucketPropsToUpdate //
/////////////////////////

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

func (c *ExtraProps) ValidateAsProps(arg ...interface{}) error {
	provider, ok := arg[0].(string)
	debug.Assert(ok)
	switch provider {
	case apc.ProviderHDFS:
		if c.HDFS.RefDirectory == "" {
			return fmt.Errorf("reference directory must be set for a bucket with HDFS provider")
		}
	case apc.ProviderHTTP:
		if c.HTTP.OrigURLBck == "" {
			return fmt.Errorf("original bucket URL must be set for a bucket with HTTP provider")
		}
	}
	return nil
}
