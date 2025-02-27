// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		Features    feat.Flags      `json:"features,string"`                // assorted features from feat.Bucket
		BID         uint64          `json:"bid,string" list:"omit"`         // unique ID
		Created     int64           `json:"created,string" list:"readonly"` // creation timestamp
		Versioning  VersionConf     `json:"versioning"`                     // versioning (see "inherit")
		RateLimit   RateLimitConf   `json:"rate_limit"`                     // adaptive rate limiting (front, back) if enabled
	}

	ExtraProps struct {
		HTTP ExtraPropsHTTP `json:"http,omitempty" list:"omitempty"`
		HDFS ExtraPropsHDFS `json:"hdfs,omitempty" list:"omitempty"` // NOTE: obsolete; rm with meta-version
		AWS  ExtraPropsAWS  `json:"aws,omitempty" list:"omitempty"`
	}
	ExtraToSet struct { // ref. bpropsFilterExtra
		AWS  *ExtraPropsAWSToSet  `json:"aws"`
		HTTP *ExtraPropsHTTPToSet `json:"http"`
		HDFS *ExtraPropsHDFSToSet `json:"hdfs"` // ditto
	}

	ExtraPropsAWS struct {
		CloudRegion string `json:"cloud_region,omitempty"`

		// from https://github.com/aws/aws-sdk-go/blob/main/aws/config.go:
		// - "An optional endpoint URL (hostname only or fully qualified URI)
		// that overrides the default generated endpoint."
		Endpoint string `json:"endpoint,omitempty"`

		// from https://github.com/aws/aws-sdk-go/blob/main/aws/session/session.go:
		// - "Overrides the config profile the Session should be created from. If not
		// set the value of the environment variable will be loaded (AWS_PROFILE,
		// or AWS_DEFAULT_PROFILE if the Shared Config is enabled)."
		Profile string `json:"profile,omitempty"`

		// Amazon S3: 1000
		// - https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-pagination.html#cli-usage-pagination-serverside
		// vs OpenStack Swift: 10,000
		// - https://docs.openstack.org/swift/latest/api/pagination.html
		MaxPageSize int64 `json:"max_pagesize,omitempty"`

		// Multipart upload size threshold must be greater or equal 5MB
		// - https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager
		// - for the AIS default, see `DefaultPartSize` in ais/s3/const
		// - NOTE: the threshold is, effectively, one of the **performance tunables**
		MultiPartSize cos.SizeIEC `json:"multipart_size,omitempty"`
	}
	ExtraPropsAWSToSet struct {
		CloudRegion   *string      `json:"cloud_region,omitempty"`
		Endpoint      *string      `json:"endpoint,omitempty"`
		Profile       *string      `json:"profile,omitempty"`
		MaxPageSize   *int64       `json:"max_pagesize,omitempty"`
		MultiPartSize *cos.SizeIEC `json:"multipart_size,omitempty"`
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
		RateLimit   *RateLimitConfToSet   `json:"rate_limit,omitempty"`
		Features    *feat.Flags           `json:"features,string,omitempty"`
		WritePolicy *WritePolicyConfToSet `json:"write_policy,omitempty"`
		Extra       *ExtraToSet           `json:"extra,omitempty"`
		Force       bool                  `json:"force,omitempty" copy:"skip" list:"omit"`
	}

	BackendBckToSet struct {
		Name     *string `json:"name"`
		Provider *string `json:"provider"`
	}
)

//
// bucket props (Bprops)
//

// By default, created buckets inherit their properties from the cluster (global) configuration.
// Global configuration, in turn, is protected versioned, checksummed, and replicated across the entire cluster.
//
// * Bucket properties can be changed at any time via `api.SetBprops`.
// * In addition, `api.CreateBucket` allows to specify (non-default) properties at bucket creation time.
// * Inherited defaults include checksum, LRU, etc. configurations - see below.
// * By default, LRU is disabled for AIS (`ais://`) buckets.
//
// See also:
//   - github.com/NVIDIA/aistore/blob/main/docs/bucket.md#default-bucket-properties
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

	// inherit cluster defaults (w/ override via api.CreateBucket and api.SetBucketProps)
	return &Bprops{
		Cksum:       cksum,
		LRU:         lru,
		Mirror:      c.Mirror,
		Versioning:  c.Versioning,
		Access:      apc.AccessAll,
		EC:          c.EC,
		WritePolicy: wp,
		RateLimit:   c.RateLimit,
		Features:    c.Features,
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
			return fmt.Errorf("invalid provider %q: only ais:// buckets can have remote backend (%q)", bp.Provider, bp.BackendBck.String())
		}
		if bp.BackendBck.Provider == "" {
			// (compare with `ErrEmptyProvider`)
			return fmt.Errorf("backend bucket %q: provider is empty", bp.BackendBck.String())
		}
		if bp.BackendBck.Name == "" {
			return fmt.Errorf("backend bucket %q: name is empty", bp.BackendBck.String())
		}
		if !bp.BackendBck.IsRemote() {
			return fmt.Errorf("backend bucket %q must be remote", bp.BackendBck.String())
		}
	}

	// run assorted props validators
	var softErr error
	for _, pv := range []PropsValidator{&bp.Cksum, &bp.Mirror, &bp.EC, &bp.Extra, &bp.WritePolicy, &bp.RateLimit} {
		var err error
		switch {
		case pv == &bp.EC:
			err = bp.EC.ValidateAsProps(targetCnt)
		case pv == &bp.Extra:
			err = bp.Extra.ValidateAsProps(bp.Provider)
		default:
			err = pv.ValidateAsProps()
		}
		if err != nil {
			if !IsErrWarning(err) {
				return err
			}
			softErr = err
		}
	}
	if bp.Mirror.Enabled && bp.EC.Enabled {
		nlog.Warningln("n-way mirroring and EC are both enabled at the same time on the same bucket")
	}

	// not inheriting cluster-scope features
	names := bp.Features.Names()
	for _, n := range names {
		if !feat.IsBucketScope(n) {
			bp.Features = bp.Features.ClearName(n)
		}
	}
	return softErr
}

func (bp *Bprops) Apply(propsToSet *BpropsToSet) {
	err := CopyProps(propsToSet, bp, apc.Daemon)
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
	if provider == apc.HT && c.HTTP.OrigURLBck == "" {
		return errors.New("original bucket URL must be set for a bucket with HTTP provider")
	}
	if provider == apc.AWS && c.AWS.MultiPartSize != 0 && (c.AWS.MultiPartSize < 5*cos.MB || c.AWS.MultiPartSize > cos.TB) {
		return fmt.Errorf("invalid aws.multipart_size %d (expecting range 5MB to 1TB)", c.AWS.MultiPartSize)
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
	to.ObjSize.Min = min(from.ObjSize.Min, to.ObjSize.Min)
	to.ObjSize.Max = max(from.ObjSize.Max, to.ObjSize.Max)
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
		// TODO -- FIXME: (local-playground + losetup, etc.)
		if testingEnv {
			break
		}
	}
	for _, summ := range s {
		if summ.ObjCount.Present > 0 {
			summ.ObjSize.Avg = int64(cos.DivRoundU64(summ.TotalSize.PresentObjs, summ.ObjCount.Present))
		}
		if summ.ObjSize.Min == math.MaxInt64 {
			summ.ObjSize.Min = 0
		}
		if totalDisksSize > 0 {
			summ.UsedPct = cos.DivRoundU64(summ.TotalSize.OnDisk*100, totalDisksSize)
		}
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
	TCOMsg struct {
		ToBck Bck `json:"tobck"`
		apc.TCOMsg
	}
)

func (msg *ArchiveBckMsg) Cname() string { return msg.ToBck.Cname(msg.ArchName) }
