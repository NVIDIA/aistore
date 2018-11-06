/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/common"
)

// NOTE: to access bucket metadata and related structures, external
//       packages and HTTP clients must import dfcpub/cluster (and not dfc)

// - bucketMD is a server-side extension of the cluster.BMD
// - bucketMD represents buckets (that store objects) and associated metadata
// - bucketMD (instance) can be obtained via bmdowner.get()
// - bucketMD is immutable and versioned
// - bucketMD versioning is monotonic and incremental
//
// - bucketMD typical update transaction:
// lock -- clone() -- modify the clone -- bmdowner.put(clone) -- unlock
//
// (*) for merges and conflict resolution, check the current version prior to put()
//     (note that version check must be protected by the same critical section)
//
type bucketMD struct {
	cluster.BMD
	vstr string // itoa(Version), to have it handy for http redirects
}

// implements cluster.Bowner interface
type bmdowner struct {
	sync.Mutex
	bucketmd unsafe.Pointer
}

func (r *bmdowner) put(bucketmd *bucketMD) {
	bucketmd.vstr = strconv.FormatInt(bucketmd.Version, 10)
	atomic.StorePointer(&r.bucketmd, unsafe.Pointer(bucketmd))
}

// implements cluster.Bowner.Get
func (r *bmdowner) Get() *cluster.BMD {
	bucketmd := (*bucketMD)(atomic.LoadPointer(&r.bucketmd))
	return &bucketmd.BMD
}
func (r *bmdowner) get() (bucketmd *bucketMD) {
	bucketmd = (*bucketMD)(atomic.LoadPointer(&r.bucketmd))
	return
}

func NewBucketProps() *api.BucketProps {
	return &api.BucketProps{
		CksumConfig: api.CksumConfig{
			Checksum: api.ChecksumInherit,
		},
		LRUConfig: ctx.config.LRU,
	}
}

func newBucketMD() *bucketMD {
	lbmap := make(map[string]api.BucketProps)
	cbmap := make(map[string]api.BucketProps)
	return &bucketMD{cluster.BMD{LBmap: lbmap, CBmap: cbmap}, ""}
}

func (m *bucketMD) add(b string, local bool, p api.BucketProps) bool {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; ok {
		return false
	}
	mm[b] = p
	m.Version++
	return true
}

func (m *bucketMD) del(b string, local bool) bool {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; !ok {
		return false
	}
	delete(mm, b)
	m.Version++
	return true
}

func (m *bucketMD) get(b string, local bool) (bool, api.BucketProps) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	p, ok := mm[b]
	return ok, p
}

func (m *bucketMD) set(b string, local bool, p api.BucketProps) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; !ok {
		common.Assert(false)
	}

	m.Version++
	mm[b] = p
}

func (m *bucketMD) islocal(bucket string) bool {
	_, ok := m.LBmap[bucket]
	return ok
}

func (m *bucketMD) propsAndChecksum(bucket string) (p api.BucketProps, checksum string, defined bool) {
	var ok bool
	ok, p = m.get(bucket, m.islocal(bucket))
	if !ok || p.Checksum == api.ChecksumInherit {
		return p, "", false
	}
	return p, p.Checksum, true
}

// lruEnabled returns whether or not LRU is enabled
// for the bucket. Returns the global setting if bucket not found
func (m *bucketMD) lruEnabled(bucket string) bool {
	ok, p := m.get(bucket, m.islocal(bucket))
	if !ok {
		return ctx.config.LRU.LRUEnabled
	}
	return p.LRUEnabled
}

func (m *bucketMD) clone() *bucketMD {
	dst := &bucketMD{}
	m.deepcopy(dst)
	return dst
}

func (m *bucketMD) deepcopy(dst *bucketMD) {
	common.CopyStruct(dst, m)
	dst.LBmap = make(map[string]api.BucketProps, len(m.LBmap))
	dst.CBmap = make(map[string]api.BucketProps, len(m.CBmap))
	inmaps := [2]map[string]api.BucketProps{m.LBmap, m.CBmap}
	outmaps := [2]map[string]api.BucketProps{dst.LBmap, dst.CBmap}
	for i := 0; i < len(inmaps); i++ {
		mm := outmaps[i]
		for name, props := range inmaps[i] {
			mm[name] = props
		}
	}
}

//
// revs interface
//
func (m *bucketMD) tag() string    { return bucketmdtag }
func (m *bucketMD) version() int64 { return m.Version }

func (m *bucketMD) marshal() ([]byte, error) {
	return jsonCompat.Marshal(m) // jsoniter + sorting
}
