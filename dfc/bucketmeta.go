// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
)

const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

type BucketProps struct {
	CloudProvider string      `json:"cloud_provider,omitempty"`
	NextTierURL   string      `json:"next_tier_url,omitempty"`
	ReadPolicy    string      `json:"read_policy,omitempty"`
	WritePolicy   string      `json:"write_policy,omitempty"`
	CksumConf     cksumconfig `json:"cksum_config"`
	LRUProps      lruconfig   `json:"lru_props"`
}

type bucketMD struct {
	LBmap   map[string]BucketProps `json:"l_bmap"`  // local cache-only buckets and their props
	CBmap   map[string]BucketProps `json:"c_bmap"`  // Cloud-based buckets and their DFC-only metadata
	Version int64                  `json:"version"` // version - gets incremented on every update
	vstr    string                 // itoa(Version), to have it handy for http redirects
}

type bmdowner struct {
	sync.Mutex
	bucketmd unsafe.Pointer
}

func (r *bmdowner) put(bucketmd *bucketMD) {
	bucketmd.vstr = strconv.FormatInt(bucketmd.Version, 10)
	atomic.StorePointer(&r.bucketmd, unsafe.Pointer(bucketmd))
}

// the intended and implied usage of this inconspicuous method is CoW:
// - read (shared/replicated bucket-metadata object) freely
// - clone for writing
// - and never-ever modify in place
func (r *bmdowner) get() (bucketmd *bucketMD) {
	bucketmd = (*bucketMD)(atomic.LoadPointer(&r.bucketmd))
	return
}

func NewBucketProps() *BucketProps {
	return &BucketProps{
		CksumConf: cksumconfig{
			Checksum: api.ChecksumInherit,
		},
		LRUProps: ctx.config.LRU,
	}
}

func newBucketMD() *bucketMD {
	return &bucketMD{
		LBmap: make(map[string]BucketProps),
		CBmap: make(map[string]BucketProps),
	}
}

func (m *bucketMD) add(b string, local bool, p BucketProps) bool {
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

func (m *bucketMD) get(b string, local bool) (bool, BucketProps) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	p, ok := mm[b]
	return ok, p
}

func (m *bucketMD) set(b string, local bool, p BucketProps) {
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

func (m *bucketMD) propsAndChecksum(bucket string) (p BucketProps, checksum string, defined bool) {
	var ok bool
	ok, p = m.get(bucket, m.islocal(bucket))
	if !ok || p.CksumConf.Checksum == api.ChecksumInherit {
		return p, "", false
	}
	return p, p.CksumConf.Checksum, true
}

// lruEnabled returns whether or not LRU is enabled
// for the bucket. Returns the global setting if bucket not found
func (m *bucketMD) lruEnabled(bucket string) bool {
	ok, p := m.get(bucket, m.islocal(bucket))
	if !ok {
		return ctx.config.LRU.LRUEnabled
	}
	return p.LRUProps.LRUEnabled
}

func (m *bucketMD) clone() *bucketMD {
	dst := &bucketMD{}
	m.deepcopy(dst)
	return dst
}

func (m *bucketMD) deepcopy(dst *bucketMD) {
	common.CopyStruct(dst, m)
	dst.LBmap = make(map[string]BucketProps, len(m.LBmap))
	dst.CBmap = make(map[string]BucketProps, len(m.CBmap))
	inmaps := [2]map[string]BucketProps{m.LBmap, m.CBmap}
	outmaps := [2]map[string]BucketProps{dst.LBmap, dst.CBmap}
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
