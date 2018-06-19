// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	RWPolicyCloud    = "cloud"
	RWPolicyNextTier = "next_tier"
)

type BucketProps struct {
	CloudProvider string `json:"cloud_provider,omitempty"`
	NextTierURL   string `json:"next_tier_url,omitempty"`
	ReadPolicy    string `json:"read_policy,omitempty"`
	WritePolicy   string `json:"write_policy,omitempty"`
}

type bucketMD struct {
	LBmap   map[string]BucketProps `json:"l_bmap"` // local cache-only buckets and their props
	CBmap   map[string]BucketProps `json:"c_bmap"` // Cloud-based buckets and their DFC-only metadata
	Version int64                  `json:"version"`
}

type bmdowner struct {
	sync.Mutex
	bucketmd unsafe.Pointer
}

func (r *bmdowner) put(bucketmd *bucketMD) {
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
		assert(false)
	}

	m.Version++
	mm[b] = p
}

func (m *bucketMD) islocal(bucket string) bool {
	_, ok := m.LBmap[bucket]
	return ok
}

func (m *bucketMD) cloneU() *bucketMD {
	dst := &bucketMD{}
	m.deepcopy(dst)
	return dst
}

func (m *bucketMD) deepcopy(dst *bucketMD) {
	copyStruct(dst, m)
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

func (m *bucketMD) cloneL() (clone interface{}) { // FIXME: remove from revs
	clone = m.cloneU()
	return
}

func (m *bucketMD) marshal() ([]byte, error) {
	return json.Marshal(m)
}
