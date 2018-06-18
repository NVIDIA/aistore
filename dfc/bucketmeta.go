// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"sync"
)

type bucketMD struct {
	sync.Mutex
	LBmap   map[string]simplekvs `json:"l_bmap"` // local cache-only buckets and their props
	CBmap   map[string]simplekvs `json:"c_bmap"` // Cloud-based buckets and their DFC-only metadata
	Version int64                `json:"version"`
}

func newBucketMD() *bucketMD {
	return &bucketMD{
		LBmap: make(map[string]simplekvs),
		CBmap: make(map[string]simplekvs),
	}
}

func (m *bucketMD) add(b string, local bool, props ...simplekvs) bool {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; ok {
		return false
	}
	if len(props) > 0 {
		mm[b] = props[0]
	} else {
		mm[b] = nil
	}
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

func (m *bucketMD) get(b string, local bool) (exists bool, props simplekvs) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	props, exists = mm[b]
	return
}

func (m *bucketMD) set(b string, local bool, props simplekvs) {
	mm := m.LBmap
	if !local {
		mm = m.CBmap
	}
	if _, ok := mm[b]; !ok {
		assert(false)
	}

	m.Version++
	// revert it to nil when there are no values or all values are empty
	havevalues := false
	for _, v := range props {
		if v != "" {
			havevalues = true
			break
		}
	}
	if havevalues {
		mm[b] = props
	} else {
		mm[b] = nil
	}
}

func (m *bucketMD) islocal(bucket string) bool {
	_, ok := m.LBmap[bucket]
	return ok
}

func (m *bucketMD) versionL() (v int64) {
	bucketMetaLock.Lock()
	v = m.Version
	bucketMetaLock.Unlock()
	return
}

func (m *bucketMD) cloneU() *bucketMD {
	dst := &bucketMD{}
	m.deepcopy(dst)
	return dst
}

func (m *bucketMD) copyL(dst *bucketMD) {
	bucketMetaLock.Lock()
	m.deepcopy(dst)
	bucketMetaLock.Unlock()
}

func (m *bucketMD) deepcopy(dst *bucketMD) {
	copyStruct(dst, m)
	dst.LBmap = make(map[string]simplekvs, len(m.LBmap))
	dst.CBmap = make(map[string]simplekvs, len(m.CBmap))
	inmaps := [2]map[string]simplekvs{m.LBmap, m.CBmap}
	outmaps := [2]map[string]simplekvs{dst.LBmap, dst.CBmap}
	for i := 0; i < len(inmaps); i++ {
		mm := outmaps[i]
		for name, props := range inmaps[i] {
			if props == nil {
				mm[name] = nil
				continue
			}
			propscopy := make(simplekvs, len(props))
			for pn, pval := range props {
				propscopy[pn] = pval
			}
			mm[name] = propscopy
		}
	}
}

//
// revs interface
//
func (m *bucketMD) tag() string    { return bucketmdtag }
func (m *bucketMD) version() int64 { return m.Version }

func (m *bucketMD) cloneL() (clone interface{}) {
	bucketMetaLock.Lock()
	clone = m.cloneU()
	bucketMetaLock.Unlock()
	return
}

func (m *bucketMD) marshal() ([]byte, error) {
	return json.Marshal(m)
}
