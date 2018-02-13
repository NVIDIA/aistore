// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/golang/glog"
)

// Decongested Map
type dmap struct {
	sync.Mutex
	m      map[string]tostringint
	filter []uint64
	mask   uint64
	abrt   chan struct{}
}

// in-progress file/object GETs, PUTs and DELETEs
type tostringint interface {
	tostring() string
}
type pendinginfo struct {
	time.Time
	verb string
}

//
// methods
//
func (info *pendinginfo) tostring() string {
	return fmt.Sprintf("%s", info.verb)
}

func newdmap(size int) *dmap {
	n := int64(size)
	numbits := len(strconv.FormatInt(n, 2))
	mask := uint64(1 << uint(numbits))
	mask--
	filter := make([]uint64, mask+1)
	m := make(map[string]tostringint, size)
	return &dmap{m: m, filter: filter, mask: mask, abrt: make(chan struct{})}
}

func (dmap *dmap) trylockname(name string, info tostringint) (ok bool, hash uint64) {
	hash = xxhash.ChecksumString64S(name, mLCG32)
	i := hash & dmap.mask
	if !atomic.CompareAndSwapUint64(&dmap.filter[i], 0, i+1) {
		return false, 0
	}
	dmap.Lock()
	defer dmap.Unlock()
	if _, ok := dmap.m[name]; ok {
		return false, 0
	}
	dmap.m[name] = info
	return true, hash
}

func (dmap *dmap) unlockname(name string, hash uint64) {
	i := hash & dmap.mask
	if !atomic.CompareAndSwapUint64(&dmap.filter[i], i+1, 0) {
		assert(false)
	}
	dmap.Lock()
	defer dmap.Unlock()
	if _, ok := dmap.m[name]; !ok {
		assert(false)
	}
	delete(dmap.m, name)
}

func (dmap *dmap) lockname(name string, info tostringint, poll time.Duration) (hash uint64) {
	var ok bool
	if ok, hash = dmap.trylockname(name, info); ok {
		return
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ok, hash = dmap.trylockname(name, info); ok {
				return
			}
			glog.Infof("dmap: lockname %s - retrying...", name)
		case <-dmap.abrt:
			break
		}
	}
	return
}

// log pending
func (dmap *dmap) log() {
	dmap.Lock()
	defer dmap.Unlock()
	for name, info := range dmap.m {
		glog.Infof("dmap: %s => %s", name, info.tostring())
	}
}
