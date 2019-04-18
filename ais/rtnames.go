// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	pollInterval = time.Millisecond * 100
	initCapacity = 128
)

// Implements cluster.NameLocker interface.
// Locks objects stored in the cluster when there's a pending GET, PUT, etc. transaction
// and we don't want the object in question to be updated or evicted concurrently.
// Objects in the cluster are locked by their unique names aka unames.
// The lock can be exclusive (write) or shared (read).

var _ cluster.NameLocker = &rtnamemap{}

type rtnamemap struct {
	mu sync.Mutex
	m  map[string]*lockInfo
}

type lockInfo struct {
	rc        int
	exclusive bool
}

//
// methods
//
func newrtnamemap() *rtnamemap {
	return &rtnamemap{
		m: make(map[string]*lockInfo, initCapacity),
	}
}

func (rtnamemap *rtnamemap) TryLock(uname string, exclusive bool) bool {
	rtnamemap.mu.Lock()

	realInfo, found := rtnamemap.m[uname]
	info := &lockInfo{}
	if exclusive {
		if found {
			rtnamemap.mu.Unlock()
			return false
		}

		rtnamemap.m[uname] = info
		info.exclusive = true
		rtnamemap.mu.Unlock()
		return true
	}

	// rlock
	if found {
		if realInfo.exclusive {
			rtnamemap.mu.Unlock()
			return false
		}
	} else {
		rtnamemap.m[uname] = info // the 1st rlock
		realInfo = info
	}
	realInfo.rc++
	rtnamemap.mu.Unlock()
	return true
}

// NOTE that Lock() stays in the loop for as long as needed to
// acquire the lock.
// The implementation is intentionally simple as we currently
// don't need cancelation (via context.Context), timeout,
// increasing/random polling intervals, sync.Cond -based wait
// for Unlock - none of the above.
// Use TryLock() to support any such semantics that makes sense.

func (rtnamemap *rtnamemap) Lock(uname string, exclusive bool) {
	if rtnamemap.TryLock(uname, exclusive) {
		return
	}
	for {
		time.Sleep(pollInterval)
		if rtnamemap.TryLock(uname, exclusive) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Lock %s(%t) - success", uname, exclusive)
			}
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Lock %s(%t) - retrying...", uname, exclusive)
		}
	}
}

func (rtnamemap *rtnamemap) DowngradeLock(uname string) {
	rtnamemap.mu.Lock()
	info, found := rtnamemap.m[uname]
	cmn.Assert(found && info.exclusive)
	info.exclusive = false
	info.rc++
	cmn.Assert(info.rc == 1)
	rtnamemap.mu.Unlock()
}

func (rtnamemap *rtnamemap) Unlock(uname string, exclusive bool) {
	rtnamemap.mu.Lock()
	info, found := rtnamemap.m[uname]
	cmn.Assert(found)
	if exclusive {
		cmn.Assert(info.exclusive)
		delete(rtnamemap.m, uname)
		rtnamemap.mu.Unlock()
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(rtnamemap.m, uname)
	}
	rtnamemap.mu.Unlock()
}
