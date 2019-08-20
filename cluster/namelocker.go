// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

// NameLocker interface locks and unlocks (and try-locks, etc.)
// arbitrary strings.
// NameLocker is currently utilized to lock objects stored in the cluster
// when there's a pending GET, PUT, etc. transaction and we don't want
// the object in question to get updated or evicted concurrently.
// Objects are locked by their unique (string) names aka unames.
// The lock can be exclusive (write) or shared (read).
type (
	NameLocker interface {
		TryLock(uname string, exclusive bool) bool
		Lock(uname string, exclusive bool)
		DowngradeLock(uname string)
		Unlock(uname string, exclusive bool)
	}

	lockInfo struct {
		rc        int
		exclusive bool
	}

	rtNameMap struct {
		mu sync.Mutex
		m  map[string]*lockInfo
	}
)

const (
	pollInterval = time.Millisecond * 100
	initCapacity = 128
)

var (
	ObjectLocker NameLocker
)

//
// methods
//
func newRTNameMap() *rtNameMap {
	return &rtNameMap{
		m: make(map[string]*lockInfo, initCapacity),
	}
}

func (rt *rtNameMap) TryLock(uname string, exclusive bool) bool {
	rt.mu.Lock()

	realInfo, found := rt.m[uname]
	info := &lockInfo{}
	if exclusive {
		if found {
			rt.mu.Unlock()
			return false
		}

		rt.m[uname] = info
		info.exclusive = true
		rt.mu.Unlock()
		return true
	}

	// rlock
	if found {
		if realInfo.exclusive {
			rt.mu.Unlock()
			return false
		}
	} else {
		rt.m[uname] = info // the 1st rlock
		realInfo = info
	}
	realInfo.rc++
	rt.mu.Unlock()
	return true
}

// NOTE: Lock() stays in the loop for as long as needed to
// acquire the lock.
// The implementation is intentionally simple as we currently
// don't need cancellation (via context.Context), timeout,
// increasing/random polling intervals, sync.Cond -based wait
// for Unlock - none of the above.
// Use TryLock() to support any such semantics that makes sense.

func (rt *rtNameMap) Lock(uname string, exclusive bool) {
	if rt.TryLock(uname, exclusive) {
		return
	}
	for {
		time.Sleep(pollInterval)
		if rt.TryLock(uname, exclusive) {
			if glog.FastV(4, glog.SmoduleCluster) {
				glog.Infof("Lock %s(%t) - success", uname, exclusive)
			}
			return
		}
		if glog.FastV(4, glog.SmoduleCluster) {
			glog.Infof("Lock %s(%t) - retrying...", uname, exclusive)
		}
	}
}

func (rt *rtNameMap) DowngradeLock(uname string) {
	rt.mu.Lock()
	info, found := rt.m[uname]
	cmn.Assert(found && info.exclusive)
	info.exclusive = false
	info.rc++
	cmn.Assert(info.rc == 1)
	rt.mu.Unlock()
}

func (rt *rtNameMap) Unlock(uname string, exclusive bool) {
	rt.mu.Lock()
	info, found := rt.m[uname]
	cmn.Assert(found)
	if exclusive {
		cmn.Assert(info.exclusive)
		delete(rt.m, uname)
		rt.mu.Unlock()
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(rt.m, uname)
	}
	rt.mu.Unlock()
}
