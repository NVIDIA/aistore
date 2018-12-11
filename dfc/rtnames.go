// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	pollInterval = time.Second
)

// Implements cluster.NameLocker interface.
// Locks objects stored in the cluster when there's a pending GET, PUT, etc. transaction
// and we don't want the object in question to be updated or evicted concurrently.
// Objects in the cluster are locked by their unique names aka unames.
// The lock can be exclusive (write) or shared (read).

var _ cluster.NameLocker = &rtnamemap{}

type rtnamemap struct {
	mu *sync.Mutex
	m  map[string]*pendinginfo
}

type pendinginfo struct {
	time.Time
	rc        int
	exclusive bool
}

//
// methods
//
func newrtnamemap(size int) *rtnamemap {
	m := make(map[string]*pendinginfo, size)
	return &rtnamemap{mu: &sync.Mutex{}, m: m}
}

func (rtnamemap *rtnamemap) TryLock(uname string, exclusive bool) bool {
	rtnamemap.mu.Lock()

	realinfo, found := rtnamemap.m[uname]
	if found && exclusive {
		rtnamemap.mu.Unlock()
		return false
	}
	info := &pendinginfo{Time: time.Now()}
	if exclusive {
		rtnamemap.m[uname] = info
		info.exclusive = true
		info.rc = 0
		rtnamemap.mu.Unlock()
		return true
	}
	// rlock
	if found {
		if realinfo.exclusive {
			rtnamemap.mu.Unlock()
			return false
		}
	} else {
		rtnamemap.m[uname] = info // the 1st rlock
		realinfo = info
		realinfo.exclusive = false
		realinfo.rc = 0
	}
	realinfo.rc++
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
			glog.Infof("Lock %s(%t) - success", uname, exclusive)
			return
		}
		glog.Infof("Lock %s(%t) - retrying...", uname, exclusive)
	}
}

func (rtnamemap *rtnamemap) DowngradeLock(uname string) {
	rtnamemap.mu.Lock()

	info, found := rtnamemap.m[uname]
	cmn.Assert(found)
	info.exclusive = false
	info.rc++
	cmn.Assert(info.rc == 1)
	rtnamemap.mu.Unlock()
}

func (rtnamemap *rtnamemap) Unlock(uname string, exclusive bool) {
	rtnamemap.mu.Lock()
	info, ok := rtnamemap.m[uname]
	cmn.Assert(ok)
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
