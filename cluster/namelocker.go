// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

// nameLocker is a 2-level structure utilized to lock objects of *any* kind
// as long as object has a (string) name and an (int) digest.
// In most cases, the digest will be some sort a hash of the name itself
// (does not necessarily need to be cryptographic).
// The lock can be exclusive (write) or shared (read).
type (
	nameLocker []nlc
	nlc        struct {
		mu sync.Mutex
		m  map[string]*lockInfo
	}
	lockInfo struct {
		rc        int
		exclusive bool
	}
)

const (
	initPollInterval = 10 * time.Microsecond
	maxPollInterval  = 100 * time.Millisecond
	initCapacity     = 128
)

//
// namelocker
//

func (nl nameLocker) init() {
	for idx := 0; idx < len(nl); idx++ {
		nlc := &nl[idx]
		nlc.m = make(map[string]*lockInfo, initCapacity)
	}
}

//
// nlc
//

func (nlc *nlc) TryLock(uname string, exclusive bool) bool {
	nlc.mu.Lock()

	realInfo, found := nlc.m[uname]
	info := &lockInfo{}
	if exclusive {
		if found {
			nlc.mu.Unlock()
			return false
		}

		nlc.m[uname] = info
		info.exclusive = true
		nlc.mu.Unlock()
		return true
	}

	// rlock
	if found {
		if realInfo.exclusive {
			nlc.mu.Unlock()
			return false
		}
	} else {
		nlc.m[uname] = info // the 1st rlock
		realInfo = info
	}
	realInfo.rc++
	nlc.mu.Unlock()
	return true
}

// NOTE: Lock() stays in the loop for as long as needed to acquire the lock.
//
// The implementation is intentionally simple as we currently don't need
// cancellation (via context.Context), timeout, sync.Cond.
func (nlc *nlc) Lock(uname string, exclusive bool) {
	if nlc.TryLock(uname, exclusive) {
		return
	}
	sleep := initPollInterval
	for {
		time.Sleep(sleep)
		if nlc.TryLock(uname, exclusive) {
			if glog.FastV(4, glog.SmoduleCluster) {
				glog.Infof("Lock %s(%t) - success", uname, exclusive)
			}
			return
		}
		if glog.FastV(4, glog.SmoduleCluster) {
			glog.Infof("Lock %s(%t) - retrying...", uname, exclusive)
		}

		sleep *= 2
		if sleep > maxPollInterval {
			sleep = maxPollInterval
		}
	}
}

func (nlc *nlc) DowngradeLock(uname string) {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	cmn.Assert(found && info.exclusive)
	info.exclusive = false
	info.rc++
	cmn.Assert(info.rc == 1)
	nlc.mu.Unlock()
}

func (nlc *nlc) TryUpgradeLock(uname string) bool {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	cmn.Assert(found && !info.exclusive && info.rc > 0)
	if info.rc == 1 {
		info.exclusive = true
		info.rc = 0
		nlc.mu.Unlock()
		return true
	}
	nlc.mu.Unlock()
	return false
}

func (nlc *nlc) Unlock(uname string, exclusive bool) {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	cmn.Assert(found)
	if exclusive {
		cmn.Assert(info.exclusive)
		delete(nlc.m, uname)
		nlc.mu.Unlock()
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(nlc.m, uname)
	}
	nlc.mu.Unlock()
}
