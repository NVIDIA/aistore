// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		room      *waitingRoom
		rc        int
		exclusive bool
	}
	waitingRoom struct {
		cond     *sync.Cond
		waiting  int  // Number of threads waiting in the room.
		finished bool // Determines if the action for the waiting room has been finished.
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
		nl[idx].init()
	}
}

func (li *lockInfo) notifyWaiters() {
	if li.room == nil || li.room.waiting == 0 {
		return
	}
	debug.Assert(li.rc >= li.room.waiting)
	if li.room.finished {
		// Job has been finished so we can wake up everybody else waiting.
		li.room.cond.Broadcast()
	} else {
		// Wake up only one "job doer".
		li.room.cond.Signal()
	}
}

func (li *lockInfo) decWaiting() {
	li.room.waiting--
	if li.room.waiting == 0 {
		li.room = nil // Clean room info.
	}
}

//
// nlc
//

func (nlc *nlc) init() {
	nlc.m = make(map[string]*lockInfo, initCapacity)
}

func (nlc *nlc) IsLocked(uname string) (rc int, exclusive bool) {
	nlc.mu.Lock()
	defer nlc.mu.Unlock()

	lockInfo, found := nlc.m[uname]
	if !found {
		return
	}
	return lockInfo.rc, lockInfo.exclusive
}

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
		// If there is someone waiting for the upgrade we cannot take a read lock
		// to not starve it - the waiter has worse than us waiting for the exclusive.
		if realInfo.room != nil && realInfo.room.waiting > 0 {
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

// NOTE: `UpgradeLock` correctly synchronizes the threads which are waiting
// for **the same** action. Otherwise, there is still potential risk that one
// action will do something unexpected during `UpgradeLock` before next action
// which already checked conditions and is also waiting for the `UpgradeLock`.
func (nlc *nlc) UpgradeLock(uname string) (finished bool) {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	debug.Assert(found && !info.exclusive && info.rc > 0)
	if info.rc == 1 {
		info.rc = 0
		info.exclusive = true
		nlc.mu.Unlock()
		return false
	}
	if info.room == nil {
		info.room = &waitingRoom{cond: sync.NewCond(&nlc.mu)}
	}
	info.room.waiting++
	// Wait until all the readers are the waiting ones.
	for info.rc != info.room.waiting {
		info.room.cond.Wait()

		// We have been awakened and the room is finished - remove us from waiting.
		if info.room.finished {
			info.decWaiting()
			nlc.mu.Unlock()
			return true
		}
	}
	// Already mark it finished but the other threads are asleep so they don't know.
	info.room.finished = true
	info.rc--
	info.decWaiting()
	info.exclusive = true
	nlc.mu.Unlock()
	return false
}

func (nlc *nlc) DowngradeLock(uname string) {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	debug.Assert(found && info.exclusive)
	info.rc++
	info.exclusive = false
	info.notifyWaiters()
	nlc.mu.Unlock()
}

func (nlc *nlc) Unlock(uname string, exclusive bool) {
	nlc.mu.Lock()
	info, found := nlc.m[uname]
	debug.Assert(found)
	if exclusive {
		debug.Assert(info.exclusive)
		if info.room != nil && info.room.waiting > 0 {
			info.exclusive = false
			info.notifyWaiters()
		} else {
			delete(nlc.m, uname)
		}
		nlc.mu.Unlock()
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(nlc.m, uname)
	}
	info.notifyWaiters()
	nlc.mu.Unlock()
}
