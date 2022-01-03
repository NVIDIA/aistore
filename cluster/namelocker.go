// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
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
		wcond     *sync.Cond // to synchronize "waiting room" upgrade logic
		rc        int32      // read-lock refcount
		waiting   int32      // waiting room count
		exclusive bool       // write-lock
		upgraded  bool       // indication for the waiters that upgrade's done
	}
)

const (
	initPollInterval = 10 * time.Microsecond
	maxPollInterval  = 100 * time.Millisecond
	initCapacity     = 128
)

////////////////
// namelocker //
////////////////

func (nl nameLocker) init() {
	for idx := 0; idx < len(nl); idx++ {
		nl[idx].init()
	}
}

func (li *lockInfo) notifyWaiters() {
	if li.wcond == nil || li.waiting == 0 {
		return
	}
	debug.Assert(li.rc >= li.waiting)
	if li.upgraded {
		// has been upgraded - wake up all waiters
		li.wcond.Broadcast()
	} else {
		// wake up only the "doer"
		li.wcond.Signal()
	}
}

func (li *lockInfo) decWaiting() {
	li.waiting--
	if li.waiting == 0 {
		li.wcond = nil
	}
}

/////////
// nlc //
/////////

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
	return int(lockInfo.rc), lockInfo.exclusive
}

func (nlc *nlc) TryLock(uname string, exclusive bool) (locked bool) {
	nlc.mu.Lock()
	locked = nlc.try(uname, exclusive)
	nlc.mu.Unlock()
	return
}

func (nlc *nlc) try(uname string, exclusive bool) bool {
	li, found := nlc.m[uname]
	// wlock
	if exclusive {
		if found {
			return false
		}
		nlc.m[uname] = &lockInfo{exclusive: true}
		return true
	}
	// rlock
	if !found {
		nlc.m[uname] = &lockInfo{rc: 1}
		return true
	}
	if li.exclusive {
		return false
	}
	// can't rlock if there's someone trying to upgrade
	if li.waiting > 0 {
		return false
	}
	li.rc++
	return true
}

// NOTE: Lock() stays in the loop for as long as needed to acquire the lock.
//
// The implementation is intentionally simple as we currently don't need
// cancellation (e.g., via context.Context), timeout, etc. semantics
func (nlc *nlc) Lock(uname string, exclusive bool) {
	if nlc.TryLock(uname, exclusive) {
		return
	}
	sleep := initPollInterval
	for {
		time.Sleep(sleep)
		if nlc.TryLock(uname, exclusive) {
			return
		}
		sleep = cos.MinDuration(sleep*2, maxPollInterval)
	}
}

// NOTE: `UpgradeLock` correctly synchronizes threads waiting for **the same** operation
//       (e.g., get object from a remote backend)
func (nlc *nlc) UpgradeLock(uname string) (upgraded bool) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found && !li.exclusive && li.rc > 0)
	if li.rc == 1 {
		li.rc = 0
		li.exclusive = true
		nlc.mu.Unlock()
		return false
	}
	if li.wcond == nil {
		li.wcond = sync.NewCond(&nlc.mu)
	}
	li.waiting++
	// Wait here until all readers get in line
	for li.rc != li.waiting {
		li.wcond.Wait()

		// Has been upgraded by smbd. else
		if li.upgraded {
			li.decWaiting()
			nlc.mu.Unlock()
			return true
		}
	}
	// Upgrading
	li.upgraded = true
	li.rc--
	li.decWaiting()
	li.exclusive = true
	nlc.mu.Unlock()
	return false
}

func (nlc *nlc) DowngradeLock(uname string) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found && li.exclusive)
	li.rc++
	li.exclusive = false
	li.notifyWaiters()
	nlc.mu.Unlock()
}

func (nlc *nlc) Unlock(uname string, exclusive bool) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found)
	if exclusive {
		debug.Assert(li.exclusive)
		if li.waiting > 0 {
			li.exclusive = false
			li.notifyWaiters()
		} else {
			delete(nlc.m, uname)
		}
		nlc.mu.Unlock()
		return
	}
	li.rc--
	if li.rc == 0 {
		delete(nlc.m, uname)
	}
	li.notifyWaiters()
	nlc.mu.Unlock()
}
