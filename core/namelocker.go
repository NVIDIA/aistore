// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/OneOfOne/xxhash"
)

const nlpTryDefault = time.Second // nlp.TryLock default duration

// nameLocker is a 2-level structure utilized to lock objects of *any* kind,
// as long as the object in question has a (string) name and an (int) digest.
// Digest (plus a certain fixed mask) is used to select one of the specific `nlc` maps;
// in most cases, digest will be some sort a hash of the name itself and does not
// need to be cryptographic.
// The lock can be exclusive (write) or shared (read).

type (
	nameLocker []nlc
	nlc        struct {
		m  map[string]*lockInfo
		mu sync.Mutex
	}
	lockInfo struct {
		wcond     *sync.Cond // to synchronize "waiting room" upgrade logic
		rc        int32      // read-lock refcount
		waiting   int32      // waiting room count
		exclusive bool       // write-lock
		upgraded  bool       // indication for the waiters that upgrade's done
	}
)

// pair
type (
	NLP interface {
		Lock()
		TryLock(timeout time.Duration) bool
		TryRLock(timeout time.Duration) bool
		Unlock()
	}

	noCopy struct{}
	nlp    struct {
		_         noCopy
		nlc       *nlc
		uname     string
		exclusive bool
	}
)

const (
	initPollInterval = 10 * time.Microsecond
	maxPollInterval  = 100 * time.Millisecond
	initCapacity     = 32
)

////////////////
// namelocker //
////////////////

func newNameLocker() (nl nameLocker) {
	nl = make(nameLocker, cos.MultiSyncMapCount)
	for idx := 0; idx < len(nl); idx++ {
		nl[idx].init()
	}
	return
}

//////////////
// lockInfo //
//////////////

func (li *lockInfo) notify() {
	if li.wcond == nil || li.waiting == 0 {
		return
	}
	debug.Assert(li.rc >= li.waiting)
	if li.upgraded {
		// has been upgraded - wake up all waiters
		li.wcond.Broadcast()
	} else {
		// wake up only the owner
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
		sleep = min(sleep*2, maxPollInterval)
	}
}

// upgrade rlock -> wlock
// e.g. usage: simultaneous cold GET
// returns true if exclusively locked by _another_ thread
func (nlc *nlc) UpgradeLock(uname string) bool {
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
	li.notify()
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
			li.notify()
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
	li.notify()
	nlc.mu.Unlock()
}

/////////
// nlp //
/////////

// interface guard
var _ NLP = (*nlp)(nil)

// NOTE: currently, is only used to lock buckets
func NewNLP(name string) NLP {
	var (
		nlp  = &nlp{uname: name}
		hash = xxhash.Checksum64S(cos.UnsafeB(name), cos.MLCG32)
		idx  = int(hash & cos.MultiSyncMapMask)
	)
	nlp.nlc = &bckLocker[idx] // NOTE: bckLocker
	return nlp
}

func (nlp *nlp) Lock() {
	nlp.nlc.Lock(nlp.uname, true)
	nlp.exclusive = true
}

func (nlp *nlp) TryLock(timeout time.Duration) (ok bool) {
	if timeout == 0 {
		timeout = nlpTryDefault
	}
	ok = nlp.withRetry(timeout, true)
	nlp.exclusive = ok
	return
}

// NOTE: ensure single-time usage (no ref counting!)
func (nlp *nlp) TryRLock(timeout time.Duration) (ok bool) {
	if timeout == 0 {
		timeout = nlpTryDefault
	}
	ok = nlp.withRetry(timeout, false)
	debug.Assert(!nlp.exclusive)
	return
}

func (nlp *nlp) Unlock() {
	nlp.nlc.Unlock(nlp.uname, nlp.exclusive)
}

func (nlp *nlp) withRetry(d time.Duration, exclusive bool) bool {
	if nlp.nlc.TryLock(nlp.uname, exclusive) {
		return true
	}
	i := d / 10
	for j := i; j < d; j += i {
		time.Sleep(i)
		if nlp.nlc.TryLock(nlp.uname, exclusive) {
			return true
		}
	}
	return false
}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
