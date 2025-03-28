// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	onexxh "github.com/OneOfOne/xxhash"
)

const nlpTryDefault = time.Second // nlp.TryLock default duration

// nameLocker is a 2-level structure utilized to lock objects of *any* kind,
// as long as the object in question has a (string) name and an (int) digest.
// Digest (plus a certain fixed mask) is used to select one of the specific `nlc` maps;
// in most cases, digest will be some sort a hash of the name itself and does not
// need to be cryptographic.
// The lock can be exclusive (write) or shared (read).

type (
	nameLocker [cos.MultiHashMapCount]nlc
	nlc        struct {
		m  map[string]*lockInfo
		mu sync.Mutex
	}
	lockInfo struct {
		rc        int32 // read-lock refcount
		exclusive bool  // write-lock
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
	for idx := range nl {
		nl[idx].init()
	}
	return
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

// lone reader: upgrade rlock -> wlock
// otherwise:   fail
func (nlc *nlc) UpgradeLock(uname string) (wlocked bool) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found && !li.exclusive && li.rc > 0,
		"found ", found, " li.exclusive ", li.exclusive, " li.rc ", li.rc)
	if li.rc == 1 {
		li.rc = 0
		li.exclusive = true
		wlocked = true
	}
	nlc.mu.Unlock()
	return wlocked
}

func (nlc *nlc) DowngradeLock(uname string) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found && li.exclusive, "found ", found, " li.exclusive ", li.exclusive, " li.rc ", li.rc)
	li.rc++
	li.exclusive = false
	nlc.mu.Unlock()
}

func (nlc *nlc) Unlock(uname string, exclusive bool) {
	nlc.mu.Lock()
	li, found := nlc.m[uname]
	debug.Assert(found)

	if exclusive {
		debug.Assert(li.exclusive)
		delete(nlc.m, uname)
		nlc.mu.Unlock()
		return
	}

	li.rc--
	if li.rc == 0 {
		delete(nlc.m, uname)
	}
	nlc.mu.Unlock()
}

/////////
// nlp //
/////////

// interface guard
var _ NLP = (*nlp)(nil)

// NOTE: currently, is only used to lock buckets
func NewNLP(name []byte) NLP {
	var (
		nlp  = &nlp{uname: cos.UnsafeS(name)}
		hash = onexxh.Checksum64S(name, cos.MLCG32)
		idx  = int(hash & cos.MultiHashMapMask)
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
