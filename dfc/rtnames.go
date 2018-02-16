// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

type rtnamemap struct {
	sync.Mutex
	m    map[string]*pendinginfo
	abrt chan struct{}
}

// in-progress file/object GETs, PUTs and DELETEs
type pendinginfo struct {
	sync.RWMutex
	time.Time
	rc  int
	fqn string
}

//
// methods
//
func (info *pendinginfo) String() string {
	return fmt.Sprintf("%s", info.fqn)
}

func newrtnamemap(size int) *rtnamemap {
	m := make(map[string]*pendinginfo, size)
	return &rtnamemap{m: m, abrt: make(chan struct{})}
}

func (rtnamemap *rtnamemap) trylockname(name string, exclusive bool, info *pendinginfo) bool {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()
	var found bool
	if _, found = rtnamemap.m[name]; found && exclusive {
		return false
	}
	if exclusive {
		rtnamemap.m[name] = info
		info.Lock() // lock the name
		return true
	}
	if !found {
		rtnamemap.m[name] = info // the 1st rlock
	}
	realinfo := rtnamemap.m[name]
	realinfo.RLock() // rlock the name
	realinfo.rc++    // upon success
	return true
}

func (rtnamemap *rtnamemap) downgradelock(name string) {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()

	info, found := rtnamemap.m[name]
	assert(found)
	info.Unlock()
	info.RLock()
	info.rc++
	assert(info.rc == 1)
}

func (rtnamemap *rtnamemap) unlockname(name string, exclusive bool) {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()
	if _, ok := rtnamemap.m[name]; !ok {
		assert(false)
	}
	info := rtnamemap.m[name]
	if exclusive {
		assert(info.rc == 0)
		info.Unlock()
		delete(rtnamemap.m, name)
		return
	}
	info.RUnlock()
	info.rc--
	if info.rc == 0 {
		delete(rtnamemap.m, name)
	}
}

func (rtnamemap *rtnamemap) lockname(name string, exclusive bool, info *pendinginfo, poll time.Duration) {
	if rtnamemap.trylockname(name, exclusive, info) {
		return
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if rtnamemap.trylockname(name, exclusive, info) {
				glog.Infof("rtnamemap: lockname %s (%v) done", info.fqn, exclusive)
				return
			}
			glog.Infof("rtnamemap: lockname %s (%v) - retrying...", info.fqn, exclusive)
		case <-rtnamemap.abrt:
			return
		}
	}
	return
}

// log pending
func (rtnamemap *rtnamemap) log() {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()
	for name, info := range rtnamemap.m {
		glog.Infof("rtnamemap: %s => %s", name, info.String())
	}
}
