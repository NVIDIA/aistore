// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"math/rand"
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
	time.Time
	fqn       string
	rc        int
	exclusive bool
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

	_, found := rtnamemap.m[name]
	if found && exclusive {
		return false
	}
	if exclusive {
		rtnamemap.m[name] = info
		info.exclusive = true
		info.rc = 0
		return true
	}
	// rlock
	var realinfo *pendinginfo
	if found {
		realinfo = rtnamemap.m[name]
		if realinfo.exclusive {
			return false
		}
	} else {
		rtnamemap.m[name] = info // the 1st rlock
		realinfo = info
		realinfo.exclusive = false
		realinfo.rc = 0
	}
	realinfo.rc++
	return true
}

func (rtnamemap *rtnamemap) downgradelock(name string) {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()

	info, found := rtnamemap.m[name]
	assert(found)
	info.exclusive = false
	info.rc++
	assert(info.rc == 1)
}

func (rtnamemap *rtnamemap) unlockname(name string, exclusive bool) {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()

	info, ok := rtnamemap.m[name]
	assert(ok)
	if exclusive {
		assert(info.exclusive)
		delete(rtnamemap.m, name)
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(rtnamemap.m, name)
	}
}

// FIXME: TODO: support timeout
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
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+1))
		case <-rtnamemap.abrt:
			return
		}
	}
}

// log pending
func (rtnamemap *rtnamemap) log() {
	rtnamemap.Lock()
	defer rtnamemap.Unlock()
	for name, info := range rtnamemap.m {
		glog.Infof("rtnamemap: %s => %s", name, info.String())
	}
}
