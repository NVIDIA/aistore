// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"math/rand"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

type rtnamemap struct {
	sync.Mutex
	m       map[string]*pendinginfo
	abrt    chan struct{}
	aborted bool
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
	return info.fqn
}

func newrtnamemap(size int) *rtnamemap {
	m := make(map[string]*pendinginfo, size)
	return &rtnamemap{m: m, abrt: make(chan struct{})}
}

func (rtnamemap *rtnamemap) trylockname(name string, exclusive bool, info *pendinginfo) bool {
	rtnamemap.Lock()

	_, found := rtnamemap.m[name]
	if found && exclusive {
		rtnamemap.Unlock()
		return false
	}
	if exclusive {
		rtnamemap.m[name] = info
		info.exclusive = true
		info.rc = 0
		rtnamemap.Unlock()
		return true
	}
	// rlock
	var realinfo *pendinginfo
	if found {
		realinfo = rtnamemap.m[name]
		if realinfo.exclusive {
			rtnamemap.Unlock()
			return false
		}
	} else {
		rtnamemap.m[name] = info // the 1st rlock
		realinfo = info
		realinfo.exclusive = false
		realinfo.rc = 0
	}
	realinfo.rc++
	rtnamemap.Unlock()
	return true
}

func (rtnamemap *rtnamemap) downgradelock(name string) {
	rtnamemap.Lock()

	info, found := rtnamemap.m[name]
	assert(found)
	info.exclusive = false
	info.rc++
	assert(info.rc == 1)
	rtnamemap.Unlock()
}

func (rtnamemap *rtnamemap) unlockname(name string, exclusive bool) {
	rtnamemap.Lock()
	if rtnamemap.aborted {
		rtnamemap.Unlock()
		return
	}
	info, ok := rtnamemap.m[name]
	assert(ok)
	if exclusive {
		assert(info.exclusive)
		delete(rtnamemap.m, name)
		rtnamemap.Unlock()
		return
	}
	info.rc--
	if info.rc == 0 {
		delete(rtnamemap.m, name)
	}
	rtnamemap.Unlock()
}

// FIXME: TODO: support timeout
func (rtnamemap *rtnamemap) lockname(name string, exclusive bool, info *pendinginfo, poll time.Duration) {
	if rtnamemap.trylockname(name, exclusive, info) {
		return
	}
	ticker := time.NewTicker(poll)
	for {
		select {
		case <-ticker.C:
			if rtnamemap.trylockname(name, exclusive, info) {
				glog.Infof("rtnamemap: lockname %s (%v) done", info.fqn, exclusive)
				ticker.Stop()
				return
			}
			glog.Infof("rtnamemap: lockname %s (%v) - retrying...", info.fqn, exclusive)
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)+1))
		case <-rtnamemap.abrt:
			rtnamemap.aborted = true
			ticker.Stop()
			return
		}
	}
}

// log pending
func (rtnamemap *rtnamemap) log() {
	rtnamemap.Lock()
	for name, info := range rtnamemap.m {
		glog.Infof("rtnamemap: %s => %s", name, info.String())
	}
	rtnamemap.Unlock()
}

func (rtnamemap *rtnamemap) stop() {
	close(rtnamemap.abrt)
}
