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
	sync.RWMutex
	m    map[string]fmt.Stringer
	abrt chan struct{}
}

// in-progress file/object GETs, PUTs and DELETEs
type pendinginfo struct {
	time.Time
	verb string
}

//
// methods
//
func (info *pendinginfo) String() string {
	return fmt.Sprintf("%s", info.verb)
}

func newrtnamemap(size int) *rtnamemap {
	m := make(map[string]fmt.Stringer, size)
	return &rtnamemap{m: m, abrt: make(chan struct{})}
}

func (rtnamemap *rtnamemap) trylockname(name string, exclusive bool, info fmt.Stringer) bool {
	if exclusive {
		rtnamemap.Lock()
		defer rtnamemap.Unlock()
	} else {
		rtnamemap.RLock()
		defer rtnamemap.RUnlock()
	}
	if _, ok := rtnamemap.m[name]; ok {
		return false
	}
	rtnamemap.m[name] = info
	return true
}

func (rtnamemap *rtnamemap) unlockname(name string, exclusive bool) {
	if exclusive {
		rtnamemap.Lock()
		defer rtnamemap.Unlock()
	} else {
		rtnamemap.RLock()
		defer rtnamemap.RUnlock()
	}
	if _, ok := rtnamemap.m[name]; !ok {
		assert(false)
	}
	delete(rtnamemap.m, name)
}

func (rtnamemap *rtnamemap) lockname(name string, exclusive bool, info fmt.Stringer, poll time.Duration) {
	if rtnamemap.trylockname(name, exclusive, info) {
		return
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if rtnamemap.trylockname(name, exclusive, info) {
				glog.Infof("rtnamemap: lockname %s (%v) done", name, exclusive)
				return
			}
			glog.Infof("rtnamemap: lockname %s (%v) - retrying...", name, exclusive)
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
