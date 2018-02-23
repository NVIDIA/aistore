// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	keepaliveival = time.Minute * 2
	errorpollival = time.Second
	errorpollmaxc = 5
)

type keepalive struct {
	namedrunner
	p            *proxyrunner
	checknow     chan error
	chstop       chan struct{}
	keepaliveinp int64
}

// run
func (r *keepalive) run() error {
	glog.Infof("Starting %s", r.name)
	r.p = getproxy()
	r.chstop = make(chan struct{}, 1)
	r.checknow = make(chan error, 1)
	ticker := time.NewTicker(keepaliveival)
	for {
		select {
		case <-ticker.C:
			r.alltargets(nil)
		case err := <-r.checknow:
			if stopped := r.alltargets(err); stopped {
				ticker.Stop()
				return nil
			}
		case <-r.chstop:
			ticker.Stop()
			return nil
		}
	}
}

// keeps all targets alive, or else
func (r *keepalive) alltargets(err error) (stopped bool) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&r.keepaliveinp, 0, aval) {
		glog.Infof("synchronizeMaps is already running")
		return
	}
	defer atomic.CompareAndSwapInt64(&r.keepaliveinp, aval, 0)

	msg := &GetMsg{GetWhat: GetWhatStats}
	jsbytes, err := json.Marshal(msg)
	assert(err == nil, err)
	for sid, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
		_, err := r.p.call(url, http.MethodGet, jsbytes)
		if err == nil {
			continue
		}
		glog.Infof("Warning: target %s does not respond to keepalive", sid)
		glog.Flush()
		responded, stopped := r.poll(url, jsbytes)
		if stopped {
			return true
		}
		if responded {
			continue
		}
		// the verdict: target is down
		glog.Errorf("Target %s does not respond to keepalive - removing it from the Smap", sid)
		ctx.smap.lock()
		ctx.smap.del(sid)
		ctx.smap.unlock()
	}
	if glog.V(3) {
		glog.Infof("KeepAlive: all good")
	}
	return false
}

func (r *keepalive) poll(url string, jsbytes []byte) (responded, stopped bool) {
	poller := time.NewTicker(errorpollival)
	defer poller.Stop()
	for i := 0; i < errorpollmaxc; i++ {
		select {
		case <-poller.C:
			_, err := r.p.call(url, http.MethodGet, jsbytes)
			if err == nil {
				return true, false
			}
		case <-r.chstop:
			return false, true
		}
	}
	return false, false
}

// stop gracefully
func (r *keepalive) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chstop <- v
	close(r.chstop)
}
