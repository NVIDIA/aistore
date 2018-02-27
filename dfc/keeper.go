// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	errorpollival = time.Second
	errorpollmaxc = 5
)

type kaliveif interface {
	onerr(err error, status int)
	timestamp(directurl string)
	keepalive(err error) (stopped bool)
}

type okmap struct {
	sync.Mutex
	okmap map[string]time.Time
}

type kalive struct {
	namedrunner
	k        kaliveif
	h        *httprunner
	checknow chan error
	chstop   chan struct{}
	atomic   int64
	okmap    *okmap
}

type proxykalive struct {
	kalive
	p *proxyrunner
}

type targetkalive struct {
	kalive
	t *targetrunner
}

// construction
func newproxykalive(p *proxyrunner) *proxykalive {
	k := &proxykalive{p: p, kalive: kalive{h: &p.httprunner}}
	k.kalive.k = k
	return k
}

func newtargetkalive(t *targetrunner) *targetkalive {
	k := &targetkalive{t: t, kalive: kalive{h: &t.httprunner}}
	k.kalive.k = k
	return k
}

//=========================================================
//
// common methods
//
//=========================================================
func (r *kalive) httpcall(si *daemonInfo, url string, method string, injson []byte) (err error, status int) {
	_, err, _, status = r.h.call(si, url, method, injson)
	return
}

func (r *kalive) onerr(err error, status int) {
	if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
		r.checknow <- err
	}
}

func (r *kalive) timestamp(sid string) {
	r.okmap.Lock()
	r.okmap.okmap[sid] = time.Now()
	r.okmap.Unlock()
}

func (r *kalive) skipCheck(sid string) bool {
	r.okmap.Lock()
	last, ok := r.okmap.okmap[sid]
	r.okmap.Unlock()
	return ok && time.Now().Sub(last) < ctx.config.KeepAliveTime
}

func (r *kalive) run() error {
	glog.Infof("Starting %s", r.name)
	r.chstop = make(chan struct{}, 16)
	r.checknow = make(chan error, 16)
	r.okmap = &okmap{okmap: make(map[string]time.Time, 16)}
	ticker := time.NewTicker(ctx.config.KeepAliveTime)
	for {
		select {
		case <-ticker.C:
			r.k.keepalive(nil)
		case err := <-r.checknow:
			if stopped := r.k.keepalive(err); stopped {
				ticker.Stop()
				return nil
			}
		case <-r.chstop:
			ticker.Stop()
			return nil
		}
	}
}

func (r *kalive) poll(si *daemonInfo, url string, jsbytes []byte) (responded, stopped bool) {
	poller := time.NewTicker(errorpollival)
	defer poller.Stop()
	for i := 0; i < errorpollmaxc; i++ {
		select {
		case <-poller.C:
			err, status := r.httpcall(si, url, http.MethodGet, jsbytes)
			if err == nil {
				return true, false
			}
			if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
				continue
			}
			glog.Warningf("keepalive: Unexpected status %d, err: %v", status, err)
		case <-r.chstop:
			return false, true
		}
	}
	return false, false
}

func (r *kalive) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chstop <- v
	close(r.chstop)
}

//==========================================
//
// proxykalive: implements keepaliveinterace
//
//===========================================
func (r *proxykalive) keepalive(err error) (stopped bool) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&r.atomic, 0, aval) {
		glog.Infof("keepalive-alltargets is in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&r.atomic, aval, 0)
	if err != nil {
		glog.Infof("keepalive-alltargets: got err %v, checking now...", err)
	}
	msg := &GetMsg{GetWhat: GetWhatStats}
	jsbytes, err := json.Marshal(msg)
	assert(err == nil, err)
	for sid, si := range ctx.smap.Smap {
		if r.skipCheck(sid) {
			continue
		}
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
		_, err, _, status := r.p.call(si, url, http.MethodGet, jsbytes)
		if err == nil {
			continue
		}
		if status > 0 {
			glog.Infof("Warning: target %s fails keepalive with status %d, err: %v", sid, status, err)
		} else {
			glog.Infof("Warning: target %s fails keepalive, err: %v", sid, err)
		}
		responded, stopped := r.poll(si, url, jsbytes)
		if stopped {
			return true
		}
		if responded {
			continue
		}
		// the verdict
		if status > 0 {
			glog.Errorf("Target %s fails keepalive with status %d, err: %v - removing from the cluster map", sid, status, err)
		} else {
			glog.Errorf("Target %s fails keepalive, err: %v - removing from the cluster map", sid, err)
		}
		ctx.smap.lock()
		ctx.smap.del(sid)
		ctx.smap.unlock()
	}
	return false
}

//==========================================
//
// targetkalive - implements keepaliveinterace
//
//===========================================
func (r *targetkalive) keepalive(err error) (stopped bool) {
	if r.t.proxysi == nil || r.skipCheck(r.t.proxysi.DaemonID) {
		return
	}
	msg := GetMsg{GetWhat: GetWhatSmap}
	jsbytes, err := json.Marshal(&msg)
	if err != nil {
		glog.Errorf("Unexpected failure to json-marshal %v, err: %v", msg, err)
		return
	}
	url := ctx.config.Proxy.URL + "/" + Rversion + "/" + Rcluster
	_, err, _, status := r.t.call(r.t.proxysi, url, http.MethodGet, jsbytes)
	if err == nil {
		return
	}
	if status > 0 {
		glog.Infof("Warning: proxy %s fails keepalive with status %d, err: %v", r.t.proxysi.DaemonID, status, err)
	} else {
		glog.Infof("Warning: proxy %s fails keepalive, err: %v", r.t.proxysi.DaemonID, err)
	}
	r.poll(r.t.proxysi, url, jsbytes)
	return
}
