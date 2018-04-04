// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

const (
	proxypollival = time.Second * 5
	targetpollivl = time.Second * 5
	kalivetimeout = time.Second * 2
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
	return ok && time.Since(last) < ctx.config.KeepAliveTime
}

func (r *kalive) run() error {
	glog.Infof("Starting %s", r.name)
	r.chstop = make(chan struct{}, 16)
	r.checknow = make(chan error, 16)
	r.okmap = &okmap{okmap: make(map[string]time.Time, 16)}
	ticker := time.NewTicker(ctx.config.KeepAliveTime)
	lastcheck := time.Time{}
	for {
		select {
		case <-ticker.C:
			lastcheck = time.Now()
			r.k.keepalive(nil)
		case err := <-r.checknow:
			if time.Since(lastcheck) >= proxypollival {
				lastcheck = time.Now()
				if stopped := r.k.keepalive(err); stopped {
					ticker.Stop()
					return nil
				}
			}
		case <-r.chstop:
			ticker.Stop()
			return nil
		}
	}
}

func (r *kalive) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chstop <- v
	close(r.chstop)
}

//==========================================
//
// proxykalive: implements kaliveif
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
	from := "?" + URLParamFromID + "=" + r.p.si.DaemonID
	for sid, si := range ctx.smap.Smap {
		if r.skipCheck(sid) {
			continue
		}
		url := si.DirectURL + "/" + Rversion + "/" + Rhealth
		url += from
		_, err, _, status := r.p.call(si, url, http.MethodGet, nil, kalivetimeout)
		if err == nil {
			continue
		}
		if status > 0 {
			glog.Infof("Warning: target %s fails keepalive with status %d, err: %v", sid, status, err)
		} else {
			glog.Infof("Warning: target %s fails keepalive, err: %v", sid, err)
		}
		responded, stopped := r.poll(si, url)
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

func (r *proxykalive) poll(si *daemonInfo, url string) (responded, stopped bool) {
	var (
		maxedout = 0
		timeout  = kalivetimeout
		poller   = time.NewTicker(proxypollival)
	)
	defer poller.Stop()
	for maxedout < 2 {
		if r.skipCheck(si.DaemonID) {
			return true, false
		}
		select {
		case <-poller.C:
			_, err, _, status := r.p.call(si, url, http.MethodGet, nil, timeout)
			if err == nil {
				return true, false
			}
			timeout = time.Duration(float64(timeout)*1.5 + 0.5)
			if timeout > ctx.config.HTTP.Timeout {
				timeout = ctx.config.HTTP.Timeout
				maxedout++
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

//==========================================
//
// targetkalive - implements kaliveif
//
//===========================================
func (r *targetkalive) keepalive(err error) (stopped bool) {
	if r.t.proxysi == nil || r.skipCheck(r.t.proxysi.DaemonID) {
		return
	}
	timeout := kalivetimeout
	status, err := r.t.register(timeout)
	if err == nil {
		return
	}
	if status > 0 {
		glog.Infof("Warning: keepalive failed with status %d, err: %v", status, err)
	} else {
		glog.Infof("Warning: keepalive failed, err: %v", err)
	}
	// until success or stop
	poller := time.NewTicker(targetpollivl)
	defer poller.Stop()
	for {
		select {
		case <-poller.C:
			status, err := r.t.register(timeout)
			if err == nil {
				glog.Infoln("keepalive: successfully re-registered")
				return
			}
			timeout = time.Duration(float64(timeout)*1.5 + 0.5)
			if timeout > ctx.config.HTTP.Timeout {
				timeout = ctx.config.HTTP.Timeout
			}
			if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
				continue
			}
			glog.Warningf("keepalive: Unexpected status %d, err: %v", status, err)
		case <-r.chstop:
			stopped = true
			return
		}
	}
}
