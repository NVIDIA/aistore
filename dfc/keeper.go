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
	timestamp(sid string)
	getTimestamp(sid string) time.Time
	keepalive(err error) (stopped bool)
}

type okmap struct {
	sync.Mutex
	okmap map[string]time.Time
}

type kalive struct {
	namedrunner
	k        kaliveif
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
	k := &proxykalive{p: p}
	k.kalive.k = k
	return k
}

func newtargetkalive(t *targetrunner) *targetkalive {
	k := &targetkalive{t: t}
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
	defer r.okmap.Unlock()
	r.okmap.okmap[sid] = time.Now()
}

func (r *kalive) getTimestamp(sid string) time.Time {
	r.okmap.Lock()
	defer r.okmap.Unlock()
	return r.okmap.okmap[sid]
}

func (r *kalive) skipCheck(sid string) bool {
	r.okmap.Lock()
	last, ok := r.okmap.okmap[sid]
	r.okmap.Unlock()
	return ok && time.Since(last) < ctx.config.Periodic.KeepAliveTime
}

func (r *kalive) run() error {
	glog.Infof("Starting %s", r.name)
	r.chstop = make(chan struct{}, 4)
	r.checknow = make(chan error, 16)
	r.okmap = &okmap{okmap: make(map[string]time.Time, 16)}
	ticker := time.NewTicker(ctx.config.Periodic.KeepAliveTime)
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

//===============================================
//
// Generic Keepalive (Non-Primary Proxy & Target)
//
//===============================================
type Registerer interface {
	register(timeout time.Duration) (int, error)
}

func keepalive(r Registerer, chstop chan struct{}, err error) (stopped bool) {
	timeout := kalivetimeout
	status, err := r.register(timeout)
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
			status, err := r.register(timeout)
			if err == nil {
				glog.Infoln("keepalive: successfully re-registered")
				return
			}
			timeout = time.Duration(float64(timeout)*1.5 + 0.5)
			if timeout > ctx.config.Timeout.MaxKeepalive || IsErrConnectionRefused(err) {
				stopped = true
				return
			}
			if status > 0 {
				glog.Infof("Warning: keepalive failed with status %d, err: %v", status, err)
			} else {
				glog.Infof("Warning: keepalive failed, err: %v", err)
			}
			if status == http.StatusRequestTimeout {
				continue
			}
			glog.Warningf("keepalive: Unexpected status %d, err: %v", status, err)
		case <-chstop:
			stopped = true
			return
		}
	}
}

//==========================================
//
// proxykalive: implements kaliveif
//
//===========================================
func (r *proxykalive) keepalive(err error) (stopped bool) {
	if r.p.primary {
		return r.primarykeepalive(err)
	}

	if r.p.proxysi == nil || r.skipCheck(r.p.proxysi.DaemonID) {
		return
	}
	stopped = keepalive(r.p, r.chstop, err)
	if stopped {
		r.p.onPrimaryProxyFailure()
	}
	return stopped
}

func (r *proxykalive) primarykeepalive(err error) (stopped bool) {
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
	for sid, si := range r.p.smap.Smap {
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
		// FIXME: Seek confirmation when keepalive fails
		// the verdict
		if status > 0 {
			glog.Errorf("Target %s fails keepalive with status %d, err: %v - removing from the cluster map", sid, status, err)
		} else {
			glog.Errorf("Target %s fails keepalive, err: %v - removing from the cluster map", sid, err)
		}
		r.p.smap.lock()
		r.p.smap.del(sid)
		r.p.smap.unlock()
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
			if timeout > ctx.config.Timeout.MaxKeepalive {
				timeout = ctx.config.Timeout.Default
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
	stopped = keepalive(r.t, r.chstop, err)
	if stopped {
		r.t.onPrimaryProxyFailure()
	}
	return stopped
}
