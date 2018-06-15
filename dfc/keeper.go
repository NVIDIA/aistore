// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	proxypollival = time.Second * 5
	targetpollivl = time.Second * 5
	kalivetimeout = time.Second * 2

	someError  = "error"
	stop       = "stop"
	register   = "register"
	unregister = "unregister"
)

type kaliveif interface {
	onerr(err error, status int)
	heardFrom(sid string, reset bool)
	keepalive(err error) (stopped bool)
	timedOut(sid string) bool
}

type kalive struct {
	namedrunner
	k                          kaliveif
	controlCh                  chan controlSignal
	primaryKeepaliveInProgress int64 // used by primary proxy only
	tracker                    KeepaliveTracker
	interval                   time.Duration
}

type proxykalive struct {
	kalive
	p *proxyrunner
}

type targetkalive struct {
	kalive
	t *targetrunner
}

// KeepaliveTracker defines the interface for keep alive tracking.
// It is safe for concurrent access.
type KeepaliveTracker interface {
	// HeardFrom notifies the tracker that a message is received from server identified by 'id'
	// 'reset' is true indicates the heard from is not a result of a regular keepalive call.
	// it could be a reconnect, re-register, normally this indicates to discard previous data and
	// start fresh.
	HeardFrom(id string, reset bool)
	// TimedOut returns true if it is determined that a message has not been received from a server
	// soon enough so it is consider that the server is down
	TimedOut(id string) bool
}

var (
	_ kaliveif = &targetkalive{}
	_ kaliveif = &proxykalive{}
)

type controlSignal struct {
	msg string
	err error
}

func newproxykalive(p *proxyrunner) *proxykalive {
	k := &proxykalive{p: p}
	k.kalive.k = k
	k.controlCh = make(chan controlSignal, 1)
	k.tracker = NewKeepaliveTracker(
		&ctx.config.KeepaliveTracker.Proxy,
		&p.statsdC,
	)
	k.interval = ctx.config.KeepaliveTracker.Proxy.Interval
	return k
}

func newtargetkalive(t *targetrunner) *targetkalive {
	k := &targetkalive{t: t}
	k.kalive.k = k
	k.controlCh = make(chan controlSignal, 1)
	k.tracker = NewKeepaliveTracker(
		&ctx.config.KeepaliveTracker.Target,
		&t.statsdC,
	)
	k.interval = ctx.config.KeepaliveTracker.Target.Interval
	return k
}

func (r *kalive) onerr(err error, status int) {
	if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
		r.controlCh <- controlSignal{msg: someError, err: err}
	}
}

func (r *kalive) heardFrom(sid string, reset bool) {
	r.tracker.HeardFrom(sid, reset)
}

func (r *kalive) timedOut(sid string) bool {
	return r.tracker.TimedOut(sid)
}

func (r *kalive) run() error {
	glog.Infof("Starting %s", r.name)
	ticker := time.NewTicker(r.interval)
	lastCheck := time.Time{}

	for {
		select {
		case <-ticker.C:
			lastCheck = time.Now()
			r.k.keepalive(nil)
		case sig := <-r.controlCh:
			switch sig.msg {
			case register:
				ticker.Stop()
				ticker = time.NewTicker(r.interval)
			case unregister:
				ticker.Stop()
			case stop:
				ticker.Stop()
				return nil
			case someError:
				if time.Since(lastCheck) >= proxypollival {
					lastCheck = time.Now()
					if stopped := r.k.keepalive(sig.err); stopped {
						ticker.Stop()
						return nil
					}
				}
			}
		}
	}
}

func (r *kalive) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	r.controlCh <- controlSignal{msg: stop}
	close(r.controlCh)
}

type Registerer interface {
	register(timeout time.Duration) (int, error)
}

// keepaliveCommon is shared by none primary proxies and targets
// calls register right away, if it fails, continue to call until successfully registered or asked to stop
func keepaliveCommon(r Registerer, controlCh chan controlSignal) (stopped bool) {
	timeout := kalivetimeout
	status, err := r.register(timeout)
	if err == nil {
		return
	}

	glog.Infof("Warning: keepalive failed with err: %v, status %d", err, status)

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
		case sig := <-controlCh:
			if sig.msg == stop {
				stopped = true
				return
			}
		}
	}
}

func (r *proxykalive) keepalive(err error) (stopped bool) {
	if err != nil {
		glog.Infof("proxy %s keepalive triggered by err %v", r.p.si.DaemonID, err)
	}

	smapLock.Lock()

	if r.p.primary {
		smapLock.Unlock()
		return r.primarykeepalive()
	}

	if r.p.smap.ProxySI == nil || !r.timedOut(r.p.smap.ProxySI.DaemonID) {
		smapLock.Unlock()
		return
	}

	smapLock.Unlock()

	stopped = keepaliveCommon(r.p, r.controlCh)
	if stopped {
		r.p.onPrimaryProxyFailure()
	}

	return stopped
}

func (r *proxykalive) keepaliveDaemonMap(
	daemons map[string]*daemonInfo, deleteFunc func(string)) (stopped bool, changed bool) {
	from := "?" + URLParamFromID + "=" + r.p.si.DaemonID

	for sid, si := range daemons {
		if si.DaemonID == r.p.si.DaemonID {
			// skip self
			continue
		}
		if !r.timedOut(sid) {
			continue
		}

		url := si.DirectURL + URLPath(Rversion, Rhealth)
		url += from
		res := r.p.call(nil, si, url, http.MethodGet, nil, kalivetimeout)
		if res.err == nil {
			continue
		}

		if res.status > 0 {
			glog.Infof("Warning: %s fails keepalive with status %d, err: %v", sid, res.status, res.err)
		} else {
			glog.Infof("Warning: %s fails keepalive, err: %v", sid, res.err)
		}

		responded, stopped := r.poll(si, url)
		if stopped {
			return true, false
		}

		if responded {
			continue
		}

		// FIXME: Seek confirmation when keepalive fails
		// the verdict
		if res.status > 0 {
			glog.Errorf("%s fails keepalive with status %d, err: %v - removing from the cluster map",
				sid, res.status, res.err)
		} else {
			glog.Errorf("%s fails keepalive, err: %v - removing from the cluster map", sid, res.err)
		}

		smapLock.Lock()
		deleteFunc(sid)
		smapLock.Unlock()
		changed = true
	}

	return false, changed
}

func (r *proxykalive) primarykeepalive() (stopped bool) {
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, 0, aval) {
		glog.Infof("primary keepalive is already in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, aval, 0)

	stopped, targetsChanged := r.keepaliveDaemonMap(r.p.smap.Tmap, r.p.smap.del)
	if stopped {
		return true
	}
	stopped, proxiesChanged := r.keepaliveDaemonMap(r.p.smap.Pmap, r.p.smap.delProxy)
	if stopped {
		return true
	}

	if targetsChanged || proxiesChanged {
		r.p.metasyncer.sync(false, r.p.smap.cloneL())
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
		if !r.timedOut(si.DaemonID) {
			return true, false
		}
		select {
		case <-poller.C:
			res := r.p.call(nil, si, url, http.MethodGet, nil, timeout)
			if res.err == nil {
				return true, false
			}

			timeout = time.Duration(float64(timeout)*1.5 + 0.5)
			if timeout > ctx.config.Timeout.MaxKeepalive {
				timeout = ctx.config.Timeout.Default
				maxedout++
			}

			if IsErrConnectionRefused(res.err) || res.status == http.StatusRequestTimeout {
				continue
			}

			glog.Warningf("keepalive: Unexpected status %d, err: %v", res.status, res.err)
		case sig := <-r.controlCh:
			if sig.msg == stop {
				return false, true
			}
		}
	}
	return false, false
}

func (r *targetkalive) keepalive(err error) (stopped bool) {
	if err != nil {
		glog.Infof("target %s keepalive triggered by err %v", r.t.si.DaemonID, err)
	}

	stopped = keepaliveCommon(r.t, r.controlCh)
	if stopped {
		r.t.onPrimaryProxyFailure()
	}

	return stopped
}
