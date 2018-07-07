// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
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

// keepaliveCommon is shared by non-primary proxies and targets
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

			glog.Warningf("keepalive: err %v (%d)", err, status)
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

	smap := r.p.smapowner.get()
	if smap == nil || !smap.isValid() {
		return
	}
	if smap.isPrimary(r.p.si) {
		return r.primarykeepalive()
	}
	if smap.ProxySI == nil || !r.timedOut(smap.ProxySI.DaemonID) {
		return
	}

	stopped = keepaliveCommon(r.p, r.controlCh)
	if stopped {
		r.p.onPrimaryProxyFailure()
	}

	return stopped
}

func (r *proxykalive) primaryLiveAndSync(checkProxies bool) (stopped, repeat, nonprimary bool) {
	var (
		daemons     map[string]*daemonInfo
		action      string
		removed     int
		clone       *Smap
		from        = "?" + URLParamFromID + "=" + r.p.si.DaemonID
		smap        = r.p.smapowner.get()
		origversion = smap.version()
		tag         = "target"
	)
	if checkProxies {
		tag = "proxy"
		daemons = smap.Pmap
	} else {
		daemons = smap.Tmap
	}
	for sid, si := range daemons {
		if si.DaemonID == r.p.si.DaemonID {
			assert(checkProxies)
			continue
		}
		if !r.p.smapowner.get().isPrimary(r.p.si) {
			nonprimary = true
			return
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

		glog.Infof("Warning: %s %s fails keepalive: err %v (status %d)", tag, sid, res.err, res.status)

		var responded bool
		responded, stopped = r.poll(si, url)
		if stopped {
			return
		}
		if responded {
			continue
		}

		// the verdict
		glog.Errorf("%s %s fails keepalive: err %v (status %d) - removing", tag, sid, res.err, res.status)
		if clone == nil {
			clone = smap.clone()
		}
		if checkProxies {
			clone.delProxy(sid)
		} else {
			clone.delTarget(sid)
		}
		removed++
	}

	if removed == 0 { // all good
		return
	}

	r.p.smapowner.Lock()
	currversion := r.p.smapowner.get().version()
	if currversion != origversion {
		// serializing keepalive removals vis-a-vis cluster map changes that may have occured
		// for any other (non keepalive-related) reasons
		glog.Warningf("Concurrent: Smap version change (%d => %d) and keepalive removals (%t/%d)",
			origversion, currversion, checkProxies, removed)
		repeat = true
		r.p.smapowner.Unlock()
		return
	}
	if !r.p.smapowner.get().isPrimary(r.p.si) {
		glog.Infof("Concurrent: primary => non-primary and keepalive removals (%t/%d)",
			origversion, currversion, checkProxies, removed)
		r.p.smapowner.Unlock()
		return
	}
	r.p.smapowner.put(clone)
	if errstr := r.p.smapowner.persist(clone, true); errstr != "" {
		glog.Errorln(errstr)
	}
	r.p.smapowner.Unlock()

	if checkProxies {
		action = fmt.Sprintf("keepalive: removed %d proxy/gateway(s)", removed)
	} else {
		action = fmt.Sprintf("keepalive: removed %d target(s)", removed)
	}
	msg := &ActionMsg{Action: action}
	pair := &revspair{clone, msg}
	r.p.metasyncer.sync(true, pair)
	return
}

func (r *proxykalive) primarykeepalive() (stopped bool) {
	var repeat, nonprimary bool
	aval := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, 0, aval) {
		glog.Infof("primary keepalive is already in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, aval, 0)
rep1:
	stopped, repeat, nonprimary = r.primaryLiveAndSync(false /*checkProxies */)
	if stopped || nonprimary {
		return
	}
	if repeat {
		time.Sleep(proxypollival)
		goto rep1
	}
rep2:
	stopped, repeat, nonprimary = r.primaryLiveAndSync(true /*checkProxies */)
	if stopped || nonprimary {
		return
	}
	if repeat {
		time.Sleep(proxypollival)
		goto rep2
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
		smap := r.t.smapowner.get()
		if smap != nil && smap.isValid() {
			r.t.onPrimaryProxyFailure()
		}
	}

	return stopped
}
