// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	pollRetryFactor        = 5
	keepaliveTimeoutFactor = 2

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
				if time.Since(lastCheck) >= ctx.config.Timeout.CplaneOperation*pollRetryFactor {
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
	timeout := ctx.config.Timeout.CplaneOperation * keepaliveTimeoutFactor
	status, err := r.register(timeout)
	if err == nil {
		return
	}

	glog.Infof("Warning: keepalive failed with err: %v, status %d", err, status)

	// until success or stop
	poller := time.NewTicker(ctx.config.Timeout.CplaneOperation * pollRetryFactor)
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
		return r.pingAllOthers()
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

// pingAllOthers is called by the primary proxy to ping all other daemons in the smap concurrently.
// All non-responding daemons are removed from the smap and the resulting smap is synced to all other daemons.
func (r *proxykalive) pingAllOthers() (stopped bool) {
	t := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, 0, t) {
		glog.Infof("primary keepalive is already in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&r.primaryKeepaliveInProgress, t, 0)

	var (
		smap       = r.p.smapowner.get()
		wg         = &sync.WaitGroup{}
		stoppedCh  = make(chan struct{}, smap.countProxies()+smap.countTargets())
		toRemoveCh = make(chan string, smap.countProxies()+smap.countTargets())
	)
	for _, daemons := range []map[string]*daemonInfo{smap.Tmap, smap.Pmap} {
		for sid, si := range daemons {
			if sid == r.p.si.DaemonID {
				continue
			}
			// Skip pinging other daemons until they time out.
			if !r.timedOut(sid) {
				continue
			}
			wg.Add(1)
			go func(si *daemonInfo) {
				if len(stoppedCh) > 0 {
					wg.Done()
					return
				}
				ok, s := r.ping(si)
				if s {
					stoppedCh <- struct{}{}
				}
				if !ok {
					toRemoveCh <- si.DaemonID
				}
				wg.Done()
			}(si)
		}
	}
	wg.Wait()
	close(stoppedCh)
	close(toRemoveCh)

	r.p.smapowner.Lock()
	newSmap := r.p.smapowner.get()
	if !newSmap.isPrimary(r.p.si) {
		glog.Infoln("primary proxy changed while sending its keepalives," +
			" not removing non-responding daemons from the smap this time")
		r.p.smapowner.Unlock()
		return false
	}
	if len(stoppedCh) > 0 {
		r.p.smapowner.Unlock()
		return true
	}
	if len(toRemoveCh) == 0 {
		r.p.smapowner.Unlock()
		return false
	}
	clone := newSmap.clone()
	for sid := range toRemoveCh {
		if clone.getProxy(sid) != nil {
			clone.delProxy(sid)
		} else {
			clone.delTarget(sid)
		}
	}

	r.p.smapowner.put(clone)
	if errstr := r.p.smapowner.persist(clone, true); errstr != "" {
		glog.Errorln(errstr)
	}
	r.p.smapowner.Unlock()

	r.p.metasyncer.sync(true, &revspair{
		revs: clone,
		msg: &ActionMsg{
			Action: fmt.Sprintf("keepalive: removing non-responding daemons"),
		},
	})
	return
}

func (r *proxykalive) ping(si *daemonInfo) (ok, stopped bool) {
	url := si.DirectURL + URLPath(Rversion, Rhealth) + "?" + URLParamFromID + "=" + r.p.si.DaemonID
	res := r.p.call(nil, si, url, http.MethodGet, nil,
		ctx.config.Timeout.CplaneOperation*keepaliveTimeoutFactor)
	if res.err == nil {
		return true, false
	}
	glog.Warningf("initial keepalive failed, err: %v, status: %d, polling again", res.err, res.status)
	return r.retry(si, url)
}

func (r *proxykalive) retry(si *daemonInfo, url string) (ok, stopped bool) {
	var (
		maxedout = 0
		timeout  = ctx.config.Timeout.CplaneOperation * keepaliveTimeoutFactor
		poller   = time.NewTicker(ctx.config.Timeout.CplaneOperation * pollRetryFactor)
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
	glog.Warningf("keepalive timed out, removing %s from Smap", si.DaemonID)
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
