/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	keepaliveRetryFactor   = 5
	keepaliveTimeoutFactor = 2

	someError  = "error"
	stop       = "stop"
	register   = "register"
	unregister = "unregister"
)

var (
	// Used as a compile-time check for correct interface implementation.
	_ keepaliver = &targetKeepaliveRunner{}
	_ keepaliver = &proxyKeepaliveRunner{}

	minKeepaliveTime = float64(time.Second.Nanoseconds())
)

type registerer interface {
	register(t time.Duration) (int, error)
}

type keepaliver interface {
	onerr(err error, status int)
	heardFrom(sid string, reset bool)
	doKeepalive() (stopped bool)
	isTimeToPing(sid string) bool
}

type targetKeepaliveRunner struct {
	t *targetrunner
	keepalive
}

type proxyKeepaliveRunner struct {
	p *proxyrunner
	keepalive
}

type keepalive struct {
	namedrunner
	k                          keepaliver
	kt                         KeepaliveTracker
	tt                         *timeoutTracker
	controlCh                  chan controlSignal
	primaryKeepaliveInProgress int64 // A toggle used only by the primary proxy.
	interval                   time.Duration
	maxKeepaliveTime           float64
}

type timeoutTracker struct {
	mu              sync.Mutex
	timeoutStatsMap map[string]*timeoutStats
}

type timeoutStats struct {
	srtt    float64 // smoothed round-trip time in ns
	rttvar  float64 // round-trip time variation in ns
	timeout float64 // in ns
}

type controlSignal struct {
	msg string
	err error
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

func newTargetKeepaliveRunner(t *targetrunner) *targetKeepaliveRunner {
	tkr := &targetKeepaliveRunner{t: t}
	tkr.keepalive.k = tkr
	tkr.kt = newKeepaliveTracker(&ctx.config.KeepaliveTracker.Target, &t.statsdC)
	tkr.tt = &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)}
	tkr.controlCh = make(chan controlSignal, 1)
	tkr.interval = ctx.config.KeepaliveTracker.Target.Interval
	tkr.maxKeepaliveTime = float64(ctx.config.Timeout.MaxKeepalive.Nanoseconds())
	return tkr
}

func newProxyKeepaliveRunner(p *proxyrunner) *proxyKeepaliveRunner {
	pkr := &proxyKeepaliveRunner{p: p}
	pkr.keepalive.k = pkr
	pkr.kt = newKeepaliveTracker(&ctx.config.KeepaliveTracker.Proxy, &p.statsdC)
	pkr.tt = &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)}
	pkr.controlCh = make(chan controlSignal, 1)
	pkr.interval = ctx.config.KeepaliveTracker.Proxy.Interval
	pkr.maxKeepaliveTime = float64(ctx.config.Timeout.MaxKeepalive.Nanoseconds())
	return pkr
}

func (tkr *targetKeepaliveRunner) doKeepalive() (stopped bool) {
	smap := tkr.t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return
	}
	if stopped = tkr.register(tkr.t, smap.ProxySI.DaemonID); stopped {
		if smap = tkr.t.smapowner.get(); smap != nil && smap.isValid() {
			tkr.t.onPrimaryProxyFailure()
		}
	}
	return
}

func (pkr *proxyKeepaliveRunner) doKeepalive() (stopped bool) {
	smap := pkr.p.smapowner.get()
	if smap == nil || !smap.isValid() {
		return
	}
	if smap.isPrimary(pkr.p.si) {
		return pkr.pingAllOthers()
	}
	if !pkr.isTimeToPing(smap.ProxySI.DaemonID) {
		return
	}

	if stopped = pkr.register(pkr.p, smap.ProxySI.DaemonID); stopped {
		pkr.p.onPrimaryProxyFailure()
	}
	return
}

// pingAllOthers is called by the primary proxy to ping all other daemons in the smap concurrently.
// All non-responding daemons are removed from the smap and the resulting smap is synced to all other daemons.
func (pkr *proxyKeepaliveRunner) pingAllOthers() (stopped bool) {
	t := time.Now().Unix()
	if !atomic.CompareAndSwapInt64(&pkr.primaryKeepaliveInProgress, 0, t) {
		glog.Infof("primary keepalive is already in progress...")
		return
	}
	defer atomic.CompareAndSwapInt64(&pkr.primaryKeepaliveInProgress, t, 0)

	var (
		smap       = pkr.p.smapowner.get()
		wg         = &sync.WaitGroup{}
		stoppedCh  = make(chan struct{}, smap.countProxies()+smap.countTargets())
		toRemoveCh = make(chan string, smap.countProxies()+smap.countTargets())
	)
	for _, daemons := range []map[string]*daemonInfo{smap.Tmap, smap.Pmap} {
		for sid, si := range daemons {
			if sid == pkr.p.si.DaemonID {
				continue
			}
			// Skip pinging other daemons until they time out.
			if !pkr.isTimeToPing(sid) {
				continue
			}
			wg.Add(1)
			go func(si *daemonInfo) {
				if len(stoppedCh) > 0 {
					wg.Done()
					return
				}
				ok, s := pkr.ping(si)
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

	pkr.p.smapowner.Lock()
	newSmap := pkr.p.smapowner.get()
	if !newSmap.isPrimary(pkr.p.si) {
		glog.Infoln("primary proxy changed while sending its keepalives," +
			" not removing non-responding daemons from the smap this time")
		pkr.p.smapowner.Unlock()
		return false
	}
	if len(stoppedCh) > 0 {
		pkr.p.smapowner.Unlock()
		return true
	}
	if len(toRemoveCh) == 0 {
		pkr.p.smapowner.Unlock()
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

	pkr.p.smapowner.put(clone)
	if errstr := pkr.p.smapowner.persist(clone, true); errstr != "" {
		glog.Errorln(errstr)
	}
	pkr.p.smapowner.Unlock()

	pkr.p.metasyncer.sync(true, &revspair{
		revs: clone,
		msg: &ActionMsg{
			Action: fmt.Sprintf("keepalive: removing non-responding daemons"),
		},
	})
	return
}

func (pkr *proxyKeepaliveRunner) ping(to *daemonInfo) (ok, stopped bool) {
	url := to.DirectURL + URLPath(Rversion, Rhealth) + "?" + URLParamFromID + "=" + pkr.p.si.DaemonID
	timeout := time.Duration(pkr.timeoutStatsForDaemon(to.DaemonID).timeout)
	t := time.Now()
	res := pkr.p.call(nil, to, url, http.MethodGet, nil, timeout)
	pkr.updateTimeoutForDaemon(to.DaemonID, time.Since(t))

	if res.err == nil {
		return true, false
	}
	glog.Warningf("initial keepalive failed, err: %v, status: %d, polling again", res.err, res.status)
	return pkr.retry(to, url)
}

func (pkr *proxyKeepaliveRunner) retry(si *daemonInfo, url string) (ok, stopped bool) {
	var (
		i       int
		timeout = time.Duration(pkr.timeoutStatsForDaemon(si.DaemonID).timeout)
		ticker  = time.NewTicker(ctx.config.Timeout.CplaneOperation * keepaliveRetryFactor)
	)
	defer ticker.Stop()
	for {
		if !pkr.isTimeToPing(si.DaemonID) {
			return true, false
		}
		select {
		case <-ticker.C:
			t := time.Now()
			res := pkr.p.call(nil, si, url, http.MethodGet, nil, timeout)
			timeout = pkr.updateTimeoutForDaemon(si.DaemonID, time.Since(t))
			if res.err == nil {
				return true, false
			}
			i++
			if i == 3 {
				glog.Warningf("keepalive failed after retrying again three times"+
					", removing daemon %s from smap", si.DaemonID)
				return false, false
			}
			if IsErrConnectionRefused(res.err) || res.status == http.StatusRequestTimeout {
				continue
			}
			glog.Warningf("keepalive: Unexpected status %d, err: %v", res.status, res.err)
		case sig := <-pkr.controlCh:
			if sig.msg == stop {
				return false, true
			}
		}
	}
}

func (k *keepalive) run() error {
	glog.Infof("Starting %s", k.name)
	ticker := time.NewTicker(k.interval)
	lastCheck := time.Time{}

	for {
		select {
		case <-ticker.C:
			lastCheck = time.Now()
			k.k.doKeepalive()
		case sig := <-k.controlCh:
			switch sig.msg {
			case register:
				ticker.Stop()
				ticker = time.NewTicker(k.interval)
			case unregister:
				ticker.Stop()
			case stop:
				ticker.Stop()
				return nil
			case someError:
				if time.Since(lastCheck) >= ctx.config.Timeout.CplaneOperation*keepaliveRetryFactor {
					lastCheck = time.Now()
					glog.Infof("keepalive triggered by err: %v", sig.err)
					if stopped := k.k.doKeepalive(); stopped {
						ticker.Stop()
						return nil
					}
				}
			}
		}
	}
}

// register is called by non-primary proxies and targets to send a keepalive to the primary proxy.
func (k *keepalive) register(r registerer, primaryProxyID string) (stopped bool) {
	timeout := time.Duration(k.timeoutStatsForDaemon(primaryProxyID).timeout)
	now := time.Now()
	s, err := r.register(timeout)
	timeout = k.updateTimeoutForDaemon(primaryProxyID, time.Since(now))
	if err == nil {
		return
	}
	glog.Infof("daemon -> primary proxy keepalive failed, err: %v, status: %d", err, s)

	var i int
	ticker := time.NewTicker(ctx.config.Timeout.CplaneOperation * keepaliveRetryFactor)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			i++
			now = time.Now()
			s, err = r.register(timeout)
			timeout = k.updateTimeoutForDaemon(primaryProxyID, time.Since(now))
			if err == nil {
				glog.Infof(
					"daemon successfully registered after retrying %d times", i)
				return
			}
			if i == 3 {
				glog.Warningf(
					"daemon failed to register after retrying three times, removing from smap")
				return true
			}
			if IsErrConnectionRefused(err) || s == http.StatusRequestTimeout {
				continue
			}
			glog.Warningf(
				"daemon received unexpected response from register, status %d, err: %v", s, err)
		case sig := <-k.controlCh:
			if sig.msg == stop {
				return true
			}
		}
	}
}

// updateTimeoutForDaemon calculates the new timeout for the daemon with ID sid, updates it in
// k.timeoutStatsForDaemon, and returns it. The algorithm is loosely based on TCP's RTO calculation,
// as documented in RFC 6298.
func (k *keepalive) updateTimeoutForDaemon(sid string, t time.Duration) time.Duration {
	const (
		alpha = 0.125
		beta  = 0.25
		c     = 4
	)
	next := float64(t.Nanoseconds())
	ts := k.timeoutStatsForDaemon(sid)
	ts.rttvar = (1-beta)*ts.rttvar + beta*(math.Abs(ts.srtt-next))
	ts.srtt = (1-alpha)*ts.srtt + alpha*next
	ts.timeout = math.Min(k.maxKeepaliveTime, ts.srtt+c*ts.rttvar)
	if ts.timeout < minKeepaliveTime {
		ts.timeout = minKeepaliveTime
	}
	return time.Duration(ts.timeout)
}

// timeoutStatsForDaemon returns the timeoutStats corresponding to daemon ID sid.
// If there is no entry in k.timeoutStats for sid, then the initial timeout will be set to
// maxKeepaliveNS, with the other stats loosely based on RFC 6298.
func (k *keepalive) timeoutStatsForDaemon(sid string) *timeoutStats {
	k.tt.mu.Lock()
	if ts, _ := k.tt.timeoutStatsMap[sid]; ts != nil {
		k.tt.mu.Unlock()
		return ts
	}
	ts := &timeoutStats{srtt: k.maxKeepaliveTime, rttvar: k.maxKeepaliveTime / 2, timeout: k.maxKeepaliveTime}
	k.tt.timeoutStatsMap[sid] = ts
	k.tt.mu.Unlock()
	return ts
}

func (k *keepalive) onerr(err error, status int) {
	if IsErrConnectionRefused(err) || status == http.StatusRequestTimeout {
		k.controlCh <- controlSignal{msg: someError, err: err}
	}
}

func (k *keepalive) heardFrom(sid string, reset bool) {
	k.kt.HeardFrom(sid, reset)
}

func (k *keepalive) isTimeToPing(sid string) bool {
	return k.kt.TimedOut(sid)
}

func (k *keepalive) stop(err error) {
	glog.Infof("Stopping %s, err: %v", k.name, err)
	k.controlCh <- controlSignal{msg: stop}
	close(k.controlCh)
}
