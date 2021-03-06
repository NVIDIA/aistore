// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/stats"
)

const (
	kaErrorMsg      = "error"
	kaStopMsg       = "stop"
	kaRegisterMsg   = "register"
	kaUnregisterMsg = "unregister"

	kaNumRetries = 3
)

// interface guard
var (
	_ cmn.ConfigListener = (*targetKeepaliveRunner)(nil)
	_ cmn.ConfigListener = (*proxyKeepaliveRunner)(nil)
	_ keepaliver         = (*targetKeepaliveRunner)(nil)
	_ keepaliver         = (*proxyKeepaliveRunner)(nil)
)

type keepaliver interface {
	onerr(err error, status int)
	heardFrom(sid string, reset bool)
	doKeepalive() (stopped bool)
	isTimeToPing(sid string) bool
	send(msg string)

	cmn.ConfigListener
}

type targetKeepaliveRunner struct {
	t *targetrunner
	keepalive
}

type proxyKeepaliveRunner struct {
	p *proxyrunner
	keepalive
	stoppedCh  chan struct{}
	toRemoveCh chan string
	latencyCh  chan time.Duration
}

type keepalive struct {
	name                       string
	k                          keepaliver
	kt                         KeepaliveTracker
	tt                         *timeoutTracker
	statsT                     stats.Tracker
	controlCh                  chan controlSignal
	primaryKeepaliveInProgress atomic.Int64 // A toggle used only by the primary proxy.
	interval                   time.Duration
	maxKeepaliveTime           float64
	startedUp                  *atomic.Bool
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
	// HeardFrom notifies tracker that the server identified by 'id' has responded;
	// 'reset'=true when it is not a regular keepalive call (could be reconnect or re-register).
	HeardFrom(id string, reset bool)
	// TimedOut returns true if the 'id` server did not respond - an indication that the server
	// could be down
	TimedOut(id string) bool
}

// interface guard
var (
	_ cmn.Runner = (*targetKeepaliveRunner)(nil)
	_ cmn.Runner = (*proxyKeepaliveRunner)(nil)
)

func newTargetKeepaliveRunner(t *targetrunner, statsT stats.Tracker, startedUp *atomic.Bool) *targetKeepaliveRunner {
	config := cmn.GCO.Get()

	tkr := &targetKeepaliveRunner{t: t}
	tkr.keepalive.name = "targetkeepalive"
	tkr.keepalive.k = tkr
	tkr.statsT = statsT
	tkr.keepalive.startedUp = startedUp
	tkr.kt = newKeepaliveTracker(config.Keepalive.Target)
	tkr.tt = &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)}
	tkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	tkr.interval = config.Keepalive.Target.Interval
	tkr.maxKeepaliveTime = float64(config.Timeout.MaxKeepalive.Nanoseconds())
	return tkr
}

func newProxyKeepaliveRunner(p *proxyrunner, statsT stats.Tracker, startedUp *atomic.Bool) *proxyKeepaliveRunner {
	config := cmn.GCO.Get()

	pkr := &proxyKeepaliveRunner{p: p}
	pkr.keepalive.name = "proxykeepalive"
	pkr.keepalive.k = pkr
	pkr.statsT = statsT
	pkr.keepalive.startedUp = startedUp
	pkr.kt = newKeepaliveTracker(config.Keepalive.Proxy)
	pkr.tt = &timeoutTracker{timeoutStatsMap: make(map[string]*timeoutStats)}
	pkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	pkr.interval = config.Keepalive.Proxy.Interval
	pkr.maxKeepaliveTime = float64(config.Timeout.MaxKeepalive.Nanoseconds())
	return pkr
}

func (tkr *targetKeepaliveRunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	if !reflect.DeepEqual(oldConf.Keepalive.Target, newConf.Keepalive.Target) {
		tkr.kt = newKeepaliveTracker(newConf.Keepalive.Target)
		tkr.interval = newConf.Keepalive.Target.Interval
	}
	tkr.maxKeepaliveTime = float64(newConf.Timeout.MaxKeepalive.Nanoseconds())
}

func (tkr *targetKeepaliveRunner) doKeepalive() (stopped bool) {
	smap := tkr.t.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	if stopped = tkr.register(tkr.t.sendKeepalive, smap.Primary.ID(), tkr.t.si.Name()); stopped {
		tkr.t.onPrimaryProxyFailure()
	}
	return
}

func (pkr *proxyKeepaliveRunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	if !reflect.DeepEqual(oldConf.Keepalive.Proxy, newConf.Keepalive.Proxy) {
		pkr.kt = newKeepaliveTracker(newConf.Keepalive.Proxy)
		pkr.interval = newConf.Keepalive.Proxy.Interval
	}
	pkr.maxKeepaliveTime = float64(newConf.Timeout.MaxKeepalive.Nanoseconds())
}

func (pkr *proxyKeepaliveRunner) doKeepalive() (stopped bool) {
	smap := pkr.p.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	if smap.isPrimary(pkr.p.si) {
		return pkr.updateSmap()
	}
	if !pkr.isTimeToPing(smap.Primary.ID()) {
		return
	}

	if stopped = pkr.register(pkr.p.sendKeepalive, smap.Primary.ID(), pkr.p.si.Name()); stopped {
		pkr.p.onPrimaryProxyFailure()
	}
	return
}

// updateSmap pings all nodes in parallel. Non-responding nodes get removed from the Smap and
// the resulting map is then metasync-ed.
func (pkr *proxyKeepaliveRunner) updateSmap() (stopped bool) {
	if !pkr.primaryKeepaliveInProgress.CAS(0, 1) {
		glog.Infof("%s: primary keepalive is in progress...", pkr.p.si)
		return
	}
	defer pkr.primaryKeepaliveInProgress.CAS(1, 0)
	var (
		p         = pkr.p
		smap      = p.owner.smap.get()
		daemonCnt = smap.Count()
	)
	pkr.openCh(daemonCnt)
	// limit parallelism, here and elsewhere
	wg := cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), daemonCnt)
	for _, daemons := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for sid, si := range daemons {
			if sid == p.si.ID() {
				continue
			}
			// skipping
			if !pkr.isTimeToPing(sid) {
				continue
			}
			if daemons.InMaintenance(si) {
				continue
			}
			// pinging
			wg.Add(1)
			go func(si *cluster.Snode) {
				defer wg.Done()
				if len(pkr.stoppedCh) > 0 {
					return
				}
				ok, s, lat := pkr.ping(si)
				if s {
					pkr.stoppedCh <- struct{}{}
				}
				if !ok {
					pkr.toRemoveCh <- si.ID()
				}
				if lat != cmn.DefaultTimeout {
					pkr.latencyCh <- lat
				}
			}(si)
		}
	}
	wg.Wait()
	if stopped = len(pkr.stoppedCh) > 0; stopped {
		pkr.closeCh()
		return
	}
	pkr.statsMinMaxLat(pkr.latencyCh)
	if len(pkr.toRemoveCh) == 0 {
		return
	}
	ctx := &smapModifier{pre: pkr._pre, final: pkr._final}
	err := p.owner.smap.modify(ctx)
	if err != nil {
		if ctx.msg != nil {
			glog.Errorf("FATAL: %v", err)
		} else {
			glog.Warning(err)
		}
	}
	return
}

func (pkr *proxyKeepaliveRunner) openCh(daemonCnt int) {
	if pkr.stoppedCh == nil || cap(pkr.stoppedCh) < daemonCnt {
		pkr.stoppedCh = make(chan struct{}, daemonCnt*2)
		pkr.toRemoveCh = make(chan string, daemonCnt*2)
		pkr.latencyCh = make(chan time.Duration, daemonCnt*2)
	}
	debug.Assert(len(pkr.stoppedCh) == 0)
	debug.Assert(len(pkr.toRemoveCh) == 0)
	debug.Assert(len(pkr.latencyCh) == 0)
}

func (pkr *proxyKeepaliveRunner) closeCh() {
	close(pkr.stoppedCh)
	close(pkr.toRemoveCh)
	close(pkr.latencyCh)
	pkr.stoppedCh, pkr.toRemoveCh, pkr.latencyCh = nil, nil, nil
}

func (pkr *proxyKeepaliveRunner) _pre(ctx *smapModifier, clone *smapX) error {
	ctx.smap = pkr.p.owner.smap.get()
	if !ctx.smap.isPrimary(pkr.p.si) {
		return newErrNotPrimary(pkr.p.si, ctx.smap)
	}
	metaction := "keepalive: removing ["
	cnt := 0
loop:
	for {
		select {
		case sid := <-pkr.toRemoveCh:
			metaction += " ["
			if clone.GetProxy(sid) != nil {
				clone.delProxy(sid)
				clone.staffIC()
				metaction += cmn.Proxy
				cnt++
			} else if clone.GetTarget(sid) != nil {
				clone.delTarget(sid)
				metaction += cmn.Target
				cnt++
			} else {
				metaction += unknownDaemonID
				glog.Warningf("%s: %s not present in the %s (old %s)", pkr.p.si, sid, clone, ctx.smap)
			}
			metaction += ":" + sid + "] "

			// Remove reverse proxy entry for the node.
			pkr.p.rproxy.nodes.Delete(sid)
		default:
			break loop
		}
	}
	metaction += "]"
	if cnt == 0 {
		return fmt.Errorf("%s: nothing to do [%s, %s]", pkr.p.si, ctx.smap.StringEx(), metaction)
	}
	ctx.msg = &cmn.ActionMsg{Value: metaction}
	return nil
}

func (pkr *proxyKeepaliveRunner) _final(ctx *smapModifier, clone *smapX) {
	msg := pkr.p.newAmsg(ctx.msg, clone, nil)
	_ = pkr.p.metasyncer.sync(revsPair{clone, msg})
}

// min & max keepalive stats
func (pkr *proxyKeepaliveRunner) statsMinMaxLat(latencyCh chan time.Duration) {
	min, max := time.Hour, time.Duration(0)
loop:
	for {
		select {
		case lat := <-latencyCh:
			if min > lat && lat != 0 {
				min = lat
			}
			if max < lat {
				max = lat
			}
		default:
			break loop
		}
	}
	if min != time.Hour {
		pkr.statsT.Add(stats.KeepAliveMinLatency, int64(min))
	}
	if max != 0 {
		pkr.statsT.Add(stats.KeepAliveMaxLatency, int64(max))
	}
}

func (pkr *proxyKeepaliveRunner) ping(to *cluster.Snode) (ok, stopped bool, delta time.Duration) {
	var (
		timeout        = time.Duration(pkr.timeoutStatsForDaemon(to.ID()).timeout)
		t              = mono.NanoTime()
		_, status, err = pkr.p.Health(to, timeout, nil)
	)
	delta = mono.Since(t)
	pkr.updateTimeoutForDaemon(to.ID(), delta)
	pkr.statsT.Add(stats.KeepAliveLatency, int64(delta))

	if err == nil {
		return true, false, delta
	}

	glog.Warningf("initial keepalive failed, err: %v(%d) - retrying...", err, status)
	ok, stopped = pkr.retry(to)
	return ok, stopped, cmn.DefaultTimeout
}

func (pkr *proxyKeepaliveRunner) retry(si *cluster.Snode) (ok, stopped bool) {
	var (
		timeout = time.Duration(pkr.timeoutStatsForDaemon(si.ID()).timeout)
		ticker  = time.NewTicker(cmn.KeepaliveRetryDuration())
		i       int
	)
	defer ticker.Stop()
	for {
		if !pkr.isTimeToPing(si.ID()) {
			return true, false
		}
		select {
		case <-ticker.C:
			t := mono.NanoTime()
			_, status, err := pkr.p.Health(si, timeout, nil)
			timeout = pkr.updateTimeoutForDaemon(si.ID(), mono.Since(t))
			if err == nil {
				return true, false
			}
			i++
			if i == kaNumRetries {
				smap := pkr.p.owner.smap.get()
				glog.Warningf("%s: keepalive failed after %d attempts, removing %s from %s",
					pkr.p.si, i, si, smap)
				return false, false
			}
			if cmn.IsUnreachable(err, status) {
				continue
			}
			glog.Warningf("%s: keepalive: unexpected error %v(%d) from %s", pkr.p.si, err, status, si)
		case sig := <-pkr.controlCh:
			if sig.msg == kaStopMsg {
				return false, true
			}
		}
	}
}

func (k *keepalive) waitStatsRunner() (stopped bool) {
	const (
		waitStartupSleep = 300 * time.Millisecond
	)
	var (
		i      time.Duration
		ticker = time.NewTicker(waitStartupSleep)
	)

	defer ticker.Stop()

	// Wait for stats runner to start
	for {
		select {
		case <-ticker.C:
			if k.startedUp.Load() {
				return false
			}

			i += waitStartupSleep
			if i > cmn.GCO.Get().Timeout.Startup {
				glog.Errorln("startup is taking unusually long time...")
				i = 0
			}
		case sig := <-k.controlCh:
			switch sig.msg {
			case kaStopMsg:
				return true
			default:
			}
		}
	}
}

func (k *keepalive) Name() string { return k.name }

func (k *keepalive) Run() error {
	if k.waitStatsRunner() {
		// Stopped during waiting - must return.
		return nil
	}
	glog.Infof("Starting %s", k.Name())
	var (
		ticker    = time.NewTicker(k.interval)
		lastCheck int64
	)
	for {
		select {
		case <-ticker.C:
			lastCheck = mono.NanoTime()
			k.k.doKeepalive()
		case sig := <-k.controlCh:
			switch sig.msg {
			case kaRegisterMsg:
				ticker.Stop()
				ticker = time.NewTicker(k.interval)
			case kaUnregisterMsg:
				ticker.Stop()
			case kaStopMsg:
				ticker.Stop()
				return nil
			case kaErrorMsg:
				if mono.Since(lastCheck) >= cmn.KeepaliveRetryDuration() {
					lastCheck = mono.NanoTime()
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
func (k *keepalive) register(sendKeepalive func(time.Duration) (int, error), primaryProxyID, hname string) (stopped bool) {
	var (
		timeout     = time.Duration(k.timeoutStatsForDaemon(primaryProxyID).timeout)
		now         = mono.NanoTime()
		status, err = sendKeepalive(timeout)
		delta       = mono.Since(now)
	)

	k.statsT.Add(stats.KeepAliveLatency, int64(delta))
	if err == nil {
		return
	}
	glog.Warningf("%s => [%s]primary keepalive failed (err %v, status %d)", hname, primaryProxyID, err, status)
	var (
		ticker = time.NewTicker(cmn.KeepaliveRetryDuration())
		i      int
	)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			i++
			now = mono.NanoTime()
			status, err = sendKeepalive(timeout)
			delta := mono.Since(now)
			// In case the error is some kind of connection error, the round-trip
			// could be much shorter than the specified `timeout`. In such case
			// we want to report the worst-case scenario, otherwise we could possibly
			// decrease next retransmission timeout (which doesn't make much sense).
			if cmn.IsErrConnectionRefused(err) || cmn.IsErrConnectionReset(err) {
				delta = time.Duration(k.maxKeepaliveTime)
			}
			timeout = k.updateTimeoutForDaemon(primaryProxyID, delta)
			if err == nil {
				glog.Infof("%s: keepalive OK after %d attempt(s)", hname, i)
				return
			}
			if i == kaNumRetries {
				glog.Warningf("%s: keepalive failed after %d attempts, removing from Smap", hname, i)
				return true
			}
			if cmn.IsUnreachable(err, status) {
				continue
			}
			glog.Warningf("%s: unexpected response (err %v, status %d)", hname, err, status)
		case sig := <-k.controlCh:
			if sig.msg == kaStopMsg {
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
	ts.timeout = math.Max(ts.timeout, k.maxKeepaliveTime/2)
	return time.Duration(ts.timeout)
}

// timeoutStatsForDaemon returns the timeoutStats corresponding to daemon ID sid.
// If there is no entry in k.timeoutStats for sid, then the initial timeout will be set to
// maxKeepaliveNS, with the other stats loosely based on RFC 6298.
func (k *keepalive) timeoutStatsForDaemon(sid string) *timeoutStats {
	k.tt.mu.Lock()
	if ts := k.tt.timeoutStatsMap[sid]; ts != nil {
		k.tt.mu.Unlock()
		return ts
	}
	ts := &timeoutStats{srtt: k.maxKeepaliveTime, rttvar: k.maxKeepaliveTime / 2, timeout: k.maxKeepaliveTime}
	k.tt.timeoutStatsMap[sid] = ts
	k.tt.mu.Unlock()
	return ts
}

func (k *keepalive) onerr(err error, status int) {
	if cmn.IsUnreachable(err, status) {
		k.controlCh <- controlSignal{msg: kaErrorMsg, err: err}
	}
}

func (k *keepalive) heardFrom(sid string, reset bool) {
	k.kt.HeardFrom(sid, reset)
}

func (k *keepalive) isTimeToPing(sid string) bool {
	return k.kt.TimedOut(sid)
}

func (k *keepalive) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", k.Name(), err)
	k.controlCh <- controlSignal{msg: kaStopMsg}
	close(k.controlCh)
}

func (k *keepalive) send(msg string) {
	glog.Infof("Sending message: %s", msg)
	k.controlCh <- controlSignal{msg: msg}
}

//
// trackers
//

// interface guard
var (
	_ KeepaliveTracker = (*HeartBeatTracker)(nil)
	_ KeepaliveTracker = (*AverageTracker)(nil)
)

// HeartBeatTracker tracks the timestamp of the last time a message is received from a server.
// Timeout: a message is not received within the interval.
type HeartBeatTracker struct {
	mtx      sync.RWMutex
	last     map[string]int64
	interval time.Duration // expected to hear from the server within the interval
}

// NewKeepaliveTracker returns a keepalive tracker based on the parameters given.
func newKeepaliveTracker(c cmn.KeepaliveTrackerConf) KeepaliveTracker {
	switch c.Name {
	case cmn.KeepaliveHeartbeatType:
		return newHeartBeatTracker(c.Interval)
	case cmn.KeepaliveAverageType:
		return newAverageTracker(c.Factor)
	}
	return nil
}

// newHeartBeatTracker returns a HeartBeatTracker.
func newHeartBeatTracker(interval time.Duration) *HeartBeatTracker {
	return &HeartBeatTracker{
		last:     make(map[string]int64),
		interval: interval,
	}
}

func (hb *HeartBeatTracker) HeardFrom(id string, reset bool) {
	hb.mtx.Lock()
	hb.last[id] = mono.NanoTime()
	hb.mtx.Unlock()
}

func (hb *HeartBeatTracker) TimedOut(id string) bool {
	hb.mtx.RLock()
	t, ok := hb.last[id]
	hb.mtx.RUnlock()
	return !ok || mono.Since(t) > hb.interval
}

// AverageTracker keeps track of the average latency of all messages.
// Timeout: last received is more than the 'factor' of current average.
type AverageTracker struct {
	mtx    sync.RWMutex
	rec    map[string]averageTrackerRecord
	factor uint8
}

type averageTrackerRecord struct {
	count   int64
	last    int64
	totalMS int64 // in ms
}

func (rec *averageTrackerRecord) avg() int64 {
	return rec.totalMS / rec.count
}

// newAverageTracker returns an AverageTracker.
func newAverageTracker(factor uint8) *AverageTracker {
	return &AverageTracker{
		rec:    make(map[string]averageTrackerRecord),
		factor: factor,
	}
}

// HeardFrom is called to indicate a keepalive message (or equivalent) has been received from a server.
func (a *AverageTracker) HeardFrom(id string, reset bool) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	var rec averageTrackerRecord
	rec, ok := a.rec[id]
	if reset || !ok {
		a.rec[id] = averageTrackerRecord{count: 0, totalMS: 0, last: mono.NanoTime()}
		return
	}

	t := mono.NanoTime()
	delta := t - rec.last
	rec.last = t
	rec.count++
	rec.totalMS += delta / int64(time.Millisecond)
	a.rec[id] = rec
}

func (a *AverageTracker) TimedOut(id string) bool {
	a.mtx.RLock()
	rec, ok := a.rec[id]
	a.mtx.RUnlock()

	if !ok {
		return true
	}
	if rec.count == 0 {
		return false
	}
	return int64(mono.Since(rec.last)/time.Millisecond) > int64(a.factor)*rec.avg()
}
