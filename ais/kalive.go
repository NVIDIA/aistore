// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/stats"
)

const (
	kaErrorMsg   = "error"
	kaStopMsg    = "stop"
	kaResumeMsg  = "resume"
	kaSuspendMsg = "suspend"

	kaNumRetries = 3
)

const (
	waitSelfJoin = 300 * time.Millisecond
	waitStandby  = 5 * time.Second
)

type (
	keepaliver interface {
		sendKalive(*smapX, time.Duration) (string, int, error)
		onerr(err error, status int)
		heardFrom(sid string, reset bool)
		do() (stopped bool)
		isTimeToPing(sid string) bool
		ctrl(msg string)
		paused() bool
		cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf
	}
	talive struct {
		t *target
		keepalive
	}
	palive struct {
		p          *proxy
		stoppedCh  chan struct{}
		toRemoveCh chan string
		keepalive
	}
	keepalive struct {
		k            keepaliver
		kt           KeepaliveTracker
		statsT       stats.Tracker
		tt           *timeoutTracker
		controlCh    chan controlSignal
		startedUp    *atomic.Bool
		name         string
		inProgress   atomic.Int64 // toggle used only by the primary
		maxKeepalive int64
		interval     time.Duration
		tickerPaused atomic.Bool
	}
	timeoutTracker struct {
		timeoutStats map[string]*timeoutStats
		mu           sync.Mutex
	}
	timeoutStats struct {
		srtt    int64 // smoothed round-trip time in ns
		rttvar  int64 // round-trip time variation in ns
		timeout int64 // in ns
	}
	controlSignal struct {
		err error
		msg string
	}

	KeepaliveTracker interface {
		// HeardFrom notifies tracker that the server identified by 'id' has responded;
		// 'reset'=true when it is not a regular keepalive call (could be reconnect or re-register).
		HeardFrom(id string, reset bool)
		// TimedOut returns true if the 'id` server did not respond - an indication that the server
		// could be down
		TimedOut(id string) bool

		changed(factor uint8, interval time.Duration) bool
	}
)

// interface guard
var (
	_ cos.Runner = (*talive)(nil)
	_ cos.Runner = (*palive)(nil)

	_ keepaliver = (*talive)(nil)
	_ keepaliver = (*palive)(nil)

	_ KeepaliveTracker = (*HBTracker)(nil)
	_ KeepaliveTracker = (*AvgTracker)(nil)
)

////////////
// talive //
////////////

func newTalive(t *target, statsT stats.Tracker, startedUp *atomic.Bool) *talive {
	config := cmn.GCO.Get()

	tkr := &talive{t: t}
	tkr.keepalive.name = "talive"
	tkr.keepalive.k = tkr
	tkr.statsT = statsT
	tkr.keepalive.startedUp = startedUp
	tkr.kt = newKeepaliveTracker(&config.Keepalive.Target)
	tkr.tt = &timeoutTracker{timeoutStats: make(map[string]*timeoutStats, 8)}
	tkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	tkr.interval = config.Keepalive.Target.Interval.D()
	tkr.maxKeepalive = int64(config.Timeout.MaxKeepalive)
	return tkr
}

func (*talive) cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf {
	return &config.Keepalive.Target
}

func (tkr *talive) sendKalive(smap *smapX, timeout time.Duration) (string, int, error) {
	return tkr.t.sendKalive(smap, tkr.t, timeout)
}

func (tkr *talive) do() (stopped bool) {
	smap := tkr.t.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	if stopped = tkr.keepalive.do(smap, tkr.t.si); stopped {
		tkr.t.onPrimaryFail(nil /*proxy*/)
	}
	return
}

////////////
// palive //
////////////

func newPalive(p *proxy, statsT stats.Tracker, startedUp *atomic.Bool) *palive {
	config := cmn.GCO.Get()

	pkr := &palive{p: p}
	pkr.keepalive.name = "palive"
	pkr.keepalive.k = pkr
	pkr.statsT = statsT
	pkr.keepalive.startedUp = startedUp
	pkr.kt = newKeepaliveTracker(&config.Keepalive.Proxy)
	pkr.tt = &timeoutTracker{timeoutStats: make(map[string]*timeoutStats, 8)}
	pkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	pkr.interval = config.Keepalive.Proxy.Interval.D()
	pkr.maxKeepalive = int64(config.Timeout.MaxKeepalive)
	return pkr
}

func (*palive) cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf {
	return &config.Keepalive.Proxy
}

func (pkr *palive) sendKalive(smap *smapX, timeout time.Duration) (pid string, status int, err error) {
	if smap == nil {
		smap = pkr.p.owner.smap.get()
		if smap == nil {
			return
		}
	}
	pid = smap.Primary.ID()
	if smap.isPrimary(pkr.p.si) && smap.version() > 0 {
		return
	}
	return pkr.p.htrun.sendKalive(smap, nil /*htext*/, timeout)
}

func (pkr *palive) do() (stopped bool) {
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
	if stopped = pkr.keepalive.do(smap, pkr.p.si); stopped {
		pkr.p.onPrimaryFail(pkr.p /*self*/)
	}
	return
}

// updateSmap pings all nodes in parallel. Non-responding nodes get removed from the Smap and
// the resulting map is then metasync-ed.
func (pkr *palive) updateSmap() (stopped bool) {
	if !pkr.inProgress.CAS(0, 1) {
		glog.Infof("%s: primary keepalive is in progress...", pkr.p)
		return
	}
	defer pkr.inProgress.CAS(1, 0)
	var (
		p         = pkr.p
		smap      = p.owner.smap.get()
		daemonCnt = smap.Count()
	)
	pkr.openCh(daemonCnt)
	// limit parallelism, here and elsewhere
	wg := cos.NewLimitedWaitGroup(meta.MaxBcastParallel(), daemonCnt)
	for _, daemons := range []meta.NodeMap{smap.Tmap, smap.Pmap} {
		for sid, si := range daemons {
			if sid == p.SID() {
				continue
			}
			// skipping
			if !pkr.isTimeToPing(sid) {
				continue
			}
			// NOTE in re maintenance-mode nodes:
			// for future activation, passively (ie, no keepalives) keeping them in the cluster map -
			// use apc.ActRmNodeUnsafe to remove, if need be
			if si.InMaintOrDecomm() {
				continue
			}
			// do keepalive
			wg.Add(1)
			go pkr.ping(si, wg)
		}
	}
	wg.Wait()
	if stopped = len(pkr.stoppedCh) > 0; stopped {
		pkr.closeCh()
		return
	}
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

func (pkr *palive) ping(si *meta.Snode, wg cos.WG) {
	defer wg.Done()
	if len(pkr.stoppedCh) > 0 {
		return
	}
	ok, stopped := pkr._pingRetry(si)
	if stopped {
		pkr.stoppedCh <- struct{}{}
	}
	if !ok {
		pkr.toRemoveCh <- si.ID()
	}
}

func (pkr *palive) _pingRetry(to *meta.Snode) (ok, stopped bool) {
	var (
		timeout        = time.Duration(pkr.timeoutStats(to.ID()).timeout)
		t              = mono.NanoTime()
		_, status, err = pkr.p.Health(to, timeout, nil)
	)
	delta := mono.Since(t)
	pkr.updateTimeoutFor(to.ID(), delta)
	pkr.statsT.Add(stats.KeepAliveLatency, int64(delta))

	if err == nil {
		return true, false
	}
	glog.Warningf("%s fails to respond, err: %v(%d) - retrying...", to.StringEx(), err, status)
	ok, stopped = pkr.retry(to)
	return ok, stopped
}

func (pkr *palive) openCh(daemonCnt int) {
	if pkr.stoppedCh == nil || cap(pkr.stoppedCh) < daemonCnt {
		pkr.stoppedCh = make(chan struct{}, daemonCnt*2)
		pkr.toRemoveCh = make(chan string, daemonCnt*2)
	}
	debug.Assert(len(pkr.stoppedCh) == 0)
	debug.Assert(len(pkr.toRemoveCh) == 0)
}

func (pkr *palive) closeCh() {
	close(pkr.stoppedCh)
	close(pkr.toRemoveCh)
	pkr.stoppedCh, pkr.toRemoveCh = nil, nil
}

func (pkr *palive) _pre(ctx *smapModifier, clone *smapX) error {
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
				metaction += apc.Proxy
				cnt++
			} else if clone.GetTarget(sid) != nil {
				clone.delTarget(sid)
				metaction += apc.Target
				cnt++
			} else {
				metaction += unknownDaemonID
				glog.Warningf("%s not present in the %s (old %s)", sid, clone, ctx.smap)
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
	ctx.msg = &apc.ActMsg{Value: metaction}
	return nil
}

func (pkr *palive) _final(ctx *smapModifier, clone *smapX) {
	msg := pkr.p.newAmsg(ctx.msg, nil)
	debug.Assert(clone._sgl != nil)
	_ = pkr.p.metasyncer.sync(revsPair{clone, msg})
}

func (pkr *palive) retry(si *meta.Snode) (ok, stopped bool) {
	var (
		timeout = time.Duration(pkr.timeoutStats(si.ID()).timeout)
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
			timeout = pkr.updateTimeoutFor(si.ID(), mono.Since(t))
			if err == nil {
				return true, false
			}
			i++
			if i == kaNumRetries {
				smap := pkr.p.owner.smap.get()
				sname := si.StringEx()
				glog.Warningf("Failed to keepalive %s after %d attempts - removing %s from the %s",
					sname, i, sname, smap)
				return false, false
			}
			if cos.IsUnreachable(err, status) {
				continue
			}
			glog.Warningf("Unexpected error %v(%d) from %s", err, status, si.StringEx())
		case sig := <-pkr.controlCh:
			if sig.msg == kaStopMsg {
				return false, true
			}
		}
	}
}

///////////////
// keepalive //
///////////////

func (k *keepalive) Name() string { return k.name }

func (k *keepalive) waitStatsRunner() (stopped bool) {
	var (
		logErr time.Duration
		ticker *time.Ticker
	)
	if daemon.cli.target.standby {
		ticker = time.NewTicker(waitStandby)
	} else {
		ticker = time.NewTicker(waitSelfJoin)
	}
	defer ticker.Stop()

	// Wait for stats runner to start
	for {
		select {
		case <-ticker.C:
			if k.startedUp.Load() {
				return false
			}
			logErr += waitSelfJoin
			config := cmn.GCO.Get()
			if logErr > config.Timeout.Startup.D() {
				glog.Errorln("startup is taking unusually long time...")
				logErr = 0
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

func (k *keepalive) Run() error {
	if k.waitStatsRunner() {
		return nil // Stopped while waiting - must exit.
	}
	glog.Infof("Starting %s", k.Name())
	var (
		ticker    = time.NewTicker(k.interval)
		lastCheck int64
	)
	k.tickerPaused.Store(false)
	for {
		select {
		case <-ticker.C:
			lastCheck = mono.NanoTime()
			k.k.do()
			config := cmn.GCO.Get()
			k.configUpdate(config.Timeout.MaxKeepalive.D(), k.k.cfg(config))
		case sig := <-k.controlCh:
			switch sig.msg {
			case kaResumeMsg:
				if k.tickerPaused.CAS(true, false) {
					ticker.Reset(k.interval)
				}
			case kaSuspendMsg:
				if k.tickerPaused.CAS(false, true) {
					ticker.Stop()
				}
			case kaStopMsg:
				ticker.Stop()
				return nil
			case kaErrorMsg:
				if mono.Since(lastCheck) >= cmn.KeepaliveRetryDuration() {
					lastCheck = mono.NanoTime()
					glog.Infof("triggered by %v", sig.err)
					if stopped := k.k.do(); stopped {
						ticker.Stop()
						return nil
					}
				}
			}
		}
	}
}

func (k *keepalive) configUpdate(maxKeepalive time.Duration, cfg *cmn.KeepaliveTrackerConf) {
	k.maxKeepalive = int64(maxKeepalive)
	if !k.kt.changed(cfg.Factor, cfg.Interval.D()) {
		return
	}
	k.interval = cfg.Interval.D()
	k.kt = newKeepaliveTracker(cfg)
}

// is called by non-primary proxies and (all) targets to send keepalive req. to the primary
func (k *keepalive) do(smap *smapX, si *meta.Snode) (stopped bool) {
	var (
		pid     = smap.Primary.ID()
		timeout = time.Duration(k.timeoutStats(pid).timeout)
		now     = mono.NanoTime()
	)
	cpid, status, err := k.k.sendKalive(smap, timeout)
	k.statsT.Add(stats.KeepAliveLatency, mono.SinceNano(now))
	if err == nil {
		return
	}
	if daemon.stopping.Load() {
		return
	}
	debug.Assert(cpid == pid && cpid != si.ID(), pid+", "+cpid+", "+si.ID())
	glog.Warningf("%s => %s keepalive failed: %v(%d)", si, meta.Pname(pid), err, status)

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
			pid, status, err = k.k.sendKalive(nil, timeout)
			if pid == si.ID() {
				return // elected as primary
			}
			delta := mono.Since(now)
			// In case the error is some kind of connection error, the round-trip
			// could be much shorter than the specified `timeout`. In such case
			// we want to report the worst-case scenario, otherwise we could possibly
			// decrease next retransmission timeout (which doesn't make much sense).
			if cos.IsRetriableConnErr(err) {
				delta = time.Duration(k.maxKeepalive)
			}
			timeout = k.updateTimeoutFor(pid, delta)
			if err == nil {
				glog.Infof("%s: OK after %d attempt%s", si, i, cos.Plural(i))
				return
			}
			if i == kaNumRetries {
				glog.Warningf("%s: failed to keepalive with %s after %d attempts", si, meta.Pname(pid), i)
				return true
			}
			if cos.IsUnreachable(err, status) {
				continue
			}
			if daemon.stopping.Load() {
				return true
			}
			err = fmt.Errorf("%s: unexpected response from %s: %v(%d)", si, meta.Pname(pid), err, status)
			debug.AssertNoErr(err)
			glog.Warning(err)
		case sig := <-k.controlCh:
			if sig.msg == kaStopMsg {
				return true
			}
		}
	}
}

// updateTimeoutForDaemon calculates the new timeout for the daemon with ID sid, updates it in
// k.timeoutStats, and returns it. The algorithm is loosely based on TCP's RTO calculation,
// as documented in RFC 6298.
func (k *keepalive) updateTimeoutFor(sid string, d time.Duration) time.Duration {
	const (
		alpha = 125
		beta  = 250
		c     = 4
	)
	next := int64(d)
	ts := k.timeoutStats(sid)
	ts.rttvar = (1000-beta)*ts.rttvar + beta*(cos.AbsI64(ts.srtt-next))
	ts.rttvar = cos.DivRound(ts.rttvar, 1000)
	ts.srtt = (1000-alpha)*ts.srtt + alpha*next
	ts.srtt = cos.DivRound(ts.srtt, 1000)
	ts.timeout = cos.MinI64(k.maxKeepalive, ts.srtt+c*ts.rttvar)
	ts.timeout = cos.MaxI64(ts.timeout, k.maxKeepalive/2)
	return time.Duration(ts.timeout)
}

// timeoutStats returns the timeoutStats corresponding to daemon ID sid.
// If there is no entry in k.timeoutStats for sid, then the initial timeout will be set to
// maxKeepaliveNS, with the other stats loosely based on RFC 6298.
func (k *keepalive) timeoutStats(sid string) *timeoutStats {
	k.tt.mu.Lock()
	if ts := k.tt.timeoutStats[sid]; ts != nil {
		k.tt.mu.Unlock()
		return ts
	}
	ts := &timeoutStats{srtt: k.maxKeepalive, rttvar: k.maxKeepalive / 2, timeout: k.maxKeepalive}
	k.tt.timeoutStats[sid] = ts
	k.tt.mu.Unlock()
	return ts
}

func (k *keepalive) onerr(err error, status int) {
	if cos.IsUnreachable(err, status) {
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

func (k *keepalive) ctrl(msg string) {
	glog.Infof("Sending %q on the control channel", msg)
	k.controlCh <- controlSignal{msg: msg}
}

func (k *keepalive) paused() bool { return k.tickerPaused.Load() }

///////////////
// HBTracker //
///////////////

// HBTracker tracks the timestamp of the last time a message is received from a server.
// Timeout: a message is not received within the `interval`.
type HBTracker struct {
	last     map[string]int64
	interval time.Duration
	mtx      sync.RWMutex
}

// NewKeepaliveTracker returns a keepalive tracker based on the parameters given.
func newKeepaliveTracker(c *cmn.KeepaliveTrackerConf) KeepaliveTracker {
	switch c.Name {
	case cmn.KeepaliveHeartbeatType:
		return newHBTracker(c.Interval.D())
	case cmn.KeepaliveAverageType:
		return newAvgTracker(c.Factor)
	}
	return nil
}

// newHBTracker returns a HBTracker.
func newHBTracker(interval time.Duration) *HBTracker {
	return &HBTracker{
		last:     make(map[string]int64),
		interval: interval,
	}
}

func (hb *HBTracker) HeardFrom(id string, _ bool) {
	hb.mtx.Lock()
	hb.last[id] = mono.NanoTime()
	hb.mtx.Unlock()
}

func (hb *HBTracker) TimedOut(id string) bool {
	hb.mtx.RLock()
	t, ok := hb.last[id]
	hb.mtx.RUnlock()
	return !ok || mono.Since(t) > hb.interval
}

func (hb *HBTracker) changed(_ uint8, interval time.Duration) bool {
	return hb.interval != interval
}

////////////////
// AvgTracker //
////////////////

// AvgTracker keeps track of the average latency of all messages.
// Timeout: last received is more than the 'factor' of current average.
type (
	AvgTracker struct {
		rec    map[string]avgTrackerRec
		mtx    sync.RWMutex
		factor uint8
	}
	avgTrackerRec struct {
		count   int64
		last    int64
		totalMS int64 // in ms
	}
)

func (rec *avgTrackerRec) avg() int64 {
	return rec.totalMS / rec.count
}

// newAvgTracker returns an AvgTracker.
func newAvgTracker(factor uint8) *AvgTracker {
	return &AvgTracker{rec: make(map[string]avgTrackerRec), factor: factor}
}

// HeardFrom is called to indicate that a keepalive message (or equivalent) has been received.
func (a *AvgTracker) HeardFrom(id string, reset bool) {
	var rec avgTrackerRec
	a.mtx.Lock()
	defer a.mtx.Unlock()
	rec, ok := a.rec[id]
	if reset || !ok {
		a.rec[id] = avgTrackerRec{count: 0, totalMS: 0, last: mono.NanoTime()}
		return
	}
	t := mono.NanoTime()
	delta := t - rec.last
	rec.last = t
	rec.count++
	rec.totalMS += delta / int64(time.Millisecond)
	a.rec[id] = rec
}

func (a *AvgTracker) TimedOut(id string) bool {
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

func (a *AvgTracker) changed(factor uint8, _ time.Duration) bool {
	return a.factor != factor
}
