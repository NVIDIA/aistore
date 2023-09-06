// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		heardFrom(sid string)
		do() (stopped bool)
		timeToPing(sid string) bool
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
		hb           hbTracker
		statsT       stats.Tracker
		tosts        sync.Map
		controlCh    chan controlSignal
		startedUp    *atomic.Bool
		name         string
		maxKeepalive int64
		interval     time.Duration
		inProgress   atomic.Bool
		tickerPaused atomic.Bool
	}
	tost struct {
		srtt    int64 // smoothed round-trip time in ns
		rttvar  int64 // round-trip time variation in ns
		timeout int64 // in ns
	}
	controlSignal struct {
		err error
		msg string
	}

	hbTracker interface {
		HeardFrom(id string)     // callback for 'id' to respond
		TimedOut(id string) bool // returns true if 'id` did not respond within expected interval

		reg(id string)
		set(interval time.Duration) bool
	}
	heartBeat struct {
		last     sync.Map
		interval time.Duration // timeout
	}
)

// interface guard
var (
	_ cos.Runner = (*talive)(nil)
	_ cos.Runner = (*palive)(nil)

	_ keepaliver = (*talive)(nil)
	_ keepaliver = (*palive)(nil)

	_ hbTracker = (*heartBeat)(nil)
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
	tkr.hb = newHB(config.Keepalive.Target.Interval.D())
	tkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	tkr.interval = config.Keepalive.Target.Interval.D()
	tkr.maxKeepalive = int64(config.Timeout.MaxKeepalive)
	return tkr
}

func (tkr *talive) Run() error {
	if stopped := tkr.wait(); stopped {
		return nil
	}

	tkr.init(tkr.t.owner.smap.get(), tkr.t.SID())

	nlog.Infof("Starting %s", tkr.Name())
	tkr._run()
	return nil
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
		tkr.t.onPrimaryDown(nil /*proxy*/, "")
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
	pkr.hb = newHB(config.Keepalive.Proxy.Interval.D())
	pkr.controlCh = make(chan controlSignal) // unbuffered on purpose
	pkr.interval = config.Keepalive.Proxy.Interval.D()
	pkr.maxKeepalive = int64(config.Timeout.MaxKeepalive)
	return pkr
}

func (pkr *palive) Run() error {
	if stopped := pkr.wait(); stopped {
		return nil
	}

	pkr.init(pkr.p.owner.smap.get(), pkr.p.SID())

	nlog.Infof("Starting %s", pkr.Name())
	pkr._run()
	return nil
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
		if !pkr.inProgress.CAS(false, true) {
			nlog.Infoln(pkr.p.String() + ": primary keepalive in progress")
			return
		}
		stopped = pkr.updateSmap()
		pkr.inProgress.Store(false)
		return
	}
	if !pkr.timeToPing(smap.Primary.ID()) {
		return
	}
	if stopped = pkr.keepalive.do(smap, pkr.p.si); stopped {
		pkr.p.onPrimaryDown(pkr.p /*self*/, "")
	}
	return
}

// updateSmap pings all nodes in parallel. Non-responding nodes get removed from the Smap and
// the resulting map is then metasync-ed.
func (pkr *palive) updateSmap() (stopped bool) {
	var (
		p    = pkr.p
		smap = p.owner.smap.get()
		cnt  = smap.Count()
	)
	pkr.openCh(cnt)
	wg := cos.NewLimitedWaitGroup(meta.MaxBcastParallel(), cnt) // limit parallelism
	for _, nm := range []meta.NodeMap{smap.Tmap, smap.Pmap} {
		for sid, si := range nm {
			if sid == p.SID() {
				continue
			}
			// skipping
			if !pkr.timeToPing(sid) {
				continue
			}
			// in re maintenance-mode nodes:
			// for future activation, passively (ie, no keepalives) keeping them in the cluster map -
			// use apc.ActRmNodeUnsafe to remove, if need be
			if si.InMaintOrDecomm() {
				continue
			}
			// do keepalive
			wg.Add(1)
			go pkr.ping(si, wg, smap)
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
			nlog.Errorf("FATAL: %v", err)
		} else {
			nlog.Warningln(err)
		}
	}
	return
}

func (pkr *palive) ping(si *meta.Snode, wg cos.WG, smap *smapX) {
	defer wg.Done()
	if len(pkr.stoppedCh) > 0 {
		return
	}
	ok, stopped := pkr._pingRetry(si, smap)
	if stopped {
		pkr.stoppedCh <- struct{}{}
	}
	if !ok {
		pkr.toRemoveCh <- si.ID()
	}
}

func (pkr *palive) _pingRetry(to *meta.Snode, smap *smapX) (ok, stopped bool) {
	var (
		timeout        = time.Duration(pkr.tost(to.ID()).timeout)
		t              = mono.NanoTime()
		_, status, err = pkr.p.reqHealth(to, timeout, nil, smap)
	)
	delta := mono.Since(t)
	pkr.updTimeout(to.ID(), delta)
	pkr.statsT.Add(stats.KeepAliveLatency, int64(delta))

	if err == nil {
		return true, false
	}

	nlog.Warningf("%s fails to respond: [%v(%d)] - retrying...", to.StringEx(), err, status)
	ticker := time.NewTicker(cmn.KeepaliveRetryDuration())
	ok, stopped = pkr.retry(to, ticker)
	ticker.Stop()

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
				nlog.Warningf("%s not present in the %s (old %s)", sid, clone, ctx.smap)
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

func (pkr *palive) retry(si *meta.Snode, ticker *time.Ticker) (ok, stopped bool) {
	var (
		timeout = time.Duration(pkr.tost(si.ID()).timeout)
		i       int
	)
	for {
		if !pkr.timeToPing(si.ID()) {
			return true, false
		}
		select {
		case <-ticker.C:
			now := mono.NanoTime()
			smap := pkr.p.owner.smap.get()
			_, status, err := pkr.p.reqHealth(si, timeout, nil, smap)
			timeout = pkr.updTimeout(si.ID(), mono.Since(now))
			if err == nil {
				return true, false
			}
			i++
			if i == kaNumRetries {
				sname := si.StringEx()
				nlog.Warningf("Failed to keepalive %s after %d attempts - removing %s from the %s",
					sname, i, sname, smap)
				return false, false
			}
			if cos.IsUnreachable(err, status) {
				continue
			}
			nlog.Warningf("Unexpected error %v(%d) from %s", err, status, si.StringEx())
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

// wait for stats-runner to set startedUp=true
func (k *keepalive) wait() (stopped bool) {
	var ticker *time.Ticker
	if daemon.cli.target.standby {
		ticker = time.NewTicker(waitStandby)
	} else {
		ticker = time.NewTicker(waitSelfJoin)
	}
	stopped = k._wait(ticker)
	ticker.Stop()
	return
}

func (k *keepalive) _wait(ticker *time.Ticker) (stopped bool) {
	for {
		select {
		case <-ticker.C:
			if k.startedUp.Load() { // i.e., `statsRunner.startedUp`
				return false
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

// pre-populate hb
func (k *keepalive) init(smap *smapX, self string) {
	for _, nm := range []meta.NodeMap{smap.Pmap, smap.Tmap} {
		for sid := range nm {
			if sid == self {
				continue
			}
			k.tosts.Store(sid, k.newTost())
			k.hb.reg(sid)
		}
	}
}

func (k *keepalive) _run() {
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
				return
			case kaErrorMsg:
				if mono.Since(lastCheck) >= cmn.KeepaliveRetryDuration() {
					lastCheck = mono.NanoTime()
					nlog.Infof("triggered by %v", sig.err)
					if stopped := k.k.do(); stopped {
						ticker.Stop()
						return
					}
				}
			}
		}
	}
}

func (k *keepalive) configUpdate(maxKeepalive time.Duration, cfg *cmn.KeepaliveTrackerConf) {
	k.maxKeepalive = int64(maxKeepalive)
	if k.hb.set(cfg.Interval.D()) {
		k.interval = cfg.Interval.D()
	}
}

// is called by non-primary proxies and (all) targets to send keepalive req. to the primary
func (k *keepalive) do(smap *smapX, si *meta.Snode) (stopped bool) {
	var (
		pid     = smap.Primary.ID()
		timeout = time.Duration(k.tost(pid).timeout)
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
	nlog.Warningf("%s => %s keepalive failed: %v(%d)", si, meta.Pname(pid), err, status)

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
			timeout = k.updTimeout(pid, delta)
			if err == nil {
				nlog.Infof("%s: OK after %d attempt%s", si, i, cos.Plural(i))
				return
			}
			if i == kaNumRetries {
				nlog.Warningf("%s: failed to keepalive with %s after %d attempts", si, meta.Pname(pid), i)
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
			nlog.Warningln(err)
		case sig := <-k.controlCh:
			if sig.msg == kaStopMsg {
				return true
			}
		}
	}
}

// update timeout for a node
// the algorithm is based on TCP's RTO (RFC 6298)
func (k *keepalive) updTimeout(sid string, d time.Duration) time.Duration {
	const (
		alpha = 125
		beta  = 250
		c     = 4
	)
	next := int64(d)
	ts := k.tost(sid)
	ts.rttvar = (1000-beta)*ts.rttvar + beta*(cos.AbsI64(ts.srtt-next))
	ts.rttvar = cos.DivRound(ts.rttvar, 1000)
	ts.srtt = (1000-alpha)*ts.srtt + alpha*next
	ts.srtt = cos.DivRound(ts.srtt, 1000)
	ts.timeout = cos.MinI64(k.maxKeepalive, ts.srtt+c*ts.rttvar)
	ts.timeout = cos.MaxI64(ts.timeout, k.maxKeepalive/2)
	return time.Duration(ts.timeout)
}

// returns timeout stats for a node; if there's no entry, initial timeout
// is set to maxKeepalive with other counters loosely based on RFC 6298
func (k *keepalive) tost(sid string) (ts *tost) {
	val, ok := k.tosts.Load(sid)
	if ok {
		ts = val.(*tost) // almost always
	} else {
		ts = k.newTost()
		k.tosts.Store(sid, ts)
	}
	return
}

func (k *keepalive) newTost() *tost {
	return &tost{srtt: k.maxKeepalive, rttvar: k.maxKeepalive / 2, timeout: k.maxKeepalive}
}

func (k *keepalive) onerr(err error, status int) {
	if cos.IsUnreachable(err, status) {
		k.controlCh <- controlSignal{msg: kaErrorMsg, err: err}
	}
}

func (k *keepalive) heardFrom(sid string) {
	k.hb.HeardFrom(sid)
}

func (k *keepalive) timeToPing(sid string) bool {
	return k.hb.TimedOut(sid)
}

func (k *keepalive) Stop(err error) {
	nlog.Infof("Stopping %s, err: %v", k.Name(), err)
	k.controlCh <- controlSignal{msg: kaStopMsg}
	close(k.controlCh)
}

func (k *keepalive) ctrl(msg string) {
	nlog.Infof("Sending %q on the control channel", msg)
	k.controlCh <- controlSignal{msg: msg}
}

func (k *keepalive) paused() bool { return k.tickerPaused.Load() }

///////////////
// heartBeat //
///////////////

func newHB(interval time.Duration) *heartBeat { return &heartBeat{interval: interval} }

func (hb *heartBeat) HeardFrom(id string) {
	var (
		val   *int64
		now   = mono.NanoTime()
		v, ok = hb.last.Load(id)
	)
	if ok {
		val = v.(*int64) // almost always
	} else {
		val = new(int64)
		hb.last.Store(id, val)
	}
	*val = now // NOTE: not using ratomic.StoreInt64(val, now)
}

func (hb *heartBeat) TimedOut(id string) bool {
	v, ok := hb.last.Load(id)
	if !ok {
		return true
	}
	val := v.(*int64)
	tim := *val
	return mono.Since(tim) > hb.interval
}

func (hb *heartBeat) reg(id string) { hb.last.Store(id, new(int64)) }

func (hb *heartBeat) set(interval time.Duration) (changed bool) {
	changed = hb.interval != interval
	hb.interval = interval
	return
}
