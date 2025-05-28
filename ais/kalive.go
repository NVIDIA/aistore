// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
)

const (
	kaErrorMsg   = "error"
	kaStopMsg    = "stop"
	kaResumeMsg  = "resume"
	kaSuspendMsg = "suspend"
)

const (
	waitSelfJoin = 300 * time.Millisecond
	waitStandby  = 5 * time.Second
)

type (
	keepaliver interface {
		sendKalive(*smapX, time.Duration, int64 /*now*/, bool) (string, int, error)
		heardFrom(sid string) int64
		do(config *cmn.Config) (stopped bool)
		timeToPing(sid string) bool
		ctrl(msg string)
		paused() bool
		cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf
		cluUptime(int64) time.Duration
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
		controlCh    chan controlSignal
		startedUp    *atomic.Bool
		name         string
		interval     time.Duration // config.Keepalive.Target.Interval or config.Keepalive.Proxy.Interval (10s)
		inProgress   atomic.Bool
		tickerPaused atomic.Bool
	}
	controlSignal struct {
		err error
		msg string
	}

	hbTracker interface {
		HeardFrom(id string, now int64) int64 // callback for 'id' to respond
		TimedOut(id string) bool              // true if 'id` didn't keepalive or called (via "heard") within the interval (above)

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
	tkr.keepalive.init(tkr, statsT, startedUp, "talive", config.Keepalive.Target.Interval.D())
	return tkr
}

func (tkr *talive) Run() error {
	if stopped := tkr.wait(); stopped {
		return nil
	}

	tkr.prepopulate(tkr.t.owner.smap.get(), tkr.t.SID())

	nlog.Infoln("Starting", tkr.Name())
	tkr._run()
	return nil
}

func (*talive) cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf {
	return &config.Keepalive.Target
}

func (tkr *talive) cluUptime(now int64) (elapsed time.Duration) {
	if at := tkr.t.startup.cluster.Load(); at > 0 {
		elapsed = time.Duration(now - at)
	}
	return
}

func (tkr *talive) sendKalive(smap *smapX, timeout time.Duration, _ int64, fast bool) (pid string, status int, err error) {
	if fast {
		// additionally
		interrupted, restarted := tkr.t.interruptedRestarted()
		fast = !interrupted && !restarted
	}
	if fast {
		debug.Assert(ec.ECM != nil)
		pid, _, err = tkr.t.fastKalive(smap, timeout, ec.ECM.IsActive())
		return pid, 0, err
	}
	return tkr.t.slowKalive(smap, tkr.t, timeout)
}

func (tkr *talive) do(config *cmn.Config) (stopped bool) {
	smap := tkr.t.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return
	}
	if !tkr.timeToPing(smap.Primary.ID()) { // skip sending keepalive
		return
	}
	if stopped = tkr.keepalive.do(smap, tkr.t.si, config); stopped {
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
	pkr.keepalive.init(pkr, statsT, startedUp, "palive", config.Keepalive.Proxy.Interval.D())
	return pkr
}

func (pkr *palive) Run() error {
	if stopped := pkr.wait(); stopped {
		return nil
	}

	pkr.prepopulate(pkr.p.owner.smap.get(), pkr.p.SID())

	nlog.Infoln("Starting", pkr.Name())
	pkr._run()
	return nil
}

func (*palive) cfg(config *cmn.Config) *cmn.KeepaliveTrackerConf {
	return &config.Keepalive.Proxy
}

func (pkr *palive) cluUptime(now int64) (elapsed time.Duration) {
	if at := pkr.p.startup.cluster.Load(); at > 0 {
		elapsed = time.Duration(now - at)
	}
	return
}

func (pkr *palive) sendKalive(smap *smapX, timeout time.Duration, now int64, fast bool) (string, int, error) {
	debug.Assert(!smap.isPrimary(pkr.p.si))

	if fast {
		pid, hdr, err := pkr.p.fastKalive(smap, timeout, false /*ec active*/)
		if err == nil {
			// (shared streams; EC streams)
			if pkr.p.ec.isActive(hdr) {
				pkr.p.ec.setActive(now)
			}
		}
		return pid, 0, err
	}

	return pkr.p.slowKalive(smap, nil /*htext*/, timeout)
}

func (pkr *palive) do(config *cmn.Config) (stopped bool) {
	smap := pkr.p.owner.smap.get()
	if smap == nil || smap.validate() != nil {
		return false
	}
	if smap.isPrimary(pkr.p.si) {
		if !pkr.inProgress.CAS(false, true) {
			nlog.Warningln(pkr.p.String(), "primary keepalive in progress") // NOTE: see wg.Wait() below
			return false
		}
		stopped = pkr.updateSmap(config)
		pkr.inProgress.Store(false)
		return stopped
	}
	if !pkr.timeToPing(smap.Primary.ID()) { // skip sending keepalive
		return false
	}
	if stopped = pkr.keepalive.do(smap, pkr.p.si, config); stopped {
		pkr.p.onPrimaryDown(pkr.p /*self*/, "")
	}
	return stopped
}

// keep-alive nodes in parallel; nodes that fail to respond get removed from the cluster map (Smap)
// (see 'maintenance-mode' comment below)
func (pkr *palive) updateSmap(config *cmn.Config) (stopped bool) {
	var (
		p    = pkr.p
		smap = p.owner.smap.get()
		cnt  = smap.Count()
	)
	pkr.openCh(cnt)
	wg := cos.NewLimitedWaitGroup(sys.MaxParallelism(), cnt) // limit parallelism
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

			// direct call first
			started := mono.NanoTime()
			_, status, err := pkr.p.reqHealth(si, config.Timeout.CplaneOperation.D(), nil, smap, false /*retry pub-addr*/)
			if err == nil {
				now := mono.NanoTime()
				pkr.statsT.Add(stats.KeepAliveLatency, now-started)
				pkr.hb.HeardFrom(si.ID(), now) // effectively, yes
				continue
			}
			// otherwise, go keepalive with retries
			nlog.Warningln(pkr.p.String(), "failed to fast-kalive", si.StringEx(), "err: [", err, status, "]")

			pkr.statsT.Inc(stats.ErrKaliveCount)
			wg.Add(1)
			go pkr.goping(si, wg, smap, config)
		}
	}
	wg.Wait()

	if stopped = len(pkr.stoppedCh) > 0; stopped {
		pkr.closeCh()
		return true
	}
	if len(pkr.toRemoveCh) == 0 {
		return false
	}

	ctx := &smapModifier{pre: pkr._pre, final: pkr._final}
	err := p.owner.smap.modify(ctx)
	if err != nil {
		if ctx.msg != nil {
			nlog.Errorln("FATAL:", err)
		} else {
			nlog.Warningln(err)
		}
	}
	return false
}

// "slow-kalive"
func (pkr *palive) goping(si *meta.Snode, wg cos.WG, smap *smapX, config *cmn.Config) {
	if len(pkr.stoppedCh) > 0 {
		wg.Done()
		return
	}
	ok, stopped := pkr._pingRetry(si, smap, config)
	if stopped {
		pkr.stoppedCh <- struct{}{}
	}
	if !ok {
		pkr.toRemoveCh <- si.ID()
	}
	wg.Done()
}

func (pkr *palive) _pingRetry(si *meta.Snode, smap *smapX, config *cmn.Config) (ok, stopped bool) {
	var (
		tout         = config.Timeout.CplaneOperation.D()
		started      = mono.NanoTime()
		pname, sname = pkr.p.String(), si.StringEx()
	)
	_, status, err := pkr.p.reqHealth(si, tout, nil, smap, true /*retry via pub-addr, if different*/)
	if err == nil {
		now := mono.NanoTime()
		pkr.statsT.Add(stats.KeepAliveLatency, now-started)
		pkr.hb.HeardFrom(si.ID(), now) // effectively, yes
		if cmn.Rom.FastV(5, cos.SmoduleKalive) {
			nlog.Infoln(pname, "slow-kalive", sname, "OK after the first attempt")
		}
		return true, false
	}

	tout = config.Timeout.MaxKeepalive.D()
	nlog.Warningln(pname, "failed to slow-kalive", sname, "- retrying [", err, status, tout, smap.StringEx(), "]")
	pkr.statsT.Inc(stats.ErrKaliveCount)

	ticker := time.NewTicker(cmn.KeepaliveRetryDuration(config))
	ok, stopped = pkr.retry(si, ticker, tout, config.Keepalive.NumRetries)
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
			switch {
			case clone.GetProxy(sid) != nil:
				clone.delProxy(sid)
				clone.staffIC()
				metaction += apc.Proxy
				cnt++
			case clone.GetTarget(sid) != nil:
				clone.delTarget(sid)
				metaction += apc.Target
				cnt++
			default:
				metaction += unknownDaemonID
				nlog.Warningf("node %s not present in the %s (old %s)", sid, clone, ctx.smap)
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

// NOTE: total number of keepalive failures prior to removing a node from Smap: (1 + 1 + kaNumRetries)
func (pkr *palive) retry(si *meta.Snode, ticker *time.Ticker, tout time.Duration, kaNumRetries int) (ok, stopped bool) {
	var i int
	for {
		if !pkr.timeToPing(si.ID()) {
			return true, false
		}
		select {
		case <-ticker.C:
			if !pkr.timeToPing(si.ID()) {
				return true, false // heard from the node, skipping health check
			}
			var (
				started = mono.NanoTime()
				smap    = pkr.p.owner.smap.get()
			)
			_, status, err := pkr.p.reqHealth(si, tout, nil, smap, true /*retry via pub-addr, if different*/)
			if err == nil {
				now := mono.NanoTime()
				pkr.statsT.Add(stats.KeepAliveLatency, now-started)
				pkr.hb.HeardFrom(si.ID(), now) // effectively, yes
				return true, false
			}

			pkr.statsT.Inc(stats.ErrKaliveCount)
			i++

			if i >= kaNumRetries {
				debug.Assert(i == kaNumRetries)
				nlog.Errorln("slow-kalive failure after", i, "attempts - removing", si.StringEx(),
					"from", smap.StringEx())
				return false, false
			}

			if cos.IsUnreachable(err, status) {
				continue
			}
			nlog.Warningf("unexpected error %v(%d) from %s", err, status, si.StringEx())
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

func (k *keepalive) init(keepaliver keepaliver, statsT stats.Tracker, startedUp *atomic.Bool, name string, ival time.Duration) {
	k.name = name
	k.k = keepaliver
	k.statsT = statsT
	k.startedUp = startedUp
	k.controlCh = make(chan controlSignal) // unbuffered on purpose
	k.interval = ival
	k.hb = newHB(ival)
}

func (k *keepalive) heardFrom(sid string) int64 {
	return k.hb.HeardFrom(sid, 0 /*now*/)
}

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

// prepopulate hb
func (k *keepalive) prepopulate(smap *smapX, self string) {
	for _, nm := range []meta.NodeMap{smap.Pmap, smap.Tmap} {
		for sid := range nm {
			if sid == self {
				continue
			}
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
			config := cmn.GCO.Get()
			k.k.do(config)
			k.configUpdate(k.k.cfg(config))
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
				config := cmn.GCO.Get()
				if mono.Since(lastCheck) >= cmn.KeepaliveRetryDuration(config) {
					lastCheck = mono.NanoTime()
					nlog.Infoln("triggered by", sig.err)
					if stopped := k.k.do(config); stopped {
						ticker.Stop()
						return
					}
				}
			}
		}
	}
}

func (k *keepalive) configUpdate(cfg *cmn.KeepaliveTrackerConf) {
	if k.hb.set(cfg.Interval.D()) {
		k.interval = cfg.Interval.D()
	}
}

// keepalive => primary
// is called by non-primary proxies and all targets
func (k *keepalive) do(smap *smapX, si *meta.Snode, config *cmn.Config) (stopped bool) {
	var (
		pid     = smap.Primary.ID()
		pname   = meta.Pname(pid)
		tout    = config.Timeout.CplaneOperation.D()
		started = mono.NanoTime()
		sname   = si.String()
	)
	if nlog.Stopping() {
		return true
	}
	fast := k.k.cluUptime(started) > max(k.interval<<2, config.Timeout.Startup.D()>>1)
	cpid, status, err := k.k.sendKalive(smap, tout, started, fast)
	if err == nil {
		now := mono.NanoTime()
		k.statsT.Add(stats.KeepAliveLatency, now-started)
		k.hb.HeardFrom(pid, now) // effectively, yes
		return false
	}

	k.statsT.Inc(stats.ErrKaliveCount)

	debug.Assert(cpid == pid && cpid != si.ID())
	nlog.Warningln(sname, "=>", pname, "failure - retrying: [", fast, tout, err, status, "]")

	//
	// retry
	//
	var (
		ticker = time.NewTicker(cmn.KeepaliveRetryDuration(config))
		i      int
	)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// NOTE: suspecting primary down, not checking k.timeToPing(smap.Primary),
			// and therefore not skipping keepalive req (compare with palive.retry)
			i++
			started := mono.NanoTime()
			pid, status, err = k.k.sendKalive(nil, tout, started, false /*fast*/)
			if pid == si.ID() {
				return false // elected as primary
			}
			pname = meta.Pname(pid)
			if err == nil {
				now := mono.NanoTime()
				k.statsT.Add(stats.KeepAliveLatency, now-started)
				k.hb.HeardFrom(pid, now) // effectively, yes
				nlog.Infoln(sname, "=>", pname, "OK after", i, "attempt"+cos.Plural(i), "tout", tout)
				return false
			}
			// repeat up to `kaNumRetries` times with max-keepalive timeout
			tout = config.Timeout.MaxKeepalive.D()

			if i == config.Keepalive.NumRetries {
				nlog.Warningln(sname, "=>", pname, "failed after", i, "attempts")
				return true
			}
			if cos.IsUnreachable(err, status) {
				continue
			}
			if nlog.Stopping() {
				return true
			}
			err = fmt.Errorf("%s: unexpected response from %s: %v(%d)", sname, pname, err, status)
			debug.AssertNoErr(err)
			nlog.Warningln(err)
		case sig := <-k.controlCh:
			if sig.msg == kaStopMsg {
				return true
			}
		}
	}
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

func (hb *heartBeat) HeardFrom(id string, now int64) int64 {
	var (
		val   *int64
		v, ok = hb.last.Load(id)
	)
	if now == 0 {
		now = mono.NanoTime()
	}
	if ok {
		val = v.(*int64) // almost always
	} else {
		val = new(int64)
		hb.last.Store(id, val)
	}
	ratomic.StoreInt64(val, now)
	return now
}

func (hb *heartBeat) TimedOut(id string) bool {
	v, ok := hb.last.Load(id)
	if !ok {
		return true
	}
	val := v.(*int64)
	tim := ratomic.LoadInt64(val)

	return mono.Since(tim) > hb.interval
}

func (hb *heartBeat) reg(id string) { hb.last.Store(id, new(int64)) }

func (hb *heartBeat) set(interval time.Duration) (changed bool) {
	changed = hb.interval != interval
	hb.interval = interval
	return
}
