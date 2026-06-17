// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
)

// SDM lifetime is request-driven. Get-batch/x-moss opens SDM explicitly
// on the DT and on each sender before traffic starts. Unlike EC streams,
// SDM expires locally after Rx/Tx inactivity.
//
// NOTE: Tx activity is tracked at SDM entry points, not at the transport
// stream-completion boundary. This is intentional: SDM is kept alive by
// a long idle window = sdmIdleTime, while receiver-side lifetime is
// protected explicitly by registered demux receivers.

const (
	sbrWinMax = 15 * time.Second

	sdmPruneIval = hk.Prune2mIval        // (2m)
	sdmIdleTime  = cmn.SharedStreamsDflt // (10m)

	epochShift    = 34
	epochDuration = time.Duration(1 << epochShift)                       // ~17.18s
	idleEpochs    = int64((sdmIdleTime+epochDuration-1)>>epochShift) + 1 // ceil

	iniSdmCap = 16
)

const nameSDM = "shared-dm"

type (
	rxent struct {
		rx transport.Receiver
	}
	sharedDM struct {
		receivers map[string]*rxent
		dm        DM

		// per-sender ErrSBR windows (Rx side) // TODO: prevent on/off flapping
		sbrs struct {
			m   map[string]*transport.ErrSBR
			mtx sync.RWMutex
		}
		stats struct {
			drops   atomic.Int64
			nsbrs   atomic.Int64
			rxerr   atomic.Int64
			lastSum int64 // hk-only
		}
		last atomic.Int64 // coarse mono-time in epochDuration units

		ocmu sync.Mutex
		rxmu sync.RWMutex
	}
)

// global
var SDM sharedDM

func (*sharedDM) trname() string   { return nameSDM }
func (sdm *sharedDM) isOpen() bool { return sdm.dm.stage.opened.Load() }

func nowEpochs() int64 {
	return mono.NanoTime() >> epochShift
}

func (sdm *sharedDM) touch() {
	last := sdm.last.Load()
	now := nowEpochs()
	if now <= last {
		return
	}

	// best-effort monotonic refresh
	sdm.last.CAS(last, now)
}

func (sdm *sharedDM) active() bool {
	sdm.rxmu.RLock()
	active := len(sdm.receivers) > 0
	sdm.rxmu.RUnlock()
	if active {
		return true
	}

	last := sdm.last.Load()
	debug.Assert(last != 0) // set in sdm.Open()
	return nowEpochs()-last < idleEpochs
}

// (same as above when rxmu locked)
func (sdm *sharedDM) activeLocked() bool {
	if len(sdm.receivers) > 0 {
		return true
	}

	last := sdm.last.Load()
	debug.Assert(last != 0) // ditto
	return nowEpochs()-last < idleEpochs
}

func (sdm *sharedDM) String() string {
	last := sdm.last.Load()
	// consider including sdm.dm.String() when verbose
	return sdm.trname() + "[approx. num-receivers: " + strconv.Itoa(len(sdm.receivers)) + ", idle approx.: " + mono.Since(last<<epochShift).String() + "]"
}

// called on-demand
// note: config changes (via dm.init) won't take effect until SDM is closed and reopened
func (sdm *sharedDM) Open(config *cmn.Config, selected *cmn.XactConf) error {
	debug.Assert(epochDuration >= time.Second)
	debug.Assert(epochDuration < sdmIdleTime/2)

	sdm.touch()

	sdm.ocmu.Lock()
	if sdm.isOpen() {
		sdm.ocmu.Unlock()
		return nil
	}

	// (re)init for a given 'selected' caller -------------------------------
	extra := Extra{
		XactConf: *selected,
		Config:   config,
	}
	SDM.dm.init(SDM.trname(), SDM.recv, cmn.OwtNone, extra)

	sdm.rxmu.Lock()
	sdm.receivers = make(map[string]*rxent, iniSdmCap)
	sdm.rxmu.Unlock()

	if err := sdm.dm.RegRecv(); err != nil {
		sdm.ocmu.Unlock()
		nlog.ErrorDepth(1, core.T.String(), err)
		debug.AssertNoErr(err)
		return err
	}
	sdm.dm.parent = &transport.Parent{
		TermedCB: sdm.reconnect,
	}
	sdm.dm.Open()
	sdm.ocmu.Unlock()

	debug.Assert(sdm.last.Load() != 0) // touched above
	hk.Reg(sdm.trname()+hk.NameSuffix, sdm.housekeep, sdmPruneIval)

	nlog.InfoDepth(1, core.T.String(), "open", sdm.trname())
	return nil
}

// TODO: reconnect() must become common for all DM-based xactions
func (sdm *sharedDM) reconnect(dstID string, err error) {
	if !sdm.isOpen() {
		return
	}
	if e := sdm.dm.data.streams.ReopenPeerStream(dstID); e != nil {
		err = fmt.Errorf("%s: failed reconnecting to %s (%w --> %w)", sdm.trname(), dstID, err, e)
		nlog.Errorln(core.T.String(), err, "- closing/aborting...")

		// tear down entire SDM bundle
		// safe to race with local idle close, see dm.Close() and dm.UnregRecv()
		sdm.Close(err)
		return
	}

	// ping remote peer
	go sdm.gosend(dstID)
}

func (sdm *sharedDM) gosend(dstID string) {
	o := transport.AllocSend()
	o.Hdr.Opcode = transport.OpcReconnect
	o.Hdr.SID = core.T.SID()

	smap := core.T.Sowner().Get()
	tsi := smap.GetNode(dstID)
	if tsi != nil && sdm.isOpen() {
		err := sdm.Send(o, nil /*roc*/, tsi, nil)
		nlog.Warningln(core.T.String(), "reconnect/send:", sdm.trname(), "-->", dstID, err)
	}
}

func (sdm *sharedDM) CanSend(sid string) (err error) {
	sdm.sbrs.mtx.RLock()
	if e, ok := sdm.sbrs.m[sid]; ok {
		if e.Since() < sbrWinMax {
			err = e
		}
	}
	sdm.sbrs.mtx.RUnlock()
	return
}

func (sdm *sharedDM) _stats() {
	a, b, c := sdm.stats.drops.Load(), sdm.stats.rxerr.Load(), sdm.stats.nsbrs.Load()
	sum := a + b + c
	if sdm.stats.lastSum == sum {
		return // no change
	}
	sdm.stats.lastSum = sum
	nlog.Infoln(sdm.trname(), "stats:", "drops", a, "rxerr", b, "sbrs", c)
}

func (sdm *sharedDM) housekeep(now int64) time.Duration {
	if !sdm.isOpen() {
		return hk.UnregInterval
	}

	sdm._stats()

	if sdm.active() {
		return hk.Jitter(sdmPruneIval, now)
	}
	// tentatively (lock and check inside)
	if err := sdm.Close(); err != nil {
		if cmn.Rom.V(4, cos.ModTransport) {
			nlog.Warningln(err)
		}
		return hk.Jitter(sdmPruneIval, now)
	}
	return hk.UnregInterval
}

// no registered receivers + sdmIdleTime inactivity
func (sdm *sharedDM) Close(errs ...error) error {
	sdm.ocmu.Lock()

	if !sdm.isOpen() {
		sdm.ocmu.Unlock()
		return nil
	}

	sdm.rxmu.Lock()

	var (
		err error
		l   = len(errs)
	)
	if l > 0 {
		err = errs[0]
	}
	if sdm.activeLocked() {
		if l == 0 {
			sdm.rxmu.Unlock()
			sdm.ocmu.Unlock()
			return errors.New("cannot close " + sdm.String())
		}
		nlog.Errorln("closing", sdm.String(), "with errors", errs)
	}

	// release rxmu (intended only to protect sdm.receivers) before calling dm.Close()
	sdm.receivers = nil
	sdm.rxmu.Unlock()

	sdm.dm.Close(err)
	sdm.dm.UnregRecv()

	sdm.ocmu.Unlock()

	sdm.sbrs.mtx.Lock()
	clear(sdm.sbrs.m)
	sdm.sbrs.mtx.Unlock()

	sdm._stats()
	nlog.InfoDepth(1, core.T.String(), "close", sdm.trname())
	return nil
}

// demux-level RegRecv (not to confuse with transport-level namesake)
func (sdm *sharedDM) RegRecv(rx transport.Receiver) {
	sdm.ocmu.Lock()
	sdm.rxmu.Lock()

	debug.Assert(sdm.isOpen(), sdm.trname(), ": RegRecv while closed: ", rx.ID())
	if sdm.isOpen() {
		en := &rxent{rx: rx}
		sdm.receivers[rx.ID()] = en
	}
	sdm.rxmu.Unlock()
	sdm.ocmu.Unlock()
}

func (sdm *sharedDM) UseRecv(rx transport.Receiver) {
	// fast path
	sdm.rxmu.RLock()
	_, ok := sdm.receivers[rx.ID()]
	sdm.rxmu.RUnlock()
	if ok {
		return
	}

	// slow and unlikely
	sdm.touch()
	sdm.RegRecv(rx)
}

// remove demux entry immediately
func (sdm *sharedDM) UnregRecv(xid string) {
	sdm.touch()
	sdm.rxmu.Lock()
	delete(sdm.receivers, xid)
	sdm.rxmu.Unlock()
}

func (sdm *sharedDM) Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *meta.Snode, xctn core.Xact) (err error) {
	sdm.touch()
	err = sdm.dm.Send(obj, roc, tsi, xctn)
	return
}

func (sdm *sharedDM) recv(hdr *transport.ObjHdr, r io.Reader, err error) error {
	if err != nil {
		if e := transport.AsErrSBR(err); e != nil {
			sdm.stats.nsbrs.Inc()
			// add and prune
			sdm.sbrs.mtx.Lock()
			if sdm.sbrs.m == nil {
				sdm.sbrs.m = make(map[string]*transport.ErrSBR, 4)
			}
			sdm.sbrs.m[e.SID()] = e
			for sid, ee := range sdm.sbrs.m {
				if ee != e && ee.Since() > sbrWinMax {
					delete(sdm.sbrs.m, sid)
				}
			}
			sdm.sbrs.mtx.Unlock()
			nlog.Warningln(core.T.String(), sdm.trname(), e)
		}
		return err
	}

	if hdr.Opcode == transport.OpcReconnect {
		sdm.sbrs.mtx.Lock()
		delete(sdm.sbrs.m, hdr.SID)
		sdm.sbrs.mtx.Unlock()
		nlog.Warningln(core.T.String(), sdm.trname(), "successful reconnect from", hdr.SID)
		return nil
	}

	xid := hdr.Demux
	if err := xact.CheckValidUUID(xid); err != nil {
		err = fmt.Errorf("%s: %w", sdm.trname(), err)
		return err
	}

	var (
		en         *rxent
		closed, ok bool
	)
	sdm.rxmu.RLock()
	closed = sdm.receivers == nil
	en, ok = sdm.receivers[xid]
	sdm.rxmu.RUnlock()

	if !ok {
		transport.DrainAndFreeReader(r)
		n := sdm.stats.drops.Inc()
		if closed {
			cmn.SparseWarn(cos.ModTransport, n, sdm.trname(), "is closed (dropping all Rx)")
		} else {
			cmn.SparseWarn(cos.ModTransport, n, sdm.trname(), "xid", xid, "not found - dropping", hdr.ObjName)
		}
		return nil
	}

	// (unlikely)
	if en.rx.ID() != xid {
		err := fmt.Errorf("%s: xid mismatch [%q vs %q]", sdm.trname(), xid, en.rx.ID())
		debug.AssertNoErr(err)
		return err
	}

	// do receive
	e := en.rx.RecvObj(hdr, r, nil) // not holding rxmu locked - race vs UnregRecv possible but benign
	if e != nil {
		n := sdm.stats.rxerr.Inc()
		cmn.SparseWarn(cos.ModTransport, n, sdm.trname(), "Rx failure: [ xid:", xid, "err:", e, "oname:", hdr.ObjName, "num Rx errors:", n, "]")
		return e
	}
	return nil
}
