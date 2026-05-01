// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// rebalance & resilver xactions

const fmtpend = "%s: rebalance[%s] is "

type (
	rebFactory struct {
		xctn *Rebalance
		xreg.RenewBase
	}
	resFactory struct {
		xctn *Resilver
		xreg.RenewBase
	}

	Rebalance struct {
		Args *xreg.RebArgs
		xact.Base

		// CtlMsg
		ctl struct {
			mu   sync.Mutex
			sb   cos.SB        // guarded by ctl.mu when refreshed
			fn   func(*cos.SB) // guarded by ctl.mu; installed by reb.Run
			last int64         // last-refresh mono nanos
		}
	}
	Resilver struct {
		Args   *xreg.ResArgs
		jgroup ratomic.Pointer[mpather.Jgroup]
		xact.Base
		Nbusy   atomic.Int64
		nvisits atomic.Int64
	}
)

// interface guard
var (
	_ core.Xact      = (*Rebalance)(nil)
	_ xreg.Renewable = (*rebFactory)(nil)

	_ core.Xact      = (*Resilver)(nil)
	_ xreg.Renewable = (*resFactory)(nil)
)

var _rebID atomic.Int64

///////////////
// Rebalance //
///////////////

func (*rebFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &rebFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *rebFactory) Start() (err error) {
	p.xctn, err = newRebalance(p)
	return err
}

func (*rebFactory) Kind() string     { return apc.ActRebalance }
func (p *rebFactory) Get() core.Xact { return p.xctn }

func (p *rebFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*rebFactory)
	if prev.Args.UUID == p.Args.UUID {
		return xreg.WprUse, nil
	}

	//
	// NOTE: we always abort _previous_ (via `reb._preempt`) prior to starting a new one
	//
	nlog.Errorln(core.T.String(), "unexpected when-prev-running call:", prev.Args.UUID, p.Args.UUID)

	ic, ec := xact.S2RebID(p.Args.UUID)
	if ec != nil {
		nlog.Errorln("FATAL:", p.Args.UUID, ec)
		return xreg.WprAbort, ec // (unlikely)
	}
	ip, ep := xact.S2RebID(prev.Args.UUID)
	if ep != nil {
		nlog.Errorln("FATAL:", prev.Args.UUID, ep)
		return xreg.WprAbort, ep
	}
	debug.Assert(ip <= ic, "curr ", p.Args.UUID, "> prev ", prev.Args.UUID)
	return xreg.WprAbort, nil
}

func newRebalance(p *rebFactory) (xreb *Rebalance, err error) {
	xreb = &Rebalance{}

	xreb.Args = p.Args.Custom.(*xreg.RebArgs)
	debug.Assert(xreb.Args != nil)

	// init
	xreb.InitBase(p.Args.UUID, p.Kind(), nil)

	// ID
	id, err := xact.S2RebID(p.Args.UUID)
	if err != nil {
		return nil, err
	}
	rebID := _rebID.Load()
	if rebID > id {
		return nil, fmt.Errorf(fmtpend+"old", core.T.String(), p.Args.UUID)
	}
	if rebID == id {
		return nil, fmt.Errorf(fmtpend+"current", core.T.String(), p.Args.UUID)
	}
	_rebID.Store(id)

	xreb.ctl.sb.Init(ctlMsgBufSize)
	xreb.writeStatic(&xreb.ctl.sb)

	return xreb, nil
}

//
// CtlMsg begin ---------------------------------------
//

const (
	ctlMsgRefreshFreq = 10 * time.Second
	ctlMsgBufSize     = 512
)

// The callback is not cleared. FinalCtlMsg freezes the last
// rendered message for historical show-job output.
func (xreb *Rebalance) SetCtlMsgFn(fn func(*cos.SB)) {
	debug.Assert(fn != nil)

	xreb.ctl.mu.Lock()
	xreb.ctl.fn = fn
	xreb.refreshLocked()
	xreb.ctl.last = mono.NanoTime()
	xreb.ctl.mu.Unlock()
}

func (xreb *Rebalance) FinalCtlMsg() {
	xreb.ctl.mu.Lock()
	xreb.refreshLocked()
	xreb.ctl.last = math.MaxInt64
	xreb.ctl.mu.Unlock()
}

func (xreb *Rebalance) CtlMsg() string {
	xreb.ctl.mu.Lock()

	last := xreb.ctl.last
	if last != math.MaxInt64 {
		now := mono.NanoTime()
		if now-last >= int64(ctlMsgRefreshFreq) {
			xreb.refreshLocked()
			xreb.ctl.last = now
		}
	}

	// safe enough here: refresh is serialized and throttled, and callers
	// consume CtlMsg immediately
	s := xreb.ctl.sb.String()
	xreb.ctl.mu.Unlock()
	return s
}

// must hold ctl.mu
func (xreb *Rebalance) refreshLocked() {
	xreb.ctl.sb.Reset(ctlMsgBufSize, false)
	xreb.writeStatic(&xreb.ctl.sb)
	if xreb.ctl.fn != nil {
		xreb.ctl.fn(&xreb.ctl.sb)
	}
}

func (xreb *Rebalance) writeStatic(sb *cos.SB) {
	if xreb.Args.Bck != nil {
		sb.WriteString(xreb.Args.Bck.Cname(xreb.Args.Prefix))
	}
	fl := xreb.Args.Flags
	if fl == 0 {
		return
	}
	sb.WriteString(", flags:")
	first := true
	if fl&xact.FlagLatestVer != 0 {
		first = false
		sb.WriteString("latest")
		fl &^= xact.FlagLatestVer
	}
	if fl&xact.FlagSync != 0 {
		if !first {
			sb.WriteUint8(',')
		}
		first = false
		sb.WriteString("sync")
		fl &^= xact.FlagSync
	}
	if fl != 0 {
		if !first {
			sb.WriteUint8(',')
		}
		sb.WriteString("0x")
		sb.WriteString(strconv.FormatUint(uint64(fl), 16))
	}
}

func (xreb *Rebalance) Snap() (snap *core.Snap) {
	snap = xreb.Base.NewSnap(xreb)
	snap.CtlMsg = xreb.CtlMsg()
	return
}

//
// CtlMsg end ---------------------------------------
//

func (*Rebalance) Run(*sync.WaitGroup) { debug.Assert(false) }

func (xreb *Rebalance) RebID() int64 {
	id, err := xact.S2RebID(xreb.ID())
	debug.AssertNoErr(err)
	return id
}

//////////////
// Resilver //
//////////////

func (*resFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &resFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *resFactory) Start() error {
	p.xctn = newResilver(p)
	return nil
}

func (*resFactory) Kind() string                                       { return apc.ActResilver }
func (p *resFactory) Get() core.Xact                                   { return p.xctn }
func (*resFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) { return xreg.WprAbort, nil }

func newResilver(p *resFactory) (xres *Resilver) {
	xres = &Resilver{}
	xres.InitBase(p.UUID(), p.Kind(), nil /*bck*/)

	xres.Args = p.Args.Custom.(*xreg.ResArgs)
	debug.Assert(xres.Args != nil)

	return xres
}

func (*Resilver) Run(*sync.WaitGroup) { debug.Assert(false) }

func (xres *Resilver) SetJgroup(jgroup *mpather.Jgroup) { xres.jgroup.Store(jgroup) }

func (xres *Resilver) CtlMsg() string {
	var (
		jgroup  = xres.jgroup.Load()
		nvisits = xres.nvisits.Load()
		sb      cos.SB
	)
	sb.Init(64)

	// visited so far
	if jgroup != nil {
		nvisits = jgroup.NumVisits()
		xres.nvisits.Store(nvisits)
	}
	if nvisits > 0 {
		sb.WriteString("visited:")
		sb.WriteString(strconv.FormatInt(nvisits, 10))
	}

	// skipped busy
	if n := xres.Nbusy.Load(); n > 0 {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("skipped-busy:")
		sb.WriteString(strconv.FormatInt(n, 10))
	}
	return sb.String()
}

func (xres *Resilver) String() string {
	if xres == nil {
		return "<xres-nil>"
	}
	return xres.Base.String()
}

func (xres *Resilver) Snap() *core.Snap { return xres.Base.NewSnap(xres) }
