// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO: support num-workers (see xact/xs)

type (
	putFactory struct {
		xreg.RenewBase
		xctn *XactPut
		lom  *core.LOM
	}
	XactPut struct {
		// implements core.Xact interface
		xact.DemandBase
		// mountpath workers
		wkg    *mpather.WorkerGroup
		workCh chan core.LIF
		// init
		mirror cmn.MirrorConf
		config *cmn.Config
	}
)

// interface guard
var (
	_ core.Xact      = (*XactPut)(nil)
	_ xreg.Renewable = (*putFactory)(nil)
)

////////////////
// putFactory //
////////////////

func (*putFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &putFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, lom: args.Custom.(*core.LOM)}
	return p
}

func (p *putFactory) _tag(bck *meta.Bck) []byte {
	var (
		uname = bck.MakeUname("")
		l     = cos.PackedStrLen(p.Kind()) + 1 + cos.PackedBytesLen(uname)
		pack  = cos.NewPacker(nil, l)
	)
	pack.WriteString(p.Kind())
	pack.WriteUint8('|')
	pack.WriteBytes(uname)
	return pack.Bytes()
}

func (p *putFactory) Start() error {
	lom := p.lom
	slab, err := core.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	debug.AssertNoErr(err)

	bck, mirror := lom.Bck(), lom.MirrorConf()
	if !mirror.Enabled {
		return fmt.Errorf("%s: mirroring disabled, nothing to do", bck.String())
	}
	if err = fs.ValidateNCopies(core.T.String(), int(mirror.Copies)); err != nil {
		nlog.Errorln(err)
		return err
	}
	r := &XactPut{mirror: *mirror, workCh: make(chan core.LIF, mirror.Burst)}

	//
	// target-local generation of a global UUID
	//
	div := uint64(xact.IdleDefault)
	beid, _, _ := xreg.GenBEID(div, p._tag(bck))
	if beid == "" {
		// is Ok (compare with x-archive, x-tco)
		beid = cos.GenUUID()
	}
	r.DemandBase.Init(beid, p.Kind(), bck, xact.IdleDefault)

	// joggers
	r.wkg, err = mpather.NewWorkerGroup(&mpather.WorkerGroupOpts{
		Callback:   r.do,
		Slab:       slab,
		WorkChSize: mirror.Burst,
	})
	if err != nil {
		return err
	}
	p.xctn = r

	// run
	go r.Run(nil)
	return nil
}

func (*putFactory) Kind() string     { return apc.ActPutCopies }
func (p *putFactory) Get() core.Xact { return p.xctn }

func (p *putFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

/////////////
// XactPut //
/////////////

func (r *XactPut) CtlMsg() string {
	if !r.mirror.Enabled {
		return "mirror disabled"
	}
	var sb cos.SB
	sb.Init(32)
	sb.WriteString("copies:")
	sb.WriteString(strconv.FormatInt(r.mirror.Copies, 10))
	sb.WriteString(", burst:")
	sb.WriteString(strconv.Itoa(r.mirror.Burst))
	return sb.String()
}

// (one worker per mountpath)
func (r *XactPut) do(lom *core.LOM, buf []byte) {
	copies := int(lom.Bprops().Mirror.Copies)

	lom.Lock(true)
	size, err := addCopies(lom, copies, buf)
	lom.Unlock(true)

	if err != nil {
		r.AddErr(err, 5, cos.ModMirror)
	} else {
		r.ObjsAdd(1, size)
	}
	r.DecPending() // (see IncPending below)
	core.FreeLOM(lom)
}

// control logic: stop and idle timer
// (LOMs get dispatched directly to workers)
func (r *XactPut) Run(*sync.WaitGroup) {
	var err error
	nlog.Infoln(r.Name())
	r.config = cmn.GCO.Get()
	r.wkg.Run()
loop:
	for {
		select {
		case <-r.IdleTimer():
			r.waitPending()
			break loop
		case <-r.ChanAbort():
			break loop
		}
	}

	err = r.stop()
	if err != nil {
		r.AddErr(err)
	}
	r.Finish()
}

// main method
func (r *XactPut) Repl(lom *core.LOM) {
	debug.Assert(!r.IsDone(), r.String())

	// ref-count on-demand, decrement via worker.Callback = r.do
	r.IncPending()
	if err := r.wkg.PostLIF(lom); err != nil {
		r.DecPending()
		r.Abort(fmt.Errorf("%s: %v", r, err))
	}
}

func (r *XactPut) waitPending() {
	const minsleep, longtime = 4 * time.Second, 30 * time.Second
	var (
		started     int64
		cnt, iniCnt int
		sleep       = max(cmn.Rom.MaxKeepalive(), minsleep)
	)
	if cnt = len(r.workCh); cnt == 0 {
		return
	}
	started, iniCnt = mono.NanoTime(), cnt
	// keep sleeping until the very end
	for cnt > 0 {
		r.IncPending()
		time.Sleep(sleep)
		r.DecPending()
		cnt = len(r.workCh)
	}
	if d := mono.Since(started); d > longtime {
		nlog.Infof("%s: took a while to finish %d pending copies: %v", r, iniCnt, d)
	}
}

func (r *XactPut) stop() (err error) {
	r.DemandBase.Stop()
	n := r.wkg.Stop()
	if nn := core.DrainLIF(r.workCh); nn > 0 {
		n += nn
	}
	if n > 0 {
		r.SubPending(n)
		err = fmt.Errorf("%s: dropped %d object%s", r, n, cos.Plural(n))
	}
	if a := r.wkg.ChanFullTotal(); a > 0 {
		nlog.Warningln(r.Name(), "work channel full (total final)", a)
	}
	return err
}

func (r *XactPut) Snap() *core.Snap { return r.Base.NewSnap(r) }
