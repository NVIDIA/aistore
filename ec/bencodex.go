// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/prob"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO: support num-workers (see xact/xs)

const rcvyWorkChanSize = 256

type (
	encFactory struct {
		xreg.RenewBase
		xctn  *XactBckEncode
		phase string
	}
	XactBckEncode struct {
		xact.Base
		bck  *meta.Bck
		wg   *sync.WaitGroup // to wait for EC finishes all objects
		smap *meta.Smap
		//
		// check and recover slices and metafiles
		//
		probFilter      *prob.Filter
		rcvyJG          map[string]*rcvyJogger
		last            atomic.Int64
		done            atomic.Bool
		checkAndRecover bool
	}
	rcvyJogger struct {
		mi       *fs.Mountpath
		workCh   chan *core.LOM
		r        *XactBckEncode
		chanFull cos.ChanFull
	}
)

// interface guard
var (
	_ core.Xact      = (*XactBckEncode)(nil)
	_ xreg.Renewable = (*encFactory)(nil)
)

////////////////
// encFactory //
////////////////

func (*encFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.ECEncodeArgs)
	p := &encFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, phase: custom.Phase}
	return p
}

func (p *encFactory) Start() error {
	custom := p.Args.Custom.(*xreg.ECEncodeArgs)
	r := &XactBckEncode{
		bck:             p.Bck,
		checkAndRecover: custom.Recover,
	}
	if err := r.init(p.UUID()); err != nil {
		return err
	}
	p.xctn = r
	return nil
}

func (*encFactory) Kind() string     { return apc.ActECEncode }
func (p *encFactory) Get() core.Xact { return p.xctn }

func (p *encFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*encFactory)
	if prev.phase == apc.Begin2PC && p.phase == apc.Commit2PC {
		prev.phase = apc.Commit2PC // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", p.Kind(), prev.xctn.Bck().Name, prev.phase, p.phase)
	return
}

///////////////////
// XactBckEncode //
///////////////////

func (r *XactBckEncode) init(uuid string) error {
	r.wg = &sync.WaitGroup{}
	r.smap = core.T.Sowner().Get()

	var ctlmsg string
	if r.checkAndRecover {
		ctlmsg = "recover"
		r.probFilter = prob.NewDefaultFilter()
	}
	r.InitBase(uuid, apc.ActECEncode, ctlmsg, r.bck)

	if err := r.bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	if !r.bck.Props.EC.Enabled {
		return fmt.Errorf("EC is disabled for %s", r.bck.Cname(""))
	}

	avail := fs.GetAvail()
	if len(avail) == 0 {
		return cmn.ErrNoMountpaths
	}
	if r.checkAndRecover {
		// construct recovery joggers
		r.rcvyJG = make(map[string]*rcvyJogger, len(avail))
		for _, mi := range avail {
			j := &rcvyJogger{
				mi:     mi,
				workCh: make(chan *core.LOM, rcvyWorkChanSize),
				r:      r,
			}
			r.rcvyJG[mi.Path] = j
		}
	}

	return nil
}

func (r *XactBckEncode) Run(gowg *sync.WaitGroup) {
	ECM.incActive(r)
	gowg.Done()

	opts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjectType},
		VisitObj: r.encode,
		DoLoad:   mpather.LoadUnsafe,
	}
	opts.Bck.Copy(r.bck.Bucket())

	if r.checkAndRecover {
		// additionally, traverse and visit
		opts.CTs = []string{fs.ObjectType, fs.ECMetaType, fs.ECSliceType}
		opts.VisitCT = r.checkRecover

		r.last.Store(mono.NanoTime())
		// run recovery joggers
		for _, j := range r.rcvyJG {
			go j.run()
		}
	}

	config := cmn.GCO.Get()
	jg := mpather.NewJoggerGroup(opts, config, nil)
	jg.Run()

	select {
	case <-r.ChanAbort():
		jg.Stop()
	case <-jg.ListenFinished():
		err := jg.Stop()
		if err != nil {
			r.AddErr(err)
		}
	}
	if r.checkAndRecover {
		// wait for in-flight and pending recovery
		r.Quiesce(xact.IdleDefault, r._quiesce)
	}
	r.done.Store(true)
	r.wg.Wait() // wait for before/afterEncode

	if !r.IsAborted() {
		for _, j := range r.rcvyJG {
			close(j.workCh)
		}
	}

	r.Finish()

	if a := r.chanFullTotal(); a > 0 {
		nlog.Warningln(r.Name(), "work channel full (final)", a)
	}
}

// at least max-host-busy without Rx or jogger action _prior_ to counting towards timeout
func (r *XactBckEncode) _quiesce(time.Duration) core.QuiRes {
	last := r.last.Load()
	debug.Assert(last != 0)
	if mono.Since(last) < max(xact.IdleDefault>>1, 20*time.Second) {
		return core.QuiActive
	}
	return core.QuiInactiveCB
}

func (r *XactBckEncode) beforeEncode() { r.wg.Add(1) }

func (r *XactBckEncode) afterEncode(lom *core.LOM, err error) {
	if err == nil {
		r.LomAdd(lom)
	} else if err != errSkipped {
		r.AddErr(err)
		nlog.Errorln(r.Name(), "failed to ec-encode", lom.Cname(), "err:", err)
	}
	r.wg.Done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (r *XactBckEncode) encode(lom *core.LOM, _ []byte) error {
	_, local, err := lom.HrwTarget(r.smap)
	if err != nil {
		return err
	}
	// An object replica - skip EC.
	if !local {
		return nil
	}
	mdFQN, _, err := core.HrwFQN(lom.Bck().Bucket(), fs.ECMetaType, lom.ObjName)
	if err != nil {
		nlog.Warningln("failed to generate md FQN for", lom.Cname(), "err:", err)
		return err
	}

	md, err := LoadMetadata(mdFQN)
	// If metafile exists, the object has been already encoded. But for
	// replicated objects we have to fall through. Otherwise, bencode
	// won't recover any missing replicas
	if err == nil && !md.IsCopy {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		nlog.Warningln("failed to fstat", mdFQN, "err:", err)
		if errDel := os.Remove(mdFQN); errDel != nil {
			nlog.Warningln("nested err: failed to delete broken metafile:", errDel)
			return nil
		}
	}

	r.beforeEncode() // (see r.wg.Wait above)
	if err = ECM.EncodeObject(lom, r.afterEncode); err != nil {
		r.afterEncode(lom, err)
		if err != errSkipped {
			return err
		}
	}
	return nil
}

func (r *XactBckEncode) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.Pack(fs.NumAvail(), len(r.rcvyJG), r.chanFullTotal())

	snap.IdleX = r.IsIdle()
	return
}

func (r *XactBckEncode) chanFullTotal() (n int64) {
	for _, j := range r.rcvyJG {
		n += j.chanFull.Load()
	}
	return n
}

// given CT, ask the "main" target to restore the corresponding object and slices, if need be
func (r *XactBckEncode) checkRecover(ct *core.CT, _ []byte) error {
	tsi, err := r.smap.HrwName2T([]byte(*ct.UnamePtr()))
	if err != nil {
		nlog.Errorln(ct.Cname(), "err:", err)
		return err
	}
	if tsi.ID() == core.T.SID() {
		return nil
	}
	return core.T.ECRestoreReq(ct, tsi, r.ID())
}

func (r *XactBckEncode) RecvRecover(lom *core.LOM) {
	r.last.Store(mono.NanoTime())

	uname := lom.UnamePtr()
	bname := cos.UnsafeBptr(uname)
	if r.probFilter.Lookup(*bname) {
		core.FreeLOM(lom)
		return
	}

	r.probFilter.Insert(*bname)
	j, ok := r.rcvyJG[lom.Mountpath().Path]
	if !ok {
		err := errLossMpath(r, lom)
		r.Abort(err)
		return
	}

	if r.done.Load() || r.IsAborted() || r.Finished() {
		core.FreeLOM(lom)
		return
	}

	j.workCh <- lom
}

func (r *XactBckEncode) setLast(lom *core.LOM, err error) {
	r.last.Store(mono.NanoTime())

	switch err {
	case nil:
		r.LomAdd(lom) // TODO: instead, count restored slices, metafiles, possibly - objects
	case ErrorECDisabled:
		r.Abort(err)
	case errSkipped:
		// do nothing
	default:
		r.AddErr(err, 4, cos.SmoduleEC)
	}
}

////////////////
// rcvyJogger //
////////////////

func (j *rcvyJogger) run() {
	var (
		r = j.r
		n int64
	)
	for {
		lom, ok := <-j.workCh
		if !ok || r.done.Load() || r.IsAborted() || r.Finished() {
			break
		}

		l, c := len(j.workCh), cap(j.workCh)
		j.chanFull.Check(l, c)

		err := ECM.Recover(lom)
		r.setLast(lom, err)
		core.FreeLOM(lom)

		n++
		// (compare with ec/putjogger where we also check memory pressure)
		if err == nil && fs.IsThrottle(n) {
			pct, _, _ := fs.ThrottlePct()
			if pct >= fs.MaxThrottlePct {
				time.Sleep(fs.Throttle10ms)
			}
		}
	}
}

func (j *rcvyJogger) String() string {
	return "j-rcvy " + j.r.ID() + "[" + j.mi.String() + "/" + j.r.Bck().Cname("") + "]"
}
