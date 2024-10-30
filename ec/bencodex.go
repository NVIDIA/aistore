// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"runtime"
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
		parent   *XactBckEncode
		chanFull atomic.Int64
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

func (p *encFactory) Start() (err error) {
	custom := p.Args.Custom.(*xreg.ECEncodeArgs)
	p.xctn, err = newXactBckEncode(p.Bck, p.UUID(), custom.Recover)
	return err
}

func (*encFactory) Kind() string     { return apc.ActECEncode }
func (p *encFactory) Get() core.Xact { return p.xctn }

func (p *encFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*encFactory)
	if prev.phase == apc.ActBegin && p.phase == apc.ActCommit {
		prev.phase = apc.ActCommit // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", p.Kind(), prev.xctn.Bck().Name, prev.phase, p.phase)
	return
}

///////////////////
// XactBckEncode //
///////////////////

func newXactBckEncode(bck *meta.Bck, uuid string, checkAndRecover bool) (r *XactBckEncode, err error) {
	r = &XactBckEncode{
		bck:             bck,
		wg:              &sync.WaitGroup{},
		smap:            core.T.Sowner().Get(),
		checkAndRecover: checkAndRecover,
	}
	if checkAndRecover {
		r.probFilter = prob.NewDefaultFilter()
	}
	r.InitBase(uuid, apc.ActECEncode, bck)

	if err = bck.Init(core.T.Bowner()); err != nil {
		return nil, err
	}
	if !bck.Props.EC.Enabled {
		return nil, fmt.Errorf("EC is disabled for %s", bck.Cname(""))
	}
	avail := fs.GetAvail()
	if len(avail) == 0 {
		return nil, cmn.ErrNoMountpaths
	}
	if r.checkAndRecover {
		// construct recovery joggers
		r.rcvyJG = make(map[string]*rcvyJogger, len(avail))
		for _, mi := range avail {
			j := &rcvyJogger{
				mi:     mi,
				workCh: make(chan *core.LOM, rcvyWorkChanSize),
				parent: r,
			}
			r.rcvyJG[mi.Path] = j
		}
	}

	return r, nil
}

func (r *XactBckEncode) Run(wg *sync.WaitGroup) {
	wg.Done()

	ECM.incActive(r)

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
		r.Quiesce(time.Minute, r._quiesce)
		r.done.Store(true)
	}
	r.wg.Wait() // wait for before/afterEncode

	for _, j := range r.rcvyJG {
		close(j.workCh)
	}

	r.Finish()
}

func (r *XactBckEncode) _quiesce(time.Duration) core.QuiRes {
	if mono.Since(r.last.Load()) > cmn.Rom.MaxKeepalive() {
		return core.QuiDone
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

	snap.IdleX = r.IsIdle()
	return
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
		return
	}

	r.probFilter.Insert(*bname)
	j, ok := r.rcvyJG[lom.Mountpath().Path]
	if !ok {
		err := errLossMpath(r, lom)
		debug.Assert(false, err)
		r.Abort(err)
		return
	}
	if !r.done.Load() {
		j.workCh <- lom
	}
}

func (r *XactBckEncode) setLast(lom *core.LOM, err error) {
	switch {
	case err == nil:
		r.LomAdd(lom) // TODO: instead, count restored slices, metafiles, possibly - objects
		r.last.Store(mono.NanoTime())
	case err == ErrorECDisabled:
		r.Abort(err)
	case err == errSkipped:
		// do nothing
	default:
		r.AddErr(err, 4, cos.SmoduleEC)
	}
}

////////////////
// rcvyJogger //
////////////////

func (j *rcvyJogger) run() {
	var n int64
	for {
		lom, ok := <-j.workCh
		if !ok {
			break
		}
		if l, c := len(j.workCh), cap(j.workCh); l > (c - c>>2) {
			runtime.Gosched() // poor man's throttle
			if l == c {
				j.chanFull.Inc()
			}
		}
		err := ECM.Recover(lom)
		j.parent.setLast(lom, err)
		core.FreeLOM(lom)

		n++
		// (compare with ec/putjogger where we also check memory pressure)
		if err == nil && fs.IsThrottle(n) {
			pct, _, _ := _throttlePct()
			if pct >= maxThrottlePct {
				time.Sleep(fs.Throttle10ms)
			}
		}
	}
	if cnt := j.chanFull.Load(); cnt > 1 {
		nlog.Warningln(j.String(), cos.ErrWorkChanFull, "cnt:", cnt)
	}
}

func (j *rcvyJogger) String() string {
	return fmt.Sprintf("j-rcvy %s[%s/%s]", j.parent.ID(), j.mi, j.parent.Bck())
}

//
// TODO -- FIXME: dedup core/lcache
//

const (
	maxThrottlePct = 60
)

func _throttlePct() (int, int64, float64) {
	var (
		util, lavg = core.T.MaxUtilLoad()
		cpus       = runtime.NumCPU()
		maxload    = max((cpus>>1)-(cpus>>3), 1)
	)
	if lavg >= float64(maxload) {
		return 100, util, lavg
	}
	ru := cos.RatioPct(100, 2, util)
	rl := cos.RatioPct(int64(10*maxload), 1, int64(10*lavg))
	return int(max(ru, rl)), util, lavg
}
