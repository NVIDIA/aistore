// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	encFactory struct {
		xreg.RenewBase
		xctn  *XactBckEncode
		phase string
	}
	XactBckEncode struct {
		xact.Base
		bck       *meta.Bck
		wg        *sync.WaitGroup // to wait for EC finishes all objects
		smap      *meta.Smap
		doRecover bool
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
	p.xctn = newXactBckEncode(p.Bck, p.UUID(), custom.Recover)
	return nil
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

func newXactBckEncode(bck *meta.Bck, uuid string, doRecover bool) (r *XactBckEncode) {
	r = &XactBckEncode{bck: bck, wg: &sync.WaitGroup{}, smap: core.T.Sowner().Get(), doRecover: doRecover}
	r.InitBase(uuid, apc.ActECEncode, bck)
	return
}

func (r *XactBckEncode) Run(wg *sync.WaitGroup) {
	wg.Done()
	bck := r.bck
	if err := bck.Init(core.T.Bowner()); err != nil {
		r.AddErr(err)
		r.Finish()
		return
	}
	if !bck.Props.EC.Enabled {
		r.AddErr(fmt.Errorf("%s does not have EC enabled", r.bck.Cname("")))
		r.Finish()
		return
	}

	ECM.incActive(r)

	ctList := []string{fs.ObjectType}
	if r.doRecover {
		ctList = []string{fs.ObjectType, fs.ECMetaType, fs.ECSliceType}
	}
	opts := &mpather.JgroupOpts{
		CTs:      ctList,
		VisitObj: r.bckEncode,
		VisitCT:  r.bckEncodeMD,
		DoLoad:   mpather.LoadUnsafe,
	}
	opts.Bck.Copy(r.bck.Bucket())
	jg := mpather.NewJoggerGroup(opts, cmn.GCO.Get(), nil)
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
	r.wg.Wait() // Need to wait for all async actions to finish.

	r.Finish()
}

func (r *XactBckEncode) beforeECObj() { r.wg.Add(1) }

func (r *XactBckEncode) afterECObj(lom *core.LOM, err error) {
	if err == nil {
		r.LomAdd(lom)
	} else if err != errSkipped {
		nlog.Errorf("failed to erasure-code %s: %v", lom.Cname(), err)
	}

	r.wg.Done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (r *XactBckEncode) bckEncode(lom *core.LOM, _ []byte) error {
	_, local, err := lom.HrwTarget(r.smap)
	if err != nil {
		nlog.Errorf("%s: %s", lom, err)
		return nil
	}
	// An object replica - skip EC.
	if !local {
		return nil
	}
	mdFQN, _, err := core.HrwFQN(lom.Bck().Bucket(), fs.ECMetaType, lom.ObjName)
	if err != nil {
		nlog.Warningln("metadata FQN generation failed", lom, ":", err)
		return nil
	}
	md, err := LoadMetadata(mdFQN)
	// If metafile exists, the object has been already encoded. But for
	// replicated objects we have to fall through. Otherwise, bencode
	// won't recover any missing replicas
	if err == nil && !md.IsCopy {
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		nlog.Warningln("failed to stat ", mdFQN, ":", err, "Deleting...")
		if errDel := os.Remove(mdFQN); errDel != nil {
			nlog.Warningln("failed to delete broken metafile ", mdFQN, ":", errDel)
			return nil
		}
	}

	// beforeECObj increases a counter, and callback afterECObj decreases it.
	// After Walk finishes, the xaction waits until counter drops to zero.
	// That means all objects have been processed and xaction can finalize.
	r.beforeECObj()
	if err = ECM.EncodeObject(lom, r.afterECObj); err != nil {
		// something went wrong: abort xaction
		r.afterECObj(lom, err)
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

// Walks through all metafiles and request the "main" target to restore
// the object if it does not exist
func (r *XactBckEncode) bckEncodeMD(ct *core.CT, _ []byte) error {
	tsi, err := r.smap.HrwName2T([]byte(*ct.UnamePtr()))
	if err != nil {
		nlog.Errorf("%s: %s", ct, err)
		return nil
	}
	// This target is the main one. Skip recovery
	if tsi.ID() == core.T.SID() {
		return nil
	}
	return core.T.ECRestoreReq(ct, tsi)
}
