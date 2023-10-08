// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		t    cluster.Target
		bck  *meta.Bck
		wg   *sync.WaitGroup // to wait for EC finishes all objects
		smap *meta.Smap
	}
)

// interface guard
var (
	_ cluster.Xact   = (*XactBckEncode)(nil)
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
	p.xctn = newXactBckEncode(p.Bck, p.T, p.UUID())
	return nil
}

func (*encFactory) Kind() string        { return apc.ActECEncode }
func (p *encFactory) Get() cluster.Xact { return p.xctn }

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

func newXactBckEncode(bck *meta.Bck, t cluster.Target, uuid string) (r *XactBckEncode) {
	r = &XactBckEncode{t: t, bck: bck, wg: &sync.WaitGroup{}, smap: t.Sowner().Get()}
	r.InitBase(uuid, apc.ActECEncode, bck)
	return
}

func (r *XactBckEncode) Run(wg *sync.WaitGroup) {
	wg.Done()
	bck := r.bck
	if err := bck.Init(r.t.Bowner()); err != nil {
		r.AddErr(err)
		r.Finish()
		return
	}
	if !bck.Props.EC.Enabled {
		r.AddErr(fmt.Errorf("bucket %q does not have EC enabled", r.bck.Name))
		r.Finish()
		return
	}

	opts := &mpather.JgroupOpts{
		T:        r.t,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.bckEncode,
		DoLoad:   mpather.LoadUnsafe,
	}
	opts.Bck.Copy(r.bck.Bucket())
	jg := mpather.NewJoggerGroup(opts)
	jg.Run()

	select {
	case <-r.ChanAbort():
		jg.Stop()
	case <-jg.ListenFinished():
		err := jg.Stop()
		r.AddErr(err)
	}
	r.wg.Wait() // Need to wait for all async actions to finish.

	r.Finish()
}

func (r *XactBckEncode) beforeECObj() { r.wg.Add(1) }

func (r *XactBckEncode) afterECObj(lom *cluster.LOM, err error) {
	if err == nil {
		r.LomAdd(lom)
	} else if err != errSkipped {
		nlog.Errorf("Failed to erasure-code %s: %v", lom.Cname(), err)
	}

	r.wg.Done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (r *XactBckEncode) bckEncode(lom *cluster.LOM, _ []byte) error {
	_, local, err := lom.HrwTarget(r.smap)
	if err != nil {
		nlog.Errorf("%s: %s", lom, err)
		return nil
	}
	// An object replica - skip EC.
	if !local {
		return nil
	}
	mdFQN, _, err := cluster.HrwFQN(lom.Bck().Bucket(), fs.ECMetaType, lom.ObjName)
	if err != nil {
		nlog.Warningf("metadata FQN generation failed %q: %v", lom, err)
		return nil
	}
	err = cos.Stat(mdFQN)
	// Metadata file exists - the object was already EC'ed before.
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		nlog.Warningf("failed to stat %q: %v", mdFQN, err)
		return nil
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

func (r *XactBckEncode) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
