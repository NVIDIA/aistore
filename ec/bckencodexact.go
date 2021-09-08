// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"os"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

type (
	encFactory struct {
		xreg.RenewBase
		xact  *XactBckEncode
		phase string
	}
	XactBckEncode struct {
		xaction.XactBase
		t    cluster.Target
		bck  *cluster.Bck
		wg   *sync.WaitGroup // to wait for EC finishes all objects
		smap *cluster.Smap
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

func (*encFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.ECEncodeArgs)
	p := &encFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, phase: custom.Phase}
	return p
}

func (p *encFactory) Start() error {
	p.xact = newXactBckEncode(p.Bck, p.T, p.UUID())
	return nil
}

func (*encFactory) Kind() string        { return cmn.ActECEncode }
func (p *encFactory) Get() cluster.Xact { return p.xact }

func (p *encFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*encFactory)
	if prev.phase == cmn.ActBegin && p.phase == cmn.ActCommit {
		prev.phase = cmn.ActCommit // transition
		wpr = xreg.WprUse
		return
	}
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", p.Kind(), prev.xact.Bck().Name, prev.phase, p.phase)
	return
}

///////////////////
// XactBckEncode //
///////////////////

func newXactBckEncode(bck *cluster.Bck, t cluster.Target, uuid string) (r *XactBckEncode) {
	r = &XactBckEncode{t: t, bck: bck, wg: &sync.WaitGroup{}, smap: t.Sowner().Get()}
	r.InitBase(uuid, cmn.ActECEncode, bck)
	return
}

func (r *XactBckEncode) Run(wg *sync.WaitGroup) {
	wg.Done()
	bck := r.bck
	if err := bck.Init(r.t.Bowner()); err != nil {
		r.Finish(err)
		return
	}
	if !bck.Props.EC.Enabled {
		r.Finish(fmt.Errorf("bucket %q does not have EC enabled", r.bck.Name))
		return
	}

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:        r.t,
		Bck:      r.bck.Bck,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.bckEncode,
		DoLoad:   mpather.Load,
	})
	jg.Run()

	var err error
	select {
	case <-r.ChanAbort():
		jg.Stop()
		err = cmn.NewErrAborted(r.Name(), "", nil)
	case <-jg.ListenFinished():
		err = jg.Stop()
	}
	r.wg.Wait() // Need to wait for all async actions to finish.

	r.Finish(err)
}

func (r *XactBckEncode) beforeECObj() { r.wg.Add(1) }

func (r *XactBckEncode) afterECObj(lom *cluster.LOM, err error) {
	if err == nil {
		r.ObjectsInc()
		r.BytesAdd(lom.SizeBytes())
	} else if err != errSkipped {
		glog.Errorf("Failed to erasure-code %s: %v", lom.FullName(), err)
	}

	r.wg.Done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (r *XactBckEncode) bckEncode(lom *cluster.LOM, _ []byte) error {
	_, local, err := lom.HrwTarget(r.smap)
	if err != nil {
		glog.Errorf("%s: %s", lom, err)
		return nil
	}
	// An object replica - skip EC.
	if !local {
		return nil
	}
	mdFQN, _, err := cluster.HrwFQN(lom.Bck(), fs.ECMetaType, lom.ObjName)
	if err != nil {
		glog.Warningf("metadata FQN generation failed %q: %v", lom.FQN, err)
		return nil
	}
	_, err = os.Stat(mdFQN)
	// Metadata file exists - the object was already EC'ed before.
	if err == nil {
		return nil
	}
	if !os.IsNotExist(err) {
		glog.Warningf("failed to stat %q: %v", mdFQN, err)
		return nil
	}

	// beforeECObj increases a counter, and callback afterECObj decreases it.
	// After Walk finishes, the xaction waits until counter drops to zero.
	// That means all objects have been processed and xaction can finalize.
	r.beforeECObj()
	if err = ECM.EncodeObject(lom, r.afterECObj); err != nil {
		// Something wrong with EC, interrupt file walk - it is critical.
		r.afterECObj(lom, err)
		if err != errSkipped {
			return fmt.Errorf("failed to EC object %q: %v", lom.FQN, err)
		}
	}
	return nil
}
