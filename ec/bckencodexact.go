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
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	// Implements `xreg.BucketEntryProvider` and `xreg.BucketEntry` interface.
	xactBckEncodeProvider struct {
		xreg.BaseBckEntry
		xact *XactBckEncode

		t     cluster.Target
		uuid  string
		phase string
	}

	XactBckEncode struct {
		xaction.XactBase
		t    cluster.Target
		bck  cmn.Bck
		wg   *sync.WaitGroup // to wait for EC finishes all objects
		smap *cluster.Smap
	}
)

// interface guard
var _ cluster.Xact = (*XactBckEncode)(nil)

func (*xactBckEncodeProvider) New(args xreg.XactArgs) xreg.BucketEntry {
	return &xactBckEncodeProvider{
		t:     args.T,
		uuid:  args.UUID,
		phase: args.Phase,
	}
}

func (p *xactBckEncodeProvider) Start(bck cmn.Bck) error {
	p.xact = NewXactBckEncode(bck, p.t, p.uuid)
	return nil
}
func (*xactBckEncodeProvider) Kind() string        { return cmn.ActECEncode }
func (p *xactBckEncodeProvider) Get() cluster.Xact { return p.xact }
func (p *xactBckEncodeProvider) PreRenewHook(previousEntry xreg.BucketEntry) (keep bool, err error) {
	// TODO: add more checks?
	prev := previousEntry.(*xactBckEncodeProvider)
	if prev.phase == cmn.ActBegin && p.phase == cmn.ActCommit {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s, phase %s): cannot %s", p.Kind(), prev.xact.Bck().Name, prev.phase, p.phase)
	return
}

func NewXactBckEncode(bck cmn.Bck, t cluster.Target, uuid string) *XactBckEncode {
	return &XactBckEncode{
		XactBase: *xaction.NewXactBaseBck(uuid, cmn.ActECEncode, bck),
		t:        t,
		bck:      bck,
		wg:       &sync.WaitGroup{},
		smap:     t.Sowner().Get(),
	}
}

func (r *XactBckEncode) Run() (err error) {
	bck := cluster.NewBckEmbed(r.bck)
	if err := bck.Init(r.t.Bowner(), r.t.Snode()); err != nil {
		r.Abort()
		return err
	}
	if !bck.Props.EC.Enabled {
		r.Abort()
		return fmt.Errorf("bucket %q does not have EC enabled", r.bck.Name)
	}

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:        r.t,
		Bck:      r.bck,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.bckEncode,
		DoLoad:   mpather.Load,
	})
	jg.Run()

	select {
	case <-r.ChanAbort():
		jg.Stop()
		err = fmt.Errorf("%s aborted, exiting", r)
	case <-jg.ListenFinished():
		err = jg.Stop()
	}
	r.wg.Wait() // Need to wait for all async actions to finish.

	r.Finish(err)
	return
}

func (r *XactBckEncode) beforeECObj() { r.wg.Add(1) }
func (r *XactBckEncode) afterECObj(lom *cluster.LOM, err error) {
	if err == nil {
		r.ObjectsInc()
		r.BytesAdd(lom.Size())
	} else {
		glog.Errorf("Failed to EC object %s/%s: %v", lom.BckName(), lom.ObjName, err)
	}

	r.wg.Done()
}

// Walks through all files in 'obj' directory, and calls EC.Encode for every
// file whose HRW points to this file and the file does not have corresponding
// metadata file in 'meta' directory
func (r *XactBckEncode) bckEncode(lom *cluster.LOM, _ []byte) error {
	si, err := cluster.HrwTarget(lom.Uname(), r.smap)
	if err != nil {
		glog.Errorf("%s: %s", lom, err)
		return nil
	}

	// An object replica - skip EC.
	if r.t.Snode().ID() != si.ID() {
		return nil
	}

	mdFQN, _, err := cluster.HrwFQN(lom.Bck(), MetaType, lom.ObjName)
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
		return fmt.Errorf("failed to EC object %q: %v", lom.FQN, err)
	}
	return nil
}
