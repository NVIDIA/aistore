// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	mossFactory struct {
		xreg.RenewBase
		xctn *xactMoss
	}
	xactMoss struct {
		xact.DemandBase
		recvCount int64
	}
)

// interface guard
var (
	_ core.Xact      = (*xactMoss)(nil)
	_ xreg.Renewable = (*mossFactory)(nil)
)

func (*mossFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &mossFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
}

func (p *mossFactory) Start() error {
	debug.Assert(cos.IsValidUUID(p.Args.UUID), p.Args.UUID)
	p.xctn = newMoss(p)
	xact.GoRunW(p.xctn)
	return nil
}

func (*mossFactory) Kind() string     { return apc.ActGetBatch }
func (p *mossFactory) Get() core.Xact { return p.xctn }

func (*mossFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

//
// xactMoss implementation
//

func newMoss(p *mossFactory) *xactMoss {
	r := &xactMoss{}
	r.DemandBase.Init(p.UUID(), p.Kind(), "" /*ctlmsg*/, p.Bck, xact.IdleDefault) // DEBUG
	return r
}

func (r *xactMoss) Name() string {
	return fmt.Sprintf("%s[%s]", r.Kind(), r.ID())
}

func (r *xactMoss) Run(wg *sync.WaitGroup) {
	nlog.InfoDepth(1, r.Name(), "starting")

	wg.Done()

	if err := bundle.SDM.Open(); err != nil {
		r.AddErr(err, 5, cos.SmoduleXs)
		return
	}

	bundle.SDM.RegRecv(r.ID(), r.recv)
	defer bundle.SDM.UnregRecv(r.ID())

	// DEBUG
	go r.runTestSimulation()

	for !r.IsAborted() && !r.Finished() {
		time.Sleep(100 * time.Millisecond)
	}

	nlog.InfoDepth(1, r.Name(), "finished, received", r.recvCount, "objects")
}

func (r *xactMoss) recv(hdr *transport.ObjHdr, reader io.Reader, err error) error {
	if err != nil {
		nlog.ErrorDepth(1, r.Name(), "recv error:", err)
		return err
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		nlog.ErrorDepth(1, r.Name(), "failed to read data:", err)
		return err
	}

	r.recvCount++
	nlog.InfoDepth(1, r.Name(), "received object:", hdr.ObjName, "size:", len(data), "data:", string(data))

	return nil
}

// DEBUG: ==========================================================================

func (r *xactMoss) runTestSimulation() {
	time.Sleep(100 * time.Millisecond) // Let the registration settle

	for i := range 10 {
		if r.IsAborted() {
			break
		}

		hdr := &transport.ObjHdr{
			ObjName: fmt.Sprintf("test-obj-%d", i),
			Opaque:  []byte(r.ID()), // This is the key part - xaction ID routing
		}
		reader := strings.NewReader(fmt.Sprintf("test-data-%d-from-moss-xaction", i))

		err := bundle.SDM.RecvDEBUG(hdr, reader, nil)
		if err != nil {
			nlog.WarningDepth(1, r.Name(), "test recv failed:", err)
			r.AddErr(err, 5, cos.SmoduleXs)
		}

		time.Sleep(time.Second)
	}

	r.Finish()
}

// e.o. DEBUG: ==========================================================================

func (r *xactMoss) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)
	snap.IdleX = r.IsIdle()
	return
}
