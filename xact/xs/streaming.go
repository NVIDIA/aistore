// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

//
// multi-object on-demand (transactional) xactions - common logic
//

const (
	opcodeDone = iota + 27182
	opcodeAbrt
)

const (
	waitRegRecv   = 4 * time.Second
	waitUnregRecv = 2 * waitRegRecv
	waitUnregMax  = 2 * waitUnregRecv

	maxNumInParallel = 256
)

type (
	streamingF struct {
		xreg.RenewBase
		xctn cluster.Xact
		dm   *bundle.DataMover
		kind string
	}
	streamingX struct {
		p   *streamingF
		err cos.ErrValue
		xact.DemandBase
		wiCnt atomic.Int32
		maxWt time.Duration
	}
)

//
// (common factory part)
//

func (p *streamingF) Kind() string      { return p.kind }
func (p *streamingF) Get() cluster.Xact { return p.xctn }

func (p *streamingF) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

// NOTE:
// - transport endpoint `trname` identifies the flow and must be identical across all participating targets
// - stream-bundle multiplier always 1 (see also: `SbundleMult`)
func (p *streamingF) newDM(trname string, recv transport.RecvObj, sizePDU int32) (err error) {
	dmxtra := bundle.Extra{Multiplier: 1, SizePDU: sizePDU}
	p.dm, err = bundle.NewDataMover(p.Args.T, trname, recv, cmn.OwtPut, dmxtra)
	if err != nil {
		return
	}
	if err = p.dm.RegRecv(); err != nil {
		var total time.Duration // retry for upto waitRegRecv
		glog.Error(err)
		sleep := cos.ProbingFrequency(waitRegRecv)
		for err != nil && transport.IsErrDuplicateTrname(err) && total < waitRegRecv {
			time.Sleep(sleep)
			total += sleep
			err = p.dm.RegRecv()
		}
	}
	return
}

//
// (common xaction part)
//

func (r *streamingX) String() (s string) {
	s = r.DemandBase.String()
	if r.p.dm == nil {
		return
	}
	return s + "-" + r.p.dm.String()
}

// limited pre-run abort
func (r *streamingX) TxnAbort() {
	err := cmn.NewErrAborted(r.Name(), "txn-abort", nil)
	r.p.dm.CloseIf(err)
	r.p.dm.UnregRecv()
	r.Base.Finish(err)
}

func (r *streamingX) raiseErr(err error, contOnErr bool, errCode ...int) {
	var s string
	if cmn.IsErrAborted(err) {
		if verbose {
			glog.Warningf("%s[%s] aborted", r.p.T, r)
		}
		return
	}
	err = fmt.Errorf("%s[%s]: %w", r.p.T, r, err)
	if len(errCode) > 0 {
		s = fmt.Sprintf(" (%d)", errCode[0])
	}
	if contOnErr {
		glog.WarningDepth(1, err.Error()+s+" - continuing...")
		return
	}
	glog.ErrorDepth(1, err.Error()+s+" - storing...")
	r.err.Store(err)
}

func (r *streamingX) sendTerm(uuid string, tsi *cluster.Snode, err error) {
	o := transport.AllocSend()
	o.Hdr.SID = r.p.T.SID()
	o.Hdr.Opaque = []byte(uuid)
	if err == nil {
		o.Hdr.Opcode = opcodeDone
	} else {
		o.Hdr.Opcode = opcodeAbrt
		o.Hdr.ObjName = err.Error()
	}
	if tsi != nil {
		r.p.dm.Send(o, nil, tsi) // to the responsible target
	} else {
		r.p.dm.Bcast(o, nil) // to all
	}
}

func (r *streamingX) fin(err error, unreg bool) error {
	if r.DemandBase.Finished() {
		// must be aborted
		r.p.dm.CloseIf(err)
		r.p.dm.UnregRecv()
		return err
	}

	r.DemandBase.Stop()
	if err == nil {
		err = r.err.Err()
	}
	if err == nil {
		err = r.AbortErr()
	}
	if err != nil {
		err = cmn.NewErrAborted(r.Name(), "streaming-fin", err)
	}
	r.p.dm.Close(err)
	r.Finish(err)

	if unreg {
		r.maxWt = 0
		r.postponeUnregRx()
	}
	return err
}

// compare w/ lso
func (r *streamingX) postponeUnregRx() { hk.Reg(r.ID()+hk.NameSuffix, r.wurr, waitUnregRecv) }

func (r *streamingX) wurr() time.Duration {
	if cnt := r.wiCnt.Load(); cnt > 0 {
		r.maxWt += waitUnregRecv
		if r.maxWt < waitUnregMax {
			return waitUnregRecv
		}
		glog.Errorf("%s: unreg timeout %v, cnt %d", r, r.maxWt, cnt)
	}
	r.p.dm.UnregRecv()
	return hk.UnregInterval
}
