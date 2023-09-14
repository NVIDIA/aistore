// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		p      *streamingF
		config *cmn.Config
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

// NOTE: transport endpoint (aka "trname") identifies the flow and MUST be identical
// across all participating targets. The mechanism involves generating so-called "best-effort UUID"
// independently on (by) all targets and using the latter as both xaction ID and receive endpoint (trname)
// for target=>target streams.

func (p *streamingF) genBEID(fromBck, toBck *meta.Bck) (string, error) {
	var (
		div = uint64(xact.IdleDefault)
		bmd = p.Args.T.Bowner().Get()
		tag = p.kind + "|" + fromBck.MakeUname("") + "|" + toBck.MakeUname("") + "|" + strconv.FormatInt(bmd.Version, 10)
	)
	beid, prev, err := xreg.GenBEID(div, tag)
	if beid != "" {
		debug.Assert(err == nil && prev == nil)
		return beid, nil
	}
	if prev != nil {
		err = fmt.Errorf("node %s is currently busy (running %s), please try again in one minute", p.Args.T, prev.Name())
	}
	return "", err
}

// transport (below, via bundle.Extra) can be extended with:
// - Compression: config.X.Compression
// - Multiplier:  config.X.SbundleMult (currently, always 1)

func (p *streamingF) newDM(trname string, recv transport.RecvObj, sizePDU int32) (err error) {
	dmxtra := bundle.Extra{Multiplier: 1, SizePDU: sizePDU}

	p.dm, err = bundle.NewDataMover(p.Args.T, trname, recv, cmn.OwtPut, dmxtra)
	if err != nil {
		return
	}
	if err = p.dm.RegRecv(); err != nil {
		var total time.Duration // retry for upto waitRegRecv
		nlog.Errorln(err)
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
func (r *streamingX) TxnAbort(err error) {
	err = cmn.NewErrAborted(r.Name(), "txn-abort", err)
	r.p.dm.CloseIf(err)
	r.p.dm.UnregRecv()
	r.AddErr(err)
	r.Base.Finish()
}

func (r *streamingX) addErr(err error, contOnErr bool, errCode ...int) {
	if r.config.FastV(5, cos.SmoduleXs) {
		nlog.InfoDepth(1, "Error: ", err, errCode)
	}
	if contOnErr {
		// TODO -- FIXME: niy
		debug.Assert(!cmn.IsErrAborted(err))
	} else {
		r.AddErr(err)
	}
}

func (r *streamingX) sendTerm(uuid string, tsi *meta.Snode, err error) {
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

func (r *streamingX) fin(unreg bool) {
	if r.DemandBase.Finished() {
		// must be aborted
		r.p.dm.CloseIf(r.Err())
		r.p.dm.UnregRecv()
		return
	}

	r.DemandBase.Stop()
	r.p.dm.Close(r.Err())
	r.Finish()
	if unreg {
		r.maxWt = 0
		r.postponeUnregRx()
	}
}

// compare w/ lso
func (r *streamingX) postponeUnregRx() { hk.Reg(r.ID()+hk.NameSuffix, r.wurr, waitUnregRecv) }

func (r *streamingX) wurr() time.Duration {
	if cnt := r.wiCnt.Load(); cnt > 0 {
		r.maxWt += waitUnregRecv
		if r.maxWt < waitUnregMax {
			return waitUnregRecv
		}
		nlog.Errorf("%s: unreg timeout %v, cnt %d", r, r.maxWt, cnt)
	}
	r.p.dm.UnregRecv()
	return hk.UnregInterval
}
