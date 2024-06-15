// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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
		xctn core.Xact
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

func (p *streamingF) Kind() string   { return p.kind }
func (p *streamingF) Get() core.Xact { return p.xctn }

func (p *streamingF) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

// NOTE: transport endpoint (aka "trname") identifies the flow and MUST be identical
// across all participating targets. The mechanism involves generating so-called "best-effort UUID"
// independently on (by) all targets and using the latter as both xaction ID and receive endpoint (trname)
// for target=>target streams.

func (p *streamingF) _tag(fromBck, toBck *meta.Bck) (tag []byte) {
	var (
		from = fromBck.MakeUname("")
		to   = toBck.MakeUname("")
		bmd  = core.T.Bowner().Get()
		l    = cos.PackedStrLen(p.kind) + 1 + cos.PackedBytesLen(from) + 1 + cos.PackedBytesLen(to) + 1 + cos.SizeofI64
		pack = cos.NewPacker(nil, l)
	)
	pack.WriteString(p.kind)
	pack.WriteByte('|')
	pack.WriteBytes(from)
	pack.WriteByte('|')
	pack.WriteBytes(to)
	pack.WriteByte('|')
	pack.WriteInt64(bmd.Version)
	tag = pack.Bytes()
	debug.Assert(len(tag) == l, len(tag), " vs ", l)
	return tag
}

func (p *streamingF) genBEID(fromBck, toBck *meta.Bck) (string, error) {
	var (
		div = uint64(xact.IdleDefault)
		tag = p._tag(fromBck, toBck)
	)
	beid, prev, err := xreg.GenBEID(div, tag)
	if beid != "" {
		debug.Assert(err == nil && prev == nil)
		return beid, nil
	}
	if prev != nil {
		err = cmn.NewErrBusy("node", core.T.String(), "running "+prev.Name())
	}
	return "", err
}

func (p *streamingF) newDM(trname string, recv transport.RecvObj, config *cmn.Config, owt cmn.OWT, sizePDU int32) (err error) {
	smap := core.T.Sowner().Get()
	if err := core.InMaintOrDecomm(smap, core.T.Snode(), p.xctn); err != nil {
		return err
	}
	if smap.CountActiveTs() <= 1 {
		return nil
	}

	// consider adding config.X.Compression, config.X.SbundleMult (currently, always 1), etc.
	dmxtra := bundle.Extra{Config: config, Multiplier: 1, SizePDU: sizePDU}
	p.dm, err = bundle.NewDataMover(trname, recv, owt, dmxtra)
	if err != nil {
		return err
	}
	if err = p.dm.RegRecv(); err == nil {
		return nil
	}

	nlog.Errorln(err)
	sleep := cos.ProbingFrequency(waitRegRecv)
	for total := time.Duration(0); err != nil && transport.IsErrDuplicateTrname(err) && total < waitRegRecv; total += sleep {
		time.Sleep(sleep)
		err = p.dm.RegRecv()
	}
	return err
}

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
	r.p.dm.Close(err)
	r.p.dm.UnregRecv()
	r.AddErr(err)
	r.Base.Finish()
}

func (r *streamingX) sendTerm(uuid string, tsi *meta.Snode, err error) {
	if r.p.dm == nil { // single target
		return
	}
	o := transport.AllocSend()
	o.Hdr.SID = core.T.SID()
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
		r.p.dm.Close(r.Err())
		r.p.dm.UnregRecv()
		return
	}

	r.DemandBase.Stop()
	r.p.dm.Close(r.Err())
	r.Finish()
	if unreg && r.p.dm != nil {
		r.maxWt = 0
		hk.Reg(r.ID()+hk.NameSuffix, r.wurr, waitUnregRecv) // compare w/ lso
	}
}

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
