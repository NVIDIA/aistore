// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/binary"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// NOTE the limitation:
// - sentinels and workers require DM
// - DM is not available when explicitly disabled (below) and for a single-node cluster

type (
	tcbFactory struct {
		xreg.RenewBase
		xctn *XactTCB
		kind string
	}
	tcbworker struct {
		r *XactTCB
	}
	XactTCB struct {
		// function: copy/transform
		copier
		// function: etl
		transform etl.Session // used for aborting websocket connections
		// args
		args *xreg.TCBArgs
		dm   *bundle.DM
		nam  string
		// function: sync
		prune prune
		// copying parallelism
		nwp struct {
			workCh   chan core.LIF
			stopCh   *cos.StopCh
			workers  []tcbworker
			wg       sync.WaitGroup
			chanFull cos.ChanFull
		}
		// function: coordinate finish, abort, progress
		sntl sentinel
		// mountpath joggers
		xact.BckJog
		// details
		owt cmn.OWT
	}
)

// interface guard
var (
	_ core.Xact      = (*XactTCB)(nil)
	_ xreg.Renewable = (*tcbFactory)(nil)
)

////////////////
// tcbFactory //
////////////////

func (p *tcbFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	return &tcbFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind}
}

func (p *tcbFactory) Start() error {
	xctn, err := newXactTCB(p.UUID(), p.kind, p.Args.Custom.(*xreg.TCBArgs))
	if err != nil {
		return err
	}
	p.xctn = xctn
	return nil
}

func newXactTCB(uuid, kind string, args *xreg.TCBArgs) (*XactTCB, error) {
	var (
		smap      = core.T.Sowner().Get()
		nat       = smap.CountActiveTs()
		config    = cmn.GCO.Get()
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // estimate
		r         = &XactTCB{args: args}
		msg       = args.Msg
	)
	debug.AssertNoErr(err)

	r.init(uuid, kind, slab, config, smap, nat)

	r.owt = cmn.OwtCopy
	if kind == apc.ActETLBck {
		r.owt = cmn.OwtTransform
		r.copier.getROC, r.copier.xetl, r.transform, err = etl.GetOfflineTransform(args.Msg.Transform.Name, r)
		if err != nil {
			return nil, err
		}
		if r.transform != nil {
			r.putWOC = r.transform.OfflineWrite
		}
	}

	if err := core.InMaintOrDecomm(smap, core.T.Snode(), r); err != nil {
		return nil, err
	}

	// single-node cluster
	if nat <= 1 {
		return r, nil // ---->
	}

	// - unlike tco and other lrit-based xactions,
	//   tcb - via xact.BckJog - employs conventional mountpath joggers;
	// - `nwpNone` (serial execution) not supported;
	// - num-workers cannot be less than the number of mountpaths (and joggers)
	// - not using any workers - is the supported default.

	if msg.NumWorkers > 0 {
		var (
			l = fs.NumAvail()
			n = max(msg.NumWorkers, l)
		)
		numWorkers, err := clampNumWorkers(r.Name(), n, l)
		if err != nil {
			return nil, err
		}
		if n != numWorkers {
			nlog.Warningln(r.Name(), "throttle num-workers:", numWorkers, "[ from", n, "]")
		}
		if numWorkers >= l {
			// delegate intra-cluster copying/transforming to additional workers;
			// run them in parallel with traversing joggers;
			r._iniNwp(numWorkers)
		} else {
			nlog.Warningln(r.Name(), "workers: 0 (ignoring ", msg.NumWorkers, "under load)")
		}
	}

	// TODO: Revisit `r.args.DisableDM` — consider removing it.
	// Previously, if the ETL transformer supported direct put, we skipped initializing the data mover
	// to avoid unnecessary setup, since transformed objects were directly delivered to targets.
	// We used `r.args.DisableDM` (derived from the ETL’s `directPut` flag) to skip DM initialization,
	// which also bypassed the multi-worker setup below.
	// But now that sentinels requires the data mover to broadcast control messages, DM is always required.
	if r.args.DisableDM {
		return r, nil
	}

	// data mover and sentinel
	// TODO: add ETL capability to provide Size(transformed-result)
	var sizePDU int32
	if kind == apc.ActETLBck {
		sizePDU = memsys.DefaultBufSize // `transport` to generate PDU-based traffic
	}
	if err := r.newDM(sizePDU); err != nil {
		return nil, err
	}

	// sentinels, to coordinate finishing, aborting, and progress;
	// use DM to communicate sentinel opcodes (transport.OpcDone, transport.OpcAbort, ...)
	r.sntl.init(r, smap, nat)
	return r, nil
}

func (r *XactTCB) _iniNwp(numWorkers int) {
	r.nwp.workers = make([]tcbworker, 0, numWorkers)
	for range numWorkers {
		r.nwp.workers = append(r.nwp.workers, tcbworker{r})
	}
	chsize := cos.ClampInt(numWorkers*nwpBurstMult, r.Config.TCB.Burst, nwpBurstMax)
	r.nwp.workCh = make(chan core.LIF, chsize)
	r.nwp.stopCh = cos.NewStopCh()
	nlog.Infoln(r.Name(), "workers:", numWorkers)
}

func (p *tcbFactory) Kind() string   { return p.kind }
func (p *tcbFactory) Get() core.Xact { return p.xctn }

func (p *tcbFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*tcbFactory)
	if p.UUID() != prev.UUID() {
		return wpr, cmn.NewErrXactUsePrev(prevEntry.Get().String())
	}
	bckEq := prev.xctn.args.BckFrom.Equal(p.xctn.args.BckFrom, true /*same BID*/, true /*same backend*/)
	debug.Assert(bckEq)
	return xreg.WprUse, nil
}

/////////////
// XactTCB copies one bucket _into_ another with or without transformation
/////////////

func (r *XactTCB) newDM(sizePDU int32) error {
	const trname = "tcb-"
	config := r.BckJog.Config
	dmExtra := bundle.Extra{
		RecvAck:     nil, // no ACKs
		Config:      config,
		Compression: config.TCB.Compression,
		Multiplier:  config.TCB.SbundleMult,
		SizePDU:     sizePDU,
	}
	// in re cmn.OwtPut: see comment inside _recv()
	dm := bundle.NewDM(trname+r.ID(), r.recv, r.owt, dmExtra)
	if err := dm.RegRecv(); err != nil {
		return err
	}
	dm.SetXact(r)
	r.dm = dm

	return nil
}

// limited pre-run abort
func (r *XactTCB) TxnAbort(err error) {
	err = cmn.NewErrAborted(r.Name(), "tcb: txn-abort", err)
	if r.dm != nil {
		r.dm.Close(err)
		r.dm.UnregRecv()
	}
	r.AddErr(err)
	if r.transform != nil {
		r.transform.Finish(err)
	}
	r.Base.Finish()
}

func (r *XactTCB) init(uuid, kind string, slab *memsys.Slab, config *cmn.Config, smap *meta.Smap, nat int) {
	var (
		args   = r.args
		msg    = r.args.Msg
		mpopts = &mpather.JgroupOpts{
			CTs:      []string{fs.ObjCT},
			VisitObj: r.do,
			Prefix:   msg.Prefix,
			Slab:     slab,
			DoLoad:   mpather.Load,
			RW:       true,
		}
	)
	mpopts.Bck.Copy(args.BckFrom.Bucket())

	// init base
	r.BckJog.Init(uuid, kind, args.BckTo, mpopts, config)

	// xname
	fromCname := args.BckFrom.Cname(msg.Prefix)
	toCname := args.BckTo.Cname(msg.Prepend)
	r._name(fromCname, toCname, r.BckJog.NumJoggers())

	r.rate.init(args.BckFrom, args.BckTo, nat)

	if msg.Sync {
		debug.Assert(msg.Prepend == "", msg.Prepend) // validated (cli, P)
		{
			r.prune.r = r
			r.prune.smap = smap
			r.prune.bckFrom = args.BckFrom
			r.prune.bckTo = args.BckTo
			r.prune.prefix = msg.Prefix
		}
		r.prune.init(config)
	}

	r.copier.r = r

	debug.Assert(args.BckFrom.Props != nil)
	// (rgetstats)
	if bck := args.BckFrom; bck.IsRemote() {
		r.bp = core.T.Backend(bck)
	}
	r.vlabs = map[string]string{
		stats.VlabBucket: args.BckFrom.Cname(""),
		stats.VlabXkind:  r.Kind(),
	}
}

func (r *XactTCB) ctlmsg(rename bool) string {
	var (
		sb        strings.Builder
		msg       = r.args.Msg
		fromCname = r.args.BckFrom.Cname(msg.Prefix)
		toCname   = r.args.BckTo.Cname(msg.Prepend)
		tag       string
	)
	switch {
	case rename:
		tag = "mv: "
	case r.Kind() == apc.ActETLBck:
		tag = "etl: "
	default:
		tag = "cp: "
	}

	sb.Grow(80)
	msg.Str(&sb, fromCname, toCname, tag)
	return sb.String()
}

// sub-routine that does the core copy bucket logic; doesn't include the Finish() call and abort check
func (r *XactTCB) run(wg *sync.WaitGroup) {
	// make sure `nat` hasn't changed between Start and now (highly unlikely)
	if r.dm != nil {
		smap := core.T.Sowner().Get()
		if err := r.sntl.checkSmap(smap, nil); err != nil {
			r.Abort(err)
			wg.Done()
			return
		}

		r.dm.SetXact(r)
		r.dm.Open()
	}
	wg.Done()

	for _, worker := range r.nwp.workers {
		buf, slab := core.T.PageMM().Alloc()
		r.nwp.wg.Add(1)
		go worker.run(buf, slab)
	}

	// run
	r.BckJog.Run()
	if r.args.Msg.Sync {
		r.prune.run() // the 2nd jgroup
	}
	nlog.Infoln(core.T.String(), "run:", r.Name())

	errJog := r.BckJog.Wait()
	if errJog != nil && !r.IsAborted() {
		nlog.Warningln(r.Name(), errJog, "- benign?")
	}

	if r.nwp.workers != nil {
		// at this point, we are done with all do() calls on the workers
		close(r.nwp.workCh)
		r.nwp.wg.Wait()
	}

	if r.dm != nil {
		abortErr := r.AbortErr()
		r.sntl.bcast("", r.dm, abortErr) // broadcast: done | abort
		if abortErr == nil {             // done
			r.sntl.initLast(mono.NanoTime())
			qui := r.Base.Quiesce(r.qival(), r.qcb) // when done: wait for others
			if qui == core.QuiAborted {
				err := r.AbortErr()
				debug.Assert(err != nil)
				r.sntl.bcast("", r.dm, err) // broadcast: abort
			}
		}
		// close
		r.dm.Close(r.AbortErr())
		r.dm.UnregRecv()
	}
	if r.args.Msg.Sync {
		// TODO -- FIXME: revisit stopCh and related
		r.prune.wait()
	}

	// finish the ETL session, if any
	if r.transform != nil {
		r.transform.Finish(nil)
	}

	r.sntl.cleanup()
}

func (r *XactTCB) Run(wg *sync.WaitGroup) {
	r.run(wg)
	r.Finish()

	if a := r.nwp.chanFull.Load(); a > 0 {
		nlog.Warningln(r.Name(), "work channel full (final)", a)
	}
}

func (r *XactTCB) qival() time.Duration {
	return cos.ClampDuration(r.Config.Timeout.MaxHostBusy.D(), 10*time.Second, time.Minute)
}

func (r *XactTCB) qcb(tot time.Duration) core.QuiRes {
	nwait := r.sntl.pend.n.Load()
	if nwait > 0 {
		// have "pending" targets
		progressTimeout := max(r.Config.Timeout.SendFile.D(), time.Minute)
		return r.sntl.qcb(r.dm, tot, r.qival(), progressTimeout, r.ErrCnt())
	}
	return core.QuiDone
}

func (r *XactTCB) do(lom *core.LOM, buf []byte) error {
	if r.nwp.workers == nil {
		args := r.args // TCBArgs
		a, err := r.copier.prepare(lom, args.BckTo, args.Msg, r.Config, buf, r.owt)
		if err != nil {
			return err
		}

		err = r.copier.do(a, lom, r.dm)
		if err == nil && args.Msg.Sync {
			r.prune.filter.Insert(cos.UnsafeB(lom.Uname()))
		}
		return err
	}

	l, c := len(r.nwp.workCh), cap(r.nwp.workCh)
	r.nwp.chanFull.Check(l, c)

	r.nwp.workCh <- lom.LIF()

	return nil
}

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCB) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsOkEOF(err) {
		nlog.Errorln(err)
		return err
	}

	// control
	if hdr.Opcode != 0 {
		switch hdr.Opcode {
		case transport.OpcDone:
			r.sntl.rxDone(hdr)
		case transport.OpcAbort:
			r.sntl.rxAbort(hdr)
		case transport.OpcRequest:
			o := transport.AllocSend()
			o.Hdr.Opcode = transport.OpcResponse
			b := make([]byte, cos.SizeofI64)
			binary.BigEndian.PutUint64(b, uint64(r.BckJog.NumVisits())) // report progress
			o.Hdr.Opaque = b
			r.dm.Bcast(o, nil) // TODO: consider limiting this broadcast to only quiescing (waiting) targets
		case transport.OpcResponse:
			r.sntl.rxProgress(hdr) // handle response: progress by others
		default:
			return abortOpcode(r, hdr.Opcode)
		}
		return nil
	}

	// data
	lom := core.AllocLOM(hdr.ObjName)
	err = r._recv(hdr, objReader, lom)
	core.FreeLOM(lom)
	transport.DrainAndFreeReader(objReader)
	return err
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactTCB) _recv(hdr *transport.ObjHdr, objReader io.Reader, lom *core.LOM) error {
	if err := lom.InitBck(&hdr.Bck); err != nil {
		r.AddErr(err, 0)
		return err
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.Cksum = cos.NewCksum(lom.CksumType(), "") // respect the destination bucket checksum type
		params.Xact = r
		params.Size = hdr.ObjAttrs.Size
		params.OWT = r.owt
	}
	if lom.AtimeUnix() == 0 {
		// TODO: sender must be setting it, remove this `if` when fixed
		lom.SetAtimeUnix(time.Now().UnixNano())
	}
	params.Atime = lom.Atime()

	erp := core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if erp != nil {
		r.AddErr(erp, 0)
		return erp // NOTE: non-nil signals transport to terminate
	}

	return nil
}

func (r *XactTCB) Abort(err error) bool {
	if !r.Base.Abort(err) { // already aborted?
		return false
	}
	if r.nwp.stopCh != nil {
		debug.Assert(r.nwp.workers != nil)
		r.nwp.stopCh.Close()
	}
	return true
}

func (r *XactTCB) Args() *xreg.TCBArgs { return r.args }

func (r *XactTCB) _name(fromCname, toCname string, numJoggers int) {
	var sb strings.Builder
	sb.Grow(80)
	sb.WriteString(r.Base.Cname())
	sb.WriteString("-p") // as in: "parallelism"
	sb.WriteString(strconv.Itoa(numJoggers))
	sb.WriteByte('-')
	sb.WriteString(fromCname)
	sb.WriteString("=>")
	sb.WriteString(toCname)

	r.nam = sb.String()
}

func (r *XactTCB) String() string { return r.nam }
func (r *XactTCB) Name() string   { return r.nam }

func (r *XactTCB) FromTo() (*meta.Bck, *meta.Bck) {
	return r.args.BckFrom, r.args.BckTo
}

func (r *XactTCB) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.AddBaseSnap(snap)

	snap.SetCtlMsg(r.Name(), r.ctlmsg(false))
	snap.Pack(fs.NumAvail(), len(r.nwp.workers), r.nwp.chanFull.Load())

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}

///////////////
// tcbworker //
///////////////

func (worker *tcbworker) run(buf []byte, slab *memsys.Slab) {
	p := &worker.r.nwp
outer:
	for {
		select {
		case lif, ok := <-p.workCh:
			if !ok {
				break outer
			}
			if aborted := worker.do(lif, buf); aborted {
				break outer
			}
		case <-p.stopCh.Listen():
			break outer
		}
	}
	p.wg.Done()
	slab.Free(buf)
}

func (worker *tcbworker) do(lif core.LIF, buf []byte) bool {
	var (
		r    = worker.r
		args = r.args // TCBArgs
	)
	lom, err := lif.LOM()
	if err != nil {
		nlog.Warningln(r.Name(), lif.Name(), err)
		r.Abort(err)
		return true
	}

	a, err := r.copier.prepare(lom, args.BckTo, args.Msg, r.Config, buf, r.owt)
	if err != nil {
		return true
	}
	if err := r.copier.do(a, lom, r.dm); err != nil {
		return r.IsAborted()
	}
	if args.Msg.Sync {
		r.prune.filter.Insert(cos.UnsafeB(lom.Uname()))
	}
	return false
}
