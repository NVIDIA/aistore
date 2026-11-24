// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// - designated target: no time limit while a peer is alive and not 'done', and none for a receive in progress
//   (a frozen peer keeps the original of in-place TAR append in the work file)
// - Abort2PC: abort the transaction, not the job - currently, TxnAbort finishes the shared job (all shards);
//   instead, remove and abandon this transaction's work item (see ais/tgttxn.go)
// - hold pending from begin to commit - otherwise, idle can stop the job in between and drop the commit
// - begin while stopping: renew and retry (ais/tgttxn.go) rather than fail the request
// - in-place TAR append: checksum
// - control: in-stream 'done' => out-of-band T2T; consolidate _recv w/ tcb, tcobjs (see xact/xs/TODO.md)
// - multi-worker list-range iterator (currently, xact.NwpNone)

type archwiState uint8

const (
	archBegun archwiState = iota
	archCommitted
	archStarted
)

type (
	archFactory struct {
		streamingF
	}
	// archival work item; implements lrwi
	archwi struct {
		wfh       cos.LomWriter // -> workFQN
		writer    archive.Writer
		r         *XactArch
		msg       *cmn.ArchiveBckMsg
		tsi       *meta.Snode
		archlom   *core.LOM
		j         *jogger
		fqn       string // workFQN --/--
		cksum     cos.CksumHashSize
		appendPos int64 // append to existing (tar only)
		lrp       int
		state     archwiState  // guarded by pending.mtx until detached
		cnt       atomic.Int32 // num archived (sender: num sent)
		peersLeft atomic.Int32 // DT: peers yet to report 'done'
		nrx       atomic.Int64 // DT: reported sends minus completed receives
		rxRefs    atomic.Int32 // DT: pending owner + active receives
		werr      atomic.Bool  // DT: fatal write error
	}
	archtask struct {
		wi   *archwi
		lrit *lrit
	}
	jogger struct {
		r        *XactArch
		mi       *fs.Mountpath
		workCh   chan *archtask
		chanFull cos.ChanFull
	}
)

type (
	XactArch struct {
		bckTo   *meta.Bck
		smap    *meta.Smap
		joggers struct {
			m   map[string]*jogger
			wg  sync.WaitGroup
			mtx sync.RWMutex
		}
		pending struct {
			m   map[string]*archwi
			mtx sync.Mutex
		}
		// coordinate finish, abort, progress (compare w/ tcb.go, tcobjs.go)
		sntl sentinel
		streamingX
	}
)

// interface guard
var (
	_ core.Xact      = (*XactArch)(nil)
	_ xreg.Renewable = (*archFactory)(nil)
	_ lrwi           = (*archwi)(nil)
)

/////////////////
// archFactory //
/////////////////

func (*archFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	p := &archFactory{streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: apc.ActArchive}}
	return p
}

// construct, initialize xact.Demand (and reg w/ HK), open streams
// (the renewal caller does xact.GoRunW)
func (p *archFactory) Start() (err error) {
	//
	// target-local generation of a global UUID
	//
	bckTo, ok := p.Args.Custom.(*meta.Bck)
	debug.Assertf(ok, "%+v", bckTo)
	if !ok || bckTo.IsEmpty() {
		bckTo = &meta.Bck{Name: "any"} // local usage to gen uuid, see r.bckTo below
	}
	p.Args.UUID, err = p.genBEID(p.Bck, bckTo)
	if err != nil {
		return err
	}

	// new x-archive
	var (
		config = cmn.GCO.Get()
		burst  = max(cmn.XactBurstDflt, config.Arch.Burst)
		r      = &XactArch{streamingX: streamingX{p: &p.streamingF, config: config}}
	)
	r.pending.m = make(map[string]*archwi, burst)

	r.joggers.m = make(map[string]*jogger, fs.NumAvail())
	r.smap = core.T.Sowner().Get()
	p.xctn = r

	r.DemandBase.Init(p.UUID(), p.kind, p.Bck /*from*/, xact.IdleDefault)

	nat := r.smap.CountActiveTs()
	if nat > 1 {
		dmxtra := bundle.Extra{
			RecvAck:  nil, // no ACKs
			Config:   r.config,
			Smap:     r.smap,
			XactConf: r.config.Arch.XactConf,
			SizePDU:  0,
		}
		if err := p.newDM(p.Args.UUID /*trname*/, r.recv, dmxtra, cmn.OwtPut); err != nil {
			return err
		}
	}
	r.sntl.init(r, &r.Base, r.p.dm, r.config, r.smap, nat)
	return nil
}

//////////////
// XactArch //
//////////////

func (r *XactArch) BeginMsg(msg *cmn.ArchiveBckMsg, archlom *core.LOM) (err error) {
	if err := archlom.InitCmnBck(&msg.ToBck); err != nil {
		r.AddErr(err, 4, cos.ModXs)
		return err
	}
	debug.AssertFunc(func() bool { return archlom.Cname() == msg.Cname() }) // relying on it

	wi := &archwi{r: r, msg: msg, archlom: archlom}

	// here and elsewhere: an extra check to make sure this target is active (ref: ignoreMaintenance)
	if err := core.InMaintOrDecomm(r.smap, core.T.Snode(), r); err != nil {
		return err
	}
	wi.tsi, err = r.smap.HrwObjHash2T(archlom.Bck(), archlom.Digest())
	if err != nil {
		r.AddErr(err, 4, cos.ModXs)
		return err
	}

	// fcreate at BEGIN time
	if core.T.SID() == wi.tsi.ID() {
		wi.fqn = archlom.GenFQN(fs.WorkCT, fs.WorkfileCreateArch)
		wi.cksum.Init(archlom.CksumType())
		wi.peersLeft.Store(int32(r.smap.CountActiveTs() - 1))
		wi.rxRefs.Store(1)
		var (
			s         string
			lmfh      cos.LomReader
			tarFormat tar.Format
		)
		switch {
		case !wi.msg.AppendIfExists:
			wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
		case wi.archlom.Load(false, false) == nil:
			s = " append"
			lmfh, tarFormat, err = wi.beginAppend()
		default:
			wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
		}
		if err != nil {
			return err
		}
		if cmn.Rom.V(5, cos.ModXs) {
			nlog.Infof("%s: begin%s %s", r.Base.Name(), s, msg.Cname())
		}

		// construct format-specific writer; serialize for multi-target conc. writing
		opts := archive.Opts{Serialize: true, TarFormat: tarFormat}
		wi.writer = archive.NewWriter(msg.Mime, wi.wfh, &wi.cksum, &opts)

		// append case (above)
		if lmfh != nil {
			err = wi.writer.Copy(lmfh, wi.archlom.Lsize())
			cos.Close(lmfh)
			wi.archlom.Unlock(false)
			if err != nil {
				wi.writer.Fini()
				wi.cleanup()
				return err
			}
		}
	}

	r.pending.mtx.Lock()
	if !r.IsRunning() {
		r.pending.mtx.Unlock()
		if wi.writer != nil {
			wi.writer.Fini()
			wi.cleanup()
		}
		return cmn.NewErrAborted(r.Base.Name(), "stopping", nil)
	}

	// most of the time there'll be a single destination bucket for the lifetime
	if r.bckTo == nil {
		if from := r.Bck().Bucket(); !from.Equal(&wi.msg.ToBck) {
			r.bckTo = meta.CloneBck(&wi.msg.ToBck)
		}
	}

	// register the jogger and work item before stopping can begin
	mi := archlom.Mountpath()
	r.joggers.mtx.Lock()
	wi.j = r.joggers.m[mi.Path]
	if wi.j == nil {
		burst := max(cmn.XactBurstDflt, r.config.Arch.Burst)
		wi.j = &jogger{r: r, mi: mi, workCh: make(chan *archtask, burst)}
		r.joggers.m[mi.Path] = wi.j
		r.joggers.wg.Go(wi.j.run)
	}
	r.joggers.mtx.Unlock()

	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.mtx.Unlock()
	return nil
}

func (r *XactArch) DoMsg(msg *cmn.ArchiveBckMsg) {
	r.pending.mtx.Lock()
	if !r.IsRunning() {
		r.pending.mtx.Unlock()
		return
	}
	wi := r.pending.m[msg.TxnUUID]
	if wi == nil {
		r.pending.mtx.Unlock()
		r.Abort(fmt.Errorf("%s: missing archive transaction %s", r.Base.Name(), msg.TxnUUID))
		return
	}
	if wi.state != archBegun {
		r.pending.mtx.Unlock()
		return
	}
	wi.state = archCommitted
	r.IncPending()
	r.pending.mtx.Unlock()

	var (
		lsflags uint64
		lrit    = &lrit{}
	)
	if msg.NonRecurs {
		lsflags = apc.LsNoRecursion
	}

	// `NwpNone` since we need a single writer to serialize adding files into an eventual `archlom`
	err := lrit.init(r, &msg.ListRange, r.Bck(), lsflags, xact.NwpNone, r.config.Arch.Burst)
	if err != nil {
		r.Abort(err)
		return
	}

	j := wi.j
	l, c := len(j.workCh), cap(j.workCh)
	j.chanFull.Check(l, c)

	select {
	case j.workCh <- &archtask{wi, lrit}:
	case <-r.IdleTimer():
	}
}

func (r *XactArch) CtlMsg() string {
	var sb cos.SB
	n := r.wiCnt.Load()
	sb.Init(64 + 80*int(n))

	sb.WriteString(r.Bck().Cname(""))
	if r.bckTo != nil {
		sb.WriteString("=>")
		sb.WriteString(r.bckTo.Cname(""))
	}
	if n == 0 {
		return sb.String()
	}

	// append work items
	const (
		maxItems = 8
	)
	var s string
	r.pending.mtx.Lock()
	wis := make([]*archwi, 0, min(len(r.pending.m), maxItems))
	for _, wi := range r.pending.m {
		if len(wis) >= maxItems {
			s = " ..."
			break
		}
		wis = append(wis, wi)
	}
	r.pending.mtx.Unlock()

	sb.WriteString(" [")
	first := true
	for _, wi := range wis {
		if !first {
			sb.WriteString("; ")
		}
		wi.append(&sb)
		first = false
	}
	if s != "" {
		sb.WriteString(s)
	}
	sb.WriteUint8(']')

	return sb.String()
}

func (r *XactArch) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())
	if r.sntl.nat > 1 {
		go bcastOnAbort(&r.Base, r.sntl.smap)
	}
	wg.Done()

	select {
	case <-r.IdleTimer():
	case <-r.ChanAbort():
	}
	r.pending.mtx.Lock()
	r.DemandBase.Stop() // prevent new joggers and work items
	r.pending.mtx.Unlock()

	// quiesce and close DM (compare w/ tcb.go, tcobjs.go)
	if r.p.dm != nil && !r.IsAborted() {
		r.sntl.bcastDone("", r.p.dm)
		r.sntl.initLast(mono.NanoTime())
		r.Quiesce(r.sntl.qival(), r.sntl.qcb) // wait for others
	}
	if r.p.dm != nil {
		r.p.dm.Close(r.AbortErr())
		r.p.dm.UnregRecv()
	}
	r.sntl.cleanup()
	r.Finish()

	r.cleanup()
}

func (r *XactArch) cleanup() {
	r.joggers.wg.Wait()

	// started items belong to finalize; claim only work the joggers left behind
	var parked []*archwi
	r.pending.mtx.Lock()
	for id, wi := range r.pending.m {
		if wi.state == archStarted {
			continue
		}
		delete(r.pending.m, id)
		r.wiCnt.Dec()
		parked = append(parked, wi)
	}
	r.pending.mtx.Unlock()

	for _, wi := range parked {
		if wi.writer == nil {
			core.FreeLOM(wi.archlom)
		} else if wi.rxRefs.Dec() == 0 {
			wi.abandon()
		} // otherwise the last receive rolls back
		if wi.state == archCommitted {
			r.DecPending()
		}
	}
}

func (r *XactArch) doSend(lom *core.LOM, wi *archwi, fh cos.ReadOpenCloser) error {
	debug.Assert(r.p.dm != nil)
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = wi.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
		hdr.Opaque = []byte(wi.msg.TxnUUID)
	}
	// o.SentCB nil on purpose (lom is freed by the iterator)
	return r.p.dm.Send(o, fh, wi.tsi)
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactArch) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsOkEOF(err) {
		r.AddErr(err, 5, cos.ModXs)
		return err
	}

	// only data counts as activity: control (e.g., a finishing peer's 'done') must not extend idle
	if hdr.Opcode == 0 {
		r.IncPending()
		err = r._recv(hdr, objReader)
		r.DecPending()
	} else {
		err = r._recv(hdr, objReader)
	}
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *XactArch) _recv(hdr *transport.ObjHdr, objReader io.Reader) error {
	// control messages (compare w/ tcb.go, tcobjs.go)
	if hdr.Opcode != 0 {
		switch hdr.Opcode {
		case transport.OpcDone:
			// Two types of OpcDone - distinguished by whether uuid matches a txnUUID:
			uuid := cos.UnsafeS(hdr.Opaque)
			r.pending.mtx.Lock()
			wi, ok := r.pending.m[uuid]
			r.pending.mtx.Unlock()
			if ok {
				// (1) Per-txn OpcDone: sender finished archiving for this transaction
				// ObjName carries the number of objects sent (see sendDone)
				if hdr.ObjName != "" {
					n, err := strconv.ParseInt(hdr.ObjName, 10, 64)
					if err != nil {
						e := fmt.Errorf("%s: invalid 'done' from %s: %w", r.Name(), meta.Tname(hdr.SID), err)
						r.AddErr(e, 4, cos.ModXs)
					} else {
						wi.nrx.Add(n) // before peersLeft (see quiesce)
					}
				}
				left := wi.peersLeft.Dec()
				debug.Assert(left >= 0)
			} else {
				// (2) Xaction-level OpcDone: sender's whole xaction is done
				r.sntl.rxDone(hdr)
			}
		default:
			return abortOpcode(r, hdr.Opcode)
		}
		return nil
	}

	// data: archive an object
	r.pending.mtx.Lock()
	wi, ok := r.pending.m[cos.UnsafeS(hdr.Opaque)] // txnUUID
	if ok {
		wi.rxRefs.Inc() // under lock (see finalize)
	}
	r.pending.mtx.Unlock()
	if !ok {
		if r.IsDone() || r.IsAborted() {
			return nil
		}
		cnt, err := r.JoinErr()
		if cnt == 0 { // see cleanup
			err = fmt.Errorf("%s: recv: failed to begin(?)", r.Name())
			nlog.Errorln(err)
		}
		return err
	}
	debug.AssertFunc(func() bool { return wi.tsi.ID() == core.T.SID() && wi.msg.TxnUUID == cos.UnsafeS(hdr.Opaque) })

	err := wi.writer.Write(wi.nameInArch(hdr.ObjName), &hdr.ObjAttrs, objReader)
	if err == nil {
		wi.cnt.Inc()
	} else {
		if !archive.IsErrEntryNotWritten(err) {
			wi.werr.Store(true) // e.g. the sender crashed in the middle of the object
		}
		r.AddErr(err, 5, cos.ModXs)
	}
	refs := wi.rxRefs.Dec() // before nrx (see finalize)
	wi.nrx.Dec()
	if refs == 0 {
		wi.abandon() // finalize or shutdown gave up
	}
	return nil
}

// NOTE: in goroutine
func (r *XactArch) finalize(wi *archwi) {
	q := wi.quiesce()

	r.pending.mtx.Lock()
	delete(r.pending.m, wi.msg.TxnUUID)
	r.wiCnt.Dec()
	r.pending.mtx.Unlock()

	// no new receives (wi is not pending)
	if n := wi.rxRefs.Dec(); n > 0 {
		// still receiving (slow or frozen peer)
		err := fmt.Errorf("%s: %s: %d object(s) from peers still being received", r, wi.msg.Cname(), n)
		r.AddErr(err, 4, cos.ModXs)
		r.DecPending()
		return
	}

	if q == core.QuiTimeout || wi.werr.Load() {
		var err error
		switch n := wi.nrx.Load(); {
		case q == core.QuiTimeout && n < 0:
			err = fmt.Errorf("%s: %s: received %d object(s) more than peers sent", r, wi.msg.Cname(), -n)
		case q == core.QuiTimeout:
			err = fmt.Errorf("%s: %s: %v (%d object(s) sent by peers not received)",
				r, wi.msg.Cname(), cmn.ErrQuiesceTimeout, n)
		default:
			err = fmt.Errorf("%s: %s: failed to archive object(s)", r, wi.msg.Cname())
		}
		r.AddErr(err, 4, cos.ModXs)
		wi.abandon() // do not publish
		r.DecPending()
		return
	}

	ecode, err := r._fini(wi)
	r.DecPending()
	if cmn.Rom.V(5, cos.ModXs) {
		var s string
		if err != nil {
			s = fmt.Sprintf(": %v(%d)", err, ecode)
		}
		nlog.Infof("%s: finalize %s%s", r.Base.Name(), wi.msg.Cname(), s)
	}
	if err == nil || r.IsAborted() { // done ok (unless aborted)
		return
	}
	r.AddErr(err, 5, cos.ModXs)
}

func (r *XactArch) _fini(wi *archwi) (ecode int, err error) {
	defer core.FreeLOM(wi.archlom)

	err = wi.writer.Fini()

	if !r.IsRunning() || err != nil {
		wi.cleanup()
		return 0, err
	}

	var size int64
	if wi.cnt.Load() == 0 {
		s := "empty"
		if wi.appendPos > 0 {
			s = "no new appends to"
		}
		if cnt, errs := r.JoinErr(); cnt > 0 {
			err = fmt.Errorf("%s: %s %s, err: %v (cnt=%d)", r, s, wi.archlom, errs, cnt)
		} else {
			err = fmt.Errorf("%s: %s %s", r, s, wi.archlom)
		}
	} else {
		size, err = wi.finalize()
	}
	if err != nil {
		wi.cleanup()
		return http.StatusInternalServerError, err
	}
	debug.Assert(wi.wfh == nil)

	origSize := wi.archlom.Lsize(true /*may be new*/)
	wi.archlom.SetSize(size)
	ecode, err = core.T.FinalizeObj(wi.archlom, wi.fqn, r, cmn.OwtArchive, false /*rm work*/)
	if err != nil {
		wi.archlom.SetSize(origSize)
		wi.cleanup()
		return ecode, err
	}
	r.ObjsAdd(1, size-wi.appendPos)
	return ecode, nil
}

func (r *XactArch) Name() (s string) {
	s = r.streamingX.Name()
	if src, dst := r.FromTo(); src != nil {
		s += " => " + dst.String()
	}
	return
}

func (r *XactArch) String() (s string) {
	s = r.streamingX.String() + " => "
	if r.wiCnt.Load() > 0 && r.bckTo != nil {
		s += r.bckTo.String()
	}
	return
}

func (r *XactArch) FromTo() (src, dst *meta.Bck) {
	if r.bckTo != nil {
		src, dst = r.Bck(), r.bckTo
	}
	return
}

func (r *XactArch) chanFullTotal() (n int64) {
	r.joggers.mtx.Lock()
	for _, j := range r.joggers.m {
		n += j.chanFull.Load()
	}
	r.joggers.mtx.Unlock()
	return n
}

func (r *XactArch) Snap() (snap *core.Snap) {
	snap = r.Base.NewSnap(r)

	snap.Pack(len(r.joggers.m), 0 /*currently, always zero*/, r.chanFullTotal())

	if f, t := r.FromTo(); f != nil {
		snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	}
	return
}

////////////
// jogger //
////////////

func (j *jogger) run() {
	nlog.Infoln("start jogger", j.r.Name(), j.mi)
outer:
	for {
		select {
		case archtask, ok := <-j.workCh:
			if !ok || !j.r.IsRunning() {
				break outer
			}
			j.do(archtask)
		case <-j.r.ChanAbort():
			break outer
		case <-j.r.IdleTimer():
			break outer
		}
	}
	nlog.Infoln("stop jogger", j.r.Name(), j.mi)
}

func (j *jogger) do(archtask *archtask) {
	var (
		r        = j.r
		lrit, wi = archtask.lrit, archtask.wi
	)
	debug.Assert(r == wi.r)
	r.pending.mtx.Lock()
	debug.Assert(wi.state == archCommitted)
	wi.state = archStarted
	r.pending.mtx.Unlock()

	wi.lrp = lrit.lrp
	if err := lrit.run(wi, j.r.smap, false /*prealloc buf*/); err != nil {
		wi.r.AddErr(err)
	}

	lrit.wait()

	if core.T.SID() == wi.tsi.ID() {
		go wi.r.finalize(wi)
	} else {
		wi.r.sendDone(wi.msg.TxnUUID, wi.tsi, int64(wi.cnt.Load()))
		wi.r.pending.mtx.Lock()
		delete(wi.r.pending.m, wi.msg.TxnUUID)
		wi.r.wiCnt.Dec()
		wi.r.pending.mtx.Unlock()
		wi.r.DecPending()

		core.FreeLOM(wi.archlom)
	}
}

////////////
// archwi //
////////////

// in-place TAR append returns its format; copy-append returns a reader with archlom locked
func (wi *archwi) beginAppend() (lmfh cos.LomReader, format tar.Format, err error) {
	msg := wi.msg
	if msg.Mime == archive.ExtTar && !wi.archlom.IsChunked() {
		// (special)
		format, err = wi.openTarForAppend()
		if err != archive.ErrTarIsEmpty {
			return nil, format, err
		}
	}

	// <extra copy>
	// prep to copy `lmfh` --> `wi.fh` with subsequent APPEND-ing
	// msg.Mime has been already validated (see ais/* for apc.ActArchive)
	wi.archlom.Lock(false)
	lmfh, err = wi.archlom.Open()
	if err != nil {
		wi.archlom.Unlock(false)
		return nil, format, err
	}
	if wi.wfh, err = wi.archlom.CreateWork(wi.fqn); err != nil {
		wi.archlom.Unlock(false)
		cos.Close(lmfh)
		lmfh = nil
	}
	return lmfh, format, err
}

// [NOTE]
// - archive.OpenTarForAppend calls os.OpenFile
// - it therefore requires archlom to be a monolithic file - no chunks
func (wi *archwi) openTarForAppend() (format tar.Format, err error) {
	if err = wi.archlom.RenameMainTo(wi.fqn); err != nil {
		return format, err
	}
	// open (rw) lom itself
	wi.wfh, format, wi.appendPos, err = archive.OpenTarForAppend(wi.archlom.Cname(), wi.fqn)
	if err == nil {
		return format, nil // can append
	}

	// back
	if errV := wi.archlom.RenameToMain(wi.fqn); errV != nil {
		nlog.Errorf("%s: nested error: failed to append %s (%v) and rename back from %s (%v)",
			wi.tsi, wi.archlom, err, wi.fqn, errV)
	}
	return format, err
}

// multi-object iterator i/f: "handle work item"
func (wi *archwi) do(lom *core.LOM, lrit *lrit, _ []byte) {
	var coldGet bool
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if !cos.IsNotExist(err) {
			wi.r.AddErr(err, 5, cos.ModXs)
			return
		}
		if coldGet = lom.Bck().IsRemote(); !coldGet {
			if lrit.lrp == lrpList {
				// listed, not found
				wi.r.AddErr(err, 5, cos.ModXs)
			}
			return
		}
	}

	if coldGet {
		// cold
		if ecode, err := core.T.GetCold(context.Background(), lom, wi.r.Kind(), cmn.OwtGetLock); err != nil {
			if lrit.lrp != lrpList && cos.IsNotExist(err, ecode) {
				return // range or prefix, not found
			}
			wi.r.AddErr(err, 5, cos.ModXs)
			return
		}
	}

	lom.Lock(false)
	lh, err := lom.NewHandle(false /*loaded*/)
	if err != nil {
		lom.Unlock(false)
		wi.r.AddErr(err, 5, cos.ModXs)
		return
	}
	if core.T.SID() != wi.tsi.ID() {
		if err := wi.r.doSend(lom, wi, lh); err != nil {
			wi.r.AddErr(err, 5, cos.ModXs)
		} else {
			wi.cnt.Inc() // see sendDone
		}
		lom.Unlock(false)
		return
	}
	// see Begin
	if wi.wfh == nil {
		lom.Unlock(false)
		// NOTE: unexpected and unlikely - aborting
		err := cos.NewErrNotFoundFmt(nil, "%s: archive destination is not ready for %q", wi.r.Name(), lom.Cname())
		wi.r.Abort(err)
		return
	}
	err = wi.writer.Write(wi.nameInArch(lom.ObjName), lom, lh /*reader*/)
	cos.Close(lh)
	lom.Unlock(false)
	if err == nil {
		wi.cnt.Inc()
	} else {
		if !archive.IsErrEntryNotWritten(err) {
			wi.werr.Store(true) // e.g., a read error in the middle of the object
		}
		wi.r.AddErr(err, 5, cos.ModXs)
	}
}

// wait for peers' 'done' and all objects they sent ('done' may arrive first on another stream)
// once all peers are done, time out after send_file_time w/ no progress on this shard
func (wi *archwi) quiesce() core.QuiRes {
	var (
		lastNrx, lastSize int64
		lastProgress      time.Duration
	)
	return wi.r.Quiesce(cmn.Rom.CplaneOperation(), func(total time.Duration) core.QuiRes {
		if wi.r.IsDone() {
			return core.QuiAborted
		}
		left, n := wi.peersLeft.Load(), wi.nrx.Load() // order: see _recv
		if left == 0 {
			switch {
			case n == 0:
				return core.QuiDone
			case n < 0:
				// got more objects than peers reported - won't converge
				return core.QuiTimeout
			}
		}
		if smap := core.T.Sowner().Get(); !wi.r.smap.CompareTargets(smap) {
			return wi.qabort(smap)
		}
		if left > 0 {
			return core.QuiActive
		}

		// track completed receives and writes still in progress
		size := lastSize
		if finfo, err := os.Stat(wi.fqn); err == nil {
			size = finfo.Size()
		}
		if n != lastNrx || size != lastSize {
			lastNrx, lastSize, lastProgress = n, size, total
		} else if total-lastProgress > wi.r.config.Timeout.SendFile.D() {
			return core.QuiTimeout
		}
		return core.QuiActive
	})
}

func (wi *archwi) qabort(smap *meta.Smap) core.QuiRes {
	r := wi.r
	if err := r.sntl.checkSmap(smap); err != nil {
		r.SetClusterAbort() // each target detects it
		return r.sntl._qabort(err)
	}
	// the same targets but a new incarnation or new endpoints (broadcast the abort)
	err := fmt.Errorf("%s: targets changed (incarnation or endpoints) [%s => %s]", r.Name(), r.smap.StringEx(), smap.StringEx())
	return r.sntl._qabort(err)
}

func (wi *archwi) nameInArch(objName string) string {
	if wi.msg.BaseNameOnly {
		objName = filepath.Base(objName)
	}
	if !wi.msg.InclSrcBname {
		return objName
	}
	buf := make([]byte, 0, len(wi.msg.FromBckName)+1+len(objName))
	buf = append(buf, wi.msg.FromBckName...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return cos.UnsafeS(buf)
}

// do not publish (see cleanup)
func (wi *archwi) abandon() {
	wi.writer.Fini()
	wi.cleanup()
	core.FreeLOM(wi.archlom)
}

func (wi *archwi) cleanup() {
	if wi.appendPos > 0 && wi.fqn != "" {
		wi.rollback()
		return
	}
	if wi.wfh != nil {
		cos.Close(wi.wfh)
		wi.wfh = nil
	}
	if wi.fqn != "" {
		if wi.archlom == nil || wi.archlom.FQN != wi.fqn {
			cos.RemoveFile(wi.fqn)
		}
		wi.fqn = ""
	}
}

func (wi *archwi) rollback() {
	if wi.wfh != nil {
		if err := wi.wfh.Close(); err != nil {
			nlog.Warningln(wi.r.Name(), "rollback:", err)
		}
		wi.wfh = nil
	}
	wi.archlom.Lock(true) // (a PUT publishes under the same lock)
	err := wi._rollback()
	wi.archlom.Unlock(true)
	if err != nil {
		nlog.Errorln(wi.r.Name(), "failed to roll back append to", wi.archlom.Cname(), "[", wi.fqn, err, "]")
	}
	wi.fqn = ""
}

func (wi *archwi) _rollback() error {
	if _, err := os.Lstat(wi.archlom.FQN); err == nil {
		if _, err := os.Lstat(wi.fqn); cos.IsNotExist(err) {
			return nil // rename already consumed the work file
		}
		// a newer object took the name while the append ran (e.g. a PUT) - keep it as before
		nlog.Warningln(wi.r.Name(), "rollback:", wi.archlom.Cname(), "has a newer object, not restoring the original")
		return cos.RemoveFile(wi.fqn)
	}
	if err := os.Truncate(wi.fqn, wi.appendPos); err != nil {
		return err
	}
	if err := os.Truncate(wi.fqn, wi.archlom.Lsize()); err != nil { // (zero-filled)
		return err
	}
	return wi.archlom.RenameToMain(wi.fqn)
}

func (wi *archwi) finalize() (int64, error) {
	err := wi.wfh.Close()
	wi.wfh = nil
	if err != nil {
		debug.AssertNoErr(err)
		return 0, err
	}
	// tar append
	if wi.appendPos > 0 {
		finfo, err := os.Lstat(wi.fqn)
		if err != nil {
			debug.AssertNoErr(err)
			return 0, err
		}
		wi.archlom.SetCksum(cos.NewCksum(cos.ChecksumNone, "")) // (see TODO)
		return finfo.Size(), nil
	}
	// default
	wi.cksum.Finalize()
	wi.archlom.SetCksum(&wi.cksum.Cksum)
	return wi.cksum.Size, nil
}

func (wi *archwi) append(sb *cos.SB) {
	msg := wi.msg
	msg.ListRange.Str(sb, wi.lrp == lrpPrefix)

	if wi.tsi.ID() != core.T.SID() {
		sb.WriteString(", target:")
		sb.WriteString(wi.tsi.ID())
	}
	if cnt := wi.cnt.Load(); cnt > 0 {
		if wi.tsi.ID() != core.T.SID() {
			sb.WriteString(", sent:")
		} else {
			sb.WriteString(", archived:")
		}
		sb.WriteInt(int(cnt))
	}
	if wi.tsi.ID() == core.T.SID() {
		if left := wi.peersLeft.Load(); left > 0 {
			sb.WriteString(", peers-not-done:")
			sb.WriteInt(int(left))
		} else if n := wi.nrx.Load(); n > 0 {
			sb.WriteString(", from-peers:")
			sb.WriteInt(int(n))
		}
	}

	sb.WriteString(", flags:")
	first := true
	if msg.BaseNameOnly {
		first = false
		sb.WriteString("basename-only")
	}
	if msg.InclSrcBname {
		if !first {
			sb.WriteUint8(',')
		}
		first = false
		sb.WriteString("incl-src-bname")
	}
	if msg.AppendIfExists {
		if !first {
			sb.WriteUint8(',')
		}
		sb.WriteString("append-iff")
	}
}
