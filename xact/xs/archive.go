// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
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
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
// - multi-worker list-range iter (see lrit.init)
// - one source multiple destination buckets (feature)

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
		tarFormat tar.Format
		cnt       atomic.Int32 // num archived
		refc      atomic.Int32 // finishing
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
		streamingX
	}
)

// interface guard
var (
	_ core.Xact      = (*XactArch)(nil)
	_ lrxact         = (*XactArch)(nil)
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
		burst  = max(minArchWorkChSize, config.Arch.Burst)
		r      = &XactArch{streamingX: streamingX{p: &p.streamingF, config: config}}
	)
	r.pending.m = make(map[string]*archwi, burst)

	r.joggers.m = make(map[string]*jogger, fs.NumAvail())
	r.smap = core.T.Sowner().Get()
	p.xctn = r

	// delay ctlmsg until DoMsg()
	r.DemandBase.Init(p.UUID(), p.kind, "" /*ctlmsg*/, p.Bck /*from*/, xact.IdleDefault)

	dmxtra := bundle.Extra{
		RecvAck:     nil, // no ACKs
		Config:      r.config,
		Compression: r.config.Arch.Compression,
		Multiplier:  r.config.Arch.SbundleMult,
		SizePDU:     0,
	}
	if err := p.newDM(p.Args.UUID /*trname*/, r.recv, r.smap, dmxtra, cmn.OwtPut); err != nil {
		return err
	}

	xact.GoRunW(r)
	return nil
}

//////////////
// XactArch //
//////////////

func (r *XactArch) BeginMsg(msg *cmn.ArchiveBckMsg, archlom *core.LOM) (err error) {
	if err := archlom.InitBck(&msg.ToBck); err != nil {
		r.AddErr(err, 4, cos.ModXs)
		return err
	}
	debug.Assert(archlom.Cname() == msg.Cname()) // relying on it

	wi := &archwi{r: r, msg: msg, archlom: archlom, tarFormat: tar.FormatUnknown}
	wi.fqn = wi.archlom.GenFQN(fs.WorkCT, fs.WorkfileCreateArch)
	wi.cksum.Init(archlom.CksumType())

	// here and elsewhere: an extra check to make sure this target is active (ref: ignoreMaintenance)
	if err := core.InMaintOrDecomm(r.smap, core.T.Snode(), r); err != nil {
		return err
	}
	nat := r.smap.CountActiveTs()
	wi.refc.Store(int32(nat - 1))

	wi.tsi, err = r.smap.HrwName2T(msg.ToBck.MakeUname(msg.ArchName))
	if err != nil {
		r.AddErr(err, 4, cos.ModXs)
		return err
	}

	// set archwi jogger based on the archlom's mountpath
	var (
		burst = max(minArchWorkChSize, r.config.Arch.Burst)
		mi    = archlom.Mountpath()
	)
	r.joggers.mtx.Lock()
	if j, ok := r.joggers.m[mi.Path]; ok {
		wi.j = j
	} else {
		// [on demand] create and run just once for this job
		j = &jogger{
			r:      r,
			mi:     mi,
			workCh: make(chan *archtask, burst),
		}
		r.joggers.m[mi.Path] = j
		wi.j = j

		// and run this one right away
		r.joggers.wg.Add(1)
		go func(j *jogger, wg *sync.WaitGroup) {
			j.run()
			wg.Done()
		}(j, &r.joggers.wg)
	}
	r.joggers.mtx.Unlock()

	// fcreate at BEGIN time
	if core.T.SID() == wi.tsi.ID() {
		var (
			s    string
			lmfh cos.LomReader
		)
		switch {
		case !wi.msg.AppendIfExists:
			wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
		case wi.archlom.Load(false, false) == nil:
			s = " append"
			lmfh, err = wi.beginAppend()
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
		opts := archive.Opts{Serialize: true, TarFormat: wi.tarFormat}
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

	// most of the time there'll be a single destination bucket for the lifetime
	if r.bckTo == nil {
		if from := r.Bck().Bucket(); !from.Equal(&wi.msg.ToBck) {
			r.bckTo = meta.CloneBck(&wi.msg.ToBck)
		}
	}

	r.pending.mtx.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.mtx.Unlock()
	return nil
}

func (r *XactArch) DoMsg(msg *cmn.ArchiveBckMsg) {
	r.IncPending()
	r.pending.mtx.Lock()
	wi, ok := r.pending.m[msg.TxnUUID]
	r.pending.mtx.Unlock()
	if !ok || wi == nil {
		// NOTE: unexpected and unlikely - aborting
		debug.Assert(r.ErrCnt() > 0) // see cleanup
		r.Abort(r.Err())
		r.DecPending()
		r.cleanup()
		return
	}

	var (
		lsflags uint64
		lrit    = &lrit{}
	)
	if msg.NonRecurs {
		lsflags = apc.LsNoRecursion
	}

	// `nwpNone` since we need a single writer to serialize adding files into an eventual `archlom`
	err := lrit.init(r, &msg.ListRange, r.Bck(), lsflags, nwpNone, r.config.Arch.Burst)
	if err != nil {
		r.Abort(err)
		r.DecPending()
		r.cleanup()
		return
	}

	// dynamic ctlmsg // TODO: ref
	{
		var sb strings.Builder
		sb.Grow(80)
		sb.WriteString(r.Bck().Cname(""))
		if r.bckTo != nil && !r.bckTo.IsEmpty() {
			sb.WriteString("=>")
			sb.WriteString(r.bckTo.Cname(""))
		}
		sb.WriteByte(' ')
		msg.ListRange.Str(&sb, lrit.lrp == lrpPrefix)
		if msg.BaseNameOnly {
			sb.WriteString(", basename-only")
		}
		if msg.InclSrcBname {
			sb.WriteString(", incl-src-basename")
		}
		if msg.AppendIfExists {
			sb.WriteString(", append-iff")
		}

		r.Base.SetCtlMsg(sb.String())
	}

	if r.IsAborted() {
		return
	}

	j := wi.j
	l, c := len(j.workCh), cap(j.workCh)
	j.chanFull.Check(l, c)

	j.workCh <- &archtask{wi, lrit}
	if r.ErrCnt() > 0 {
		wi.cleanup()
		r.Abort(r.Err())
		r.cleanup()
	}
}

func (r *XactArch) Run(wg *sync.WaitGroup) {
	nlog.Infoln(r.Name())
	wg.Done()

	select {
	case <-r.IdleTimer():
		r.cleanup()
	case <-r.ChanAbort():
		r.cleanup()
	}
}

func (r *XactArch) cleanup() {
	r.streamingX.fin(true /*unreg Rx*/)
	if r.ErrCnt() == 0 {
		return
	}

	// [cleanup] close and rm unfinished archives (compare w/ finalize)
	r.pending.mtx.Lock()
	for _, wi := range r.pending.m {
		wi.cleanup()
	}
	clear(r.pending.m)
	r.pending.mtx.Unlock()

	r.joggers.wg.Wait()
}

func (r *XactArch) doSend(lom *core.LOM, wi *archwi, fh cos.ReadOpenCloser) {
	debug.Assert(r.p.dm != nil)
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = wi.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
		hdr.Opaque = []byte(wi.msg.TxnUUID)
	}
	// o.Callback nil on purpose (lom is freed by the iterator)
	r.p.dm.Send(o, fh, wi.tsi)
}

// (note: ObjHdr and its fields must be consumed synchronously)
func (r *XactArch) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		r.AddErr(err, 5, cos.ModXs)
		return err
	}

	r.IncPending()
	err = r._recv(hdr, objReader)
	r.DecPending()
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *XactArch) _recv(hdr *transport.ObjHdr, objReader io.Reader) error {
	r.pending.mtx.Lock()
	wi, ok := r.pending.m[cos.UnsafeS(hdr.Opaque)] // txnUUID
	r.pending.mtx.Unlock()
	if !ok {
		if r.Finished() || r.IsAborted() {
			return nil
		}
		cnt, err := r.JoinErr()
		if cnt == 0 { // see cleanup
			err = fmt.Errorf("%s: recv: failed to begin(?)", r.Name())
			nlog.Errorln(err)
		}
		return err
	}
	debug.Assert(wi.tsi.ID() == core.T.SID() && wi.msg.TxnUUID == cos.UnsafeS(hdr.Opaque))

	// NOTE: best-effort via ref-counting
	if hdr.Opcode == opDone {
		refc := wi.refc.Dec()
		debug.Assert(refc >= 0)
		return nil
	}

	debug.Assert(hdr.Opcode == 0)
	err := wi.writer.Write(wi.nameInArch(hdr.ObjName), &hdr.ObjAttrs, objReader)
	if err == nil {
		wi.cnt.Inc()
	} else {
		r.AddErr(err, 5, cos.ModXs)
	}
	return nil
}

// NOTE: in goroutine
func (r *XactArch) finalize(wi *archwi) {
	q := wi.quiesce()
	if q == core.QuiTimeout {
		err := fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout)
		r.AddErr(err, 4, cos.ModXs)
	}

	r.pending.mtx.Lock()
	delete(r.pending.m, wi.msg.TxnUUID)
	r.wiCnt.Dec()
	r.pending.mtx.Unlock()

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
	debug.Assert(q != core.QuiAborted)

	wi.cleanup()
	r.AddErr(err, 5, cos.ModXs)
}

func (r *XactArch) _fini(wi *archwi) (ecode int, err error) {
	err = wi.writer.Fini()

	if r.IsAborted() || err != nil {
		wi.cleanup()
		core.FreeLOM(wi.archlom)
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
		core.FreeLOM(wi.archlom)
		return http.StatusInternalServerError, err
	}
	debug.Assert(wi.wfh == nil)

	wi.archlom.SetSize(size)
	ecode, err = core.T.FinalizeObj(wi.archlom, wi.fqn, r, cmn.OwtArchive)
	core.FreeLOM(wi.archlom)
	r.ObjsAdd(1, size-wi.appendPos)

	return ecode, err
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
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.Pack(len(r.joggers.m), 0 /*currently, always zero*/, r.chanFullTotal())

	snap.IdleX = r.IsIdle()
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
			if !ok {
				break outer
			}
			j.do(archtask)
		case <-j.r.ChanAbort():
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
	if err := lrit.run(wi, j.r.smap, false /*prealloc buf*/); err != nil {
		wi.r.AddErr(err)
	}

	lrit.wait()

	if core.T.SID() == wi.tsi.ID() {
		go wi.r.finalize(wi) // TODO -- FIXME: async finalize this shard vs stopping
	} else {
		wi.r.sendTerm(wi.msg.TxnUUID, wi.tsi, nil)
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

// returns one of:
// 1. (TAR opened for append, lmfh nil, and archlom not locked)
// 2. (lmfh and archlom locked)
// 3. error
func (wi *archwi) beginAppend() (lmfh cos.LomReader, err error) {
	msg := wi.msg
	if msg.Mime == archive.ExtTar && !wi.archlom.IsChunked() {
		// (special)
		err = wi.openTarForAppend()
		if err == nil /*can append*/ || err != archive.ErrTarIsEmpty /*fail XactArch.Begin*/ {
			return nil, err
		}
	}

	// <extra copy>
	// prep to copy `lmfh` --> `wi.fh` with subsequent APPEND-ing
	// msg.Mime has been already validated (see ais/* for apc.ActArchive)
	wi.archlom.Lock(false)
	lmfh, err = wi.archlom.Open()
	if err != nil {
		wi.archlom.Unlock(false)
		return nil, err
	}
	if wi.wfh, err = wi.archlom.CreateWork(wi.fqn); err != nil {
		wi.archlom.Unlock(false)
		cos.Close(lmfh)
		lmfh = nil
	}
	return lmfh, err
}

// [NOTE]
// - archive.OpenTarForAppend calls os.OpenFile
// - it therefore requires archlom to be a monolithic file - no chunks
func (wi *archwi) openTarForAppend() (err error) {
	if err = wi.archlom.RenameMainTo(wi.fqn); err != nil {
		return err
	}
	// open (rw) lom itself
	wi.wfh, wi.tarFormat, wi.appendPos, err = archive.OpenTarForAppend(wi.archlom.Cname(), wi.fqn)
	if err == nil {
		return nil // can append
	}

	// back
	if errV := wi.archlom.RenameToMain(wi.fqn); errV != nil {
		nlog.Errorf("%s: nested error: failed to append %s (%v) and rename back from %s (%v)",
			wi.tsi, wi.archlom, err, wi.fqn, errV)
	} else {
		wi.fqn = ""
	}
	return err
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
		wi.r.doSend(lom, wi, lh)
		lom.Unlock(false)
		return
	}
	// see Begin
	if wi.wfh == nil {
		lom.Unlock(false)
		// NOTE: unexpected and unlikely - aborting
		err = fmt.Errorf("%s: destination %q does not exist (not open)", wi.r.Name(), wi.fqn)
		wi.r.Abort(err)
		return
	}
	err = wi.writer.Write(wi.nameInArch(lom.ObjName), lom, lh /*reader*/)
	cos.Close(lh)
	lom.Unlock(false)
	if err == nil {
		wi.cnt.Inc()
	} else {
		wi.r.AddErr(err, 5, cos.ModXs)
	}
}

func (wi *archwi) quiesce() core.QuiRes {
	timeout := cmn.Rom.CplaneOperation()
	return wi.r.Quiesce(timeout, func(total time.Duration) core.QuiRes {
		if wi.refc.Load() == 0 && wi.r.wiCnt.Load() == 1 /*the last wi (so far) about to `fini`*/ {
			return core.QuiDone
		}
		return xact.RefcntQuiCB(&wi.refc, wi.r.config.Timeout.SendFile.D()/2, total)
	})
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

func (wi *archwi) cleanup() {
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
		wi.archlom.SetCksum(cos.NewCksum(cos.ChecksumNone, "")) // TODO: checksum NIY
		return finfo.Size(), nil
	}
	// default
	wi.cksum.Finalize()
	wi.archlom.SetCksum(&wi.cksum.Cksum)
	return wi.cksum.Size, nil
}
