// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// - enable multi-threaded list-range iter (see lrit.init)
// - one source multiple destination buckets (feature)

type (
	archFactory struct {
		streamingF
	}
	archwi struct { // archival work item; implements lrwi
		j       *jogger
		writer  archive.Writer
		r       *XactArch
		msg     *cmn.ArchiveBckMsg
		tsi     *meta.Snode
		archlom *core.LOM
		fqn     string        // workFQN --/--
		wfh     cos.LomWriter // -> workFQN
		cksum   cos.CksumHashSize
		cnt     atomic.Int32 // num archived
		// tar only
		appendPos int64 // append to existing
		tarFormat tar.Format
		// finishing
		refc atomic.Int32
	}
	archtask struct {
		wi   *archwi
		lrit *lrit
	}
	jogger struct {
		mpath  *fs.Mountpath
		workCh chan *archtask
		stopCh cos.StopCh
	}
	XactArch struct {
		streamingX
		bckTo   *meta.Bck
		joggers struct {
			wg sync.WaitGroup
			m  map[string]*jogger
			sync.RWMutex
		}
		pending struct {
			m map[string]*archwi
			sync.RWMutex
		}
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
	//
	// new x-archive
	//
	r := &XactArch{streamingX: streamingX{p: &p.streamingF, config: cmn.GCO.Get()}}
	r.pending.m = make(map[string]*archwi, maxNumInParallel)
	avail := fs.GetAvail()
	r.joggers.m = make(map[string]*jogger, len(avail))
	p.xctn = r
	r.DemandBase.Init(p.UUID(), p.kind, "" /*ctlmsg later via Do()*/, p.Bck /*from*/, xact.IdleDefault)

	if err := p.newDM(p.Args.UUID /*trname*/, r.recv, r.config, cmn.OwtPut, 0 /*pdu*/); err != nil {
		return err
	}
	if r.p.dm != nil {
		r.p.dm.SetXact(r)
		r.p.dm.Open()
	}
	xact.GoRunW(r)
	return
}

//////////////
// XactArch //
//////////////

func (r *XactArch) Begin(msg *cmn.ArchiveBckMsg, archlom *core.LOM) (err error) {
	if err = archlom.InitBck(&msg.ToBck); err != nil {
		r.AddErr(err, 4, cos.SmoduleXs)
		return err
	}
	debug.Assert(archlom.Cname() == msg.Cname()) // relying on it

	wi := &archwi{r: r, msg: msg, archlom: archlom, tarFormat: tar.FormatUnknown}
	wi.fqn = fs.CSM.Gen(wi.archlom, fs.WorkfileType, fs.WorkfileCreateArch)
	wi.cksum.Init(archlom.CksumType())

	// here and elsewhere: an extra check to make sure this target is active (ref: ignoreMaintenance)
	smap := core.T.Sowner().Get()
	if err = core.InMaintOrDecomm(smap, core.T.Snode(), r); err != nil {
		return err
	}
	nat := smap.CountActiveTs()
	wi.refc.Store(int32(nat - 1))

	wi.tsi, err = smap.HrwName2T(msg.ToBck.MakeUname(msg.ArchName))
	if err != nil {
		r.AddErr(err, 4, cos.SmoduleXs)
		return err
	}

	// bind a new/existing jogger to this archwi based on archlom's mountpath
	var (
		mpath  = archlom.Mountpath()
		exists bool
	)
	r.joggers.Lock()
	if wi.j, exists = r.joggers.m[mpath.Path]; !exists {
		r.joggers.m[mpath.Path] = &jogger{
			mpath:  mpath,
			workCh: make(chan *archtask, maxNumInParallel*2),
		}
		wi.j = r.joggers.m[mpath.Path]
		wi.j.stopCh.Init()
		r.joggers.wg.Add(1)
		go func() {
			wi.j.run()
			r.joggers.wg.Done()
		}()
	}
	r.joggers.Unlock()

	// fcreate at BEGIN time
	if core.T.SID() == wi.tsi.ID() {
		var (
			s    string
			lmfh cos.LomReader
		)
		if !wi.msg.AppendIfExists {
			wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
		} else if errX := wi.archlom.Load(false, false); errX == nil {
			if !wi.archlom.IsChunked() {
				s = " append"
				lmfh, err = wi.beginAppend()
			} else {
				wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
			}
		} else {
			wi.wfh, err = wi.archlom.CreateWork(wi.fqn)
		}
		if err != nil {
			return err
		}
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infof("%s: begin%s %s", r.Base.Name(), s, msg.Cname())
		}

		// construct format-specific writer; serialize for multi-target conc. writing
		opts := archive.Opts{Serialize: true, TarFormat: wi.tarFormat}
		wi.writer = archive.NewWriter(msg.Mime, wi.wfh, &wi.cksum, &opts)

		// append case (above)
		if lmfh != nil {
			err = wi.writer.Copy(lmfh, wi.archlom.Lsize())
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

	r.pending.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.Unlock()
	return nil
}

func (r *XactArch) Do(msg *cmn.ArchiveBckMsg) {
	r.IncPending()
	r.pending.RLock()
	wi, ok := r.pending.m[msg.TxnUUID]
	r.pending.RUnlock()
	if !ok || wi == nil {
		// NOTE: unexpected and unlikely - aborting
		debug.Assert(r.ErrCnt() > 0) // see cleanup
		r.Abort(r.Err())
		r.DecPending()
		r.cleanup()
		return
	}

	// lrpWorkersNone since we need a single writer to serialize adding files
	// into an eventual `archlom`
	lrit := &lrit{}
	err := lrit.init(r, &msg.ListRange, r.Bck(), lrpWorkersNone)
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

	wi.j.workCh <- &archtask{wi, lrit}
	if r.Err() != nil {
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
	if r.Err() == nil {
		return
	}

	// [cleanup] close and rm unfinished archives (compare w/ finalize)
	r.pending.Lock()
	for _, wi := range r.pending.m {
		wi.cleanup()
	}
	clear(r.pending.m)
	r.pending.Unlock()

	r.joggers.Lock()
	for _, jogger := range r.joggers.m {
		jogger.stopCh.Close()
	}
	clear(r.joggers.m)
	r.joggers.Unlock()
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

func (r *XactArch) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		r.AddErr(err, 5, cos.SmoduleXs)
		return err
	}

	r.IncPending()
	err = r._recv(hdr, objReader)
	r.DecPending()
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *XactArch) _recv(hdr *transport.ObjHdr, objReader io.Reader) error {
	r.pending.RLock()
	wi, ok := r.pending.m[cos.UnsafeS(hdr.Opaque)] // txnUUID
	r.pending.RUnlock()
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
	if hdr.Opcode == opcodeDone {
		refc := wi.refc.Dec()
		debug.Assert(refc >= 0)
		return nil
	}

	debug.Assert(hdr.Opcode == 0)
	err := wi.writer.Write(wi.nameInArch(hdr.ObjName), &hdr.ObjAttrs, objReader)
	if err == nil {
		wi.cnt.Inc()
	} else {
		r.AddErr(err, 5, cos.SmoduleXs)
	}
	return nil
}

// NOTE: in goroutine
func (r *XactArch) finalize(wi *archwi) {
	q := wi.quiesce()
	if q == core.QuiTimeout {
		err := fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout)
		r.AddErr(err, 4, cos.SmoduleXs)
	}

	r.pending.Lock()
	delete(r.pending.m, wi.msg.TxnUUID)
	r.wiCnt.Dec()
	r.pending.Unlock()

	ecode, err := r._fini(wi)
	r.DecPending()
	if cmn.Rom.FastV(5, cos.SmoduleXs) {
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
	r.AddErr(err, 5, cos.SmoduleXs)
}

func (r *XactArch) _fini(wi *archwi) (ecode int, err error) {
	wi.writer.Fini()

	if r.IsAborted() {
		wi.cleanup()
		core.FreeLOM(wi.archlom)
		return
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
		ecode = http.StatusInternalServerError
		return
	}
	debug.Assert(wi.wfh == nil)

	wi.archlom.SetSize(size)
	ecode, err = core.T.FinalizeObj(wi.archlom, wi.fqn, r, cmn.OwtArchive)
	core.FreeLOM(wi.archlom)
	r.ObjsAdd(1, size-wi.appendPos)

	return
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

func (r *XactArch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

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
	nlog.Infoln("jogger started in mount path", j.mpath)
	for {
		select {
		case archtask := <-j.workCh:
			lrit, wi := archtask.lrit, archtask.wi
			smap := core.T.Sowner().Get()
			err := lrit.run(wi, smap)
			if err != nil {
				wi.r.AddErr(err)
			}

			lrit.wait()
			if core.T.SID() == wi.tsi.ID() {
				go wi.r.finalize(wi) // async finalize this shard
			} else {
				wi.r.sendTerm(wi.msg.TxnUUID, wi.tsi, nil)
				wi.r.pending.Lock()
				delete(wi.r.pending.m, wi.msg.TxnUUID)
				wi.r.wiCnt.Dec()
				wi.r.pending.Unlock()
				wi.r.DecPending()

				core.FreeLOM(wi.archlom)
			}
		case <-j.stopCh.Listen():
			return
		}
	}
}

////////////
// archwi //
////////////

func (wi *archwi) beginAppend() (lmfh cos.LomReader, err error) {
	msg := wi.msg
	if msg.Mime == archive.ExtTar {
		err = wi.openTarForAppend()
		if err == nil /*can append*/ || err != archive.ErrTarIsEmpty /*fail XactArch.Begin*/ {
			return nil, err
		}
	}

	// <extra copy>
	// prep to copy `lmfh` --> `wi.fh` with subsequent APPEND-ing
	// msg.Mime has been already validated (see ais/* for apc.ActArchive)
	lmfh, err = wi.archlom.Open()
	if err != nil {
		return nil, err
	}
	if wi.wfh, err = wi.archlom.CreateWork(wi.fqn); err != nil {
		cos.Close(lmfh)
		lmfh = nil
	}
	return lmfh, err
}

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
func (wi *archwi) do(lom *core.LOM, lrit *lrit) {
	var coldGet bool
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if !cos.IsNotExist(err, 0) {
			wi.r.AddErr(err, 5, cos.SmoduleXs)
			return
		}
		if coldGet = lom.Bck().IsRemote(); !coldGet {
			if lrit.lrp == lrpList {
				// listed, not found
				wi.r.AddErr(err, 5, cos.SmoduleXs)
			}
			return
		}
	}

	if coldGet {
		// cold
		if ecode, err := core.T.GetCold(context.Background(), lom, cmn.OwtGetLock); err != nil {
			if lrit.lrp != lrpList && cos.IsNotExist(err, ecode) {
				return // range or prefix, not found
			}
			wi.r.AddErr(err, 5, cos.SmoduleXs)
			return
		}
	}

	fh, err := cos.NewFileHandle(lom.FQN)
	if err != nil {
		wi.r.AddErr(err, 5, cos.SmoduleXs)
		return
	}
	if core.T.SID() != wi.tsi.ID() {
		wi.r.doSend(lom, wi, fh)
		return
	}
	// see Begin
	if wi.wfh == nil {
		// NOTE: unexpected and unlikely - aborting
		err = fmt.Errorf("%s: destination %q does not exist (not open)", wi.r.Name(), wi.fqn)
		wi.r.Abort(err)
		return
	}
	err = wi.writer.Write(wi.nameInArch(lom.ObjName), lom, fh /*reader*/)
	cos.Close(fh)
	if err == nil {
		wi.cnt.Inc()
	} else {
		wi.r.AddErr(err, 5, cos.SmoduleXs)
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
		finfo, err := os.Stat(wi.fqn)
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
