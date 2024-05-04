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
		writer  archive.Writer
		r       *XactArch
		msg     *cmn.ArchiveBckMsg
		tsi     *meta.Snode
		archlom *core.LOM
		fqn     string   // workFQN --/--
		wfh     *os.File // --/--
		cksum   cos.CksumHashSize
		cnt     atomic.Int32 // num archived
		// tar only
		appendPos int64 // append to existing
		tarFormat tar.Format
		// finishing
		refc atomic.Int32
	}
	XactArch struct {
		streamingX
		workCh  chan *cmn.ArchiveBckMsg
		bckTo   *meta.Bck
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
	workCh := make(chan *cmn.ArchiveBckMsg, maxNumInParallel)
	r := &XactArch{streamingX: streamingX{p: &p.streamingF, config: cmn.GCO.Get()}, workCh: workCh}
	r.pending.m = make(map[string]*archwi, maxNumInParallel)
	p.xctn = r
	r.DemandBase.Init(p.UUID() /*== p.Args.UUID above*/, p.kind, p.Bck /*from*/, xact.IdleDefault)

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
		return
	}
	nat := smap.CountActiveTs()
	wi.refc.Store(int32(nat - 1))

	wi.tsi, err = smap.HrwName2T(msg.ToBck.MakeUname(msg.ArchName))
	if err != nil {
		r.AddErr(err, 4, cos.SmoduleXs)
		return
	}

	// fcreate at BEGIN time
	if core.T.SID() == wi.tsi.ID() {
		var (
			s           string
			lmfh        *os.File
			finfo, errX = os.Stat(wi.archlom.FQN)
			exists      = errX == nil
		)
		if exists && wi.msg.AppendIfExists {
			s = " append"
			lmfh, err = wi.beginAppend()
		} else {
			wi.wfh, err = wi.archlom.CreateFile(wi.fqn)
		}
		if err != nil {
			return
		}
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Infof("%s: begin%s %s", r.Base.Name(), s, msg.Cname())
		}

		// construct format-specific writer; serialize for multi-target conc. writing
		opts := archive.Opts{Serialize: nat > 1, TarFormat: wi.tarFormat}
		wi.writer = archive.NewWriter(msg.Mime, wi.wfh, &wi.cksum, &opts)

		// append case (above)
		if lmfh != nil {
			err = wi.writer.Copy(lmfh, finfo.Size())
			if err != nil {
				wi.writer.Fini()
				wi.cleanup()
				return
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
	return
}

func (r *XactArch) Do(msg *cmn.ArchiveBckMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactArch) Run(wg *sync.WaitGroup) {
	var err error
	nlog.Infoln(r.Name())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			r.pending.RLock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			if !ok {
				debug.Assert(r.ErrCnt() > 0) // see cleanup
				goto fin
			}
			var (
				smap = core.T.Sowner().Get()
				lrit = &lriterator{}
			)
			err = lrit.init(r, &msg.ListRange, r.Bck(), true /*TODO: remove blocking*/)
			if err != nil {
				r.Abort(err)
				goto fin
			}
			err = lrit.run(wi, smap)
			if err != nil {
				r.AddErr(err)
			}
			lrit.wait()
			if r.Err() != nil {
				wi.cleanup()
				goto fin
			}
			if core.T.SID() == wi.tsi.ID() {
				go r.finalize(wi) // async finalize this shard
			} else {
				r.sendTerm(wi.msg.TxnUUID, wi.tsi, nil)
				r.pending.Lock()
				delete(r.pending.m, msg.TxnUUID)
				r.wiCnt.Dec()
				r.pending.Unlock()
				r.DecPending()

				core.FreeLOM(wi.archlom)
			}
		case <-r.IdleTimer():
			goto fin
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
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
	txnUUID := string(hdr.Opaque)
	r.pending.RLock()
	wi, ok := r.pending.m[txnUUID]
	r.pending.RUnlock()
	if !ok {
		if r.Finished() || r.IsAborted() {
			return nil
		}
		cnt, err := r.JoinErr()
		debug.Assert(cnt > 0) // see cleanup
		return err
	}
	debug.Assert(wi.tsi.ID() == core.T.SID() && wi.msg.TxnUUID == txnUUID)

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

	ecode, err := r.fini(wi)
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

func (r *XactArch) fini(wi *archwi) (ecode int, err error) {
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

	wi.archlom.SetSize(size)
	cos.Close(wi.wfh)
	wi.wfh = nil

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
// archwi //
////////////

func (wi *archwi) beginAppend() (lmfh *os.File, err error) {
	msg := wi.msg
	if msg.Mime == archive.ExtTar {
		if err = wi.openTarForAppend(); err == nil || err != archive.ErrTarIsEmpty {
			return
		}
	}
	// msg.Mime has been already validated (see ais/* for apc.ActArchive)
	// prep to copy `lmfh` --> `wi.fh` with subsequent APPEND-ing
	lmfh, err = wi.archlom.OpenFile()
	if err != nil {
		return
	}
	if wi.wfh, err = wi.archlom.CreateFile(wi.fqn); err != nil {
		cos.Close(lmfh)
		lmfh = nil
	}
	return
}

func (wi *archwi) openTarForAppend() (err error) {
	if err = os.Rename(wi.archlom.FQN, wi.fqn); err != nil {
		return
	}
	// open (rw) lom itself
	wi.wfh, wi.tarFormat, err = archive.OpenTarSeekEnd(wi.archlom.ObjName, wi.fqn)
	if err != nil {
		goto roll
	}
	wi.appendPos, err = wi.wfh.Seek(0, io.SeekCurrent)
	if err == nil {
		return // can append
	}
	wi.appendPos, wi.tarFormat = 0, tar.FormatUnknown // reset
	cos.Close(wi.wfh)
	wi.wfh = nil
roll:
	if errV := wi.archlom.RenameFrom(wi.fqn); errV != nil {
		nlog.Errorf("%s: nested error: failed to append %s (%v) and rename back from %s (%v)",
			wi.tsi, wi.archlom, err, wi.fqn, errV)
	} else {
		wi.fqn = ""
	}
	return
}

// multi-object iterator i/f: "handle work item"
func (wi *archwi) do(lom *core.LOM, lrit *lriterator) {
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
	debug.Assert(wi.wfh != nil) // see Begin
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
	if wi.appendPos > 0 {
		size, err := wi.wfh.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		debug.Assertf(size > wi.appendPos, "%d vs %d", size, wi.appendPos)
		// checksum traded off
		wi.archlom.SetCksum(cos.NewCksum(cos.ChecksumNone, ""))
		return size, nil
	}
	wi.cksum.Finalize()
	wi.archlom.SetCksum(&wi.cksum.Cksum)
	return wi.cksum.Size, nil
}
