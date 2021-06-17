// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"archive/tar"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	archFactory struct {
		xreg.BaseBckEntry
		xact *XactPutArchive
		t    cluster.Target
		uuid string
	}
	zwork struct {
		msg *cmn.ArchiveMsg
		lom *cluster.LOM // of the archive
		fqn string       // workFQN --/--
		fh  *os.File     // --/--
		tw  *tar.Writer
		mu  sync.Mutex
		tsi *cluster.Snode
	}
	XactPutArchive struct {
		xaction.XactDemandBase
		t       cluster.Target
		bckFrom cmn.Bck
		dm      *bundle.DataMover
		workCh  chan *cmn.ArchiveMsg
		pending struct {
			sync.RWMutex
			m map[string]*zwork
		}
	}
)

const (
	maxNumInParallel = 64
)

// interface guard
var (
	_ cluster.Xact    = (*XactPutArchive)(nil)
	_ xreg.BckFactory = (*archFactory)(nil)
)

////////////////
// archFactory //
////////////////

func (*archFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	return &archFactory{t: args.T, uuid: args.UUID}
}

func (*archFactory) Kind() string        { return cmn.ActArchive }
func (p *archFactory) Get() cluster.Xact { return p.xact }

func (p *archFactory) Start(bckFrom cmn.Bck) error {
	var (
		xargs       = xaction.Args{ID: xaction.BaseID(p.uuid), Kind: cmn.ActArchive, Bck: &bckFrom}
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	r := &XactPutArchive{
		XactDemandBase: *xaction.NewXDB(xargs, totallyIdle, likelyIdle),
		t:              p.t,
		bckFrom:        bckFrom,
		workCh:         make(chan *cmn.ArchiveMsg, maxNumInParallel),
	}
	r.pending.m = make(map[string]*zwork, maxNumInParallel)
	p.xact = r
	r.InitIdle()
	if err := p.newDM(bckFrom, r); err != nil {
		return err
	}
	r.dm.SetXact(r)
	r.dm.Open()

	go r.Run()
	return nil
}

func (p *archFactory) newDM(bckFrom cmn.Bck, r *XactPutArchive) error {
	// NOTE: transport stream name
	trname := "arch-" + bckFrom.Provider + "-" + bckFrom.Name
	dm, err := bundle.NewDataMover(p.t, trname, r.recvObjDM, cluster.RegularPut, bundle.Extra{Multiplier: 1})
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	r.dm = dm
	return nil
}

////////////////////
// XactPutArchive //
////////////////////

func (r *XactPutArchive) Begin(msg *cmn.ArchiveMsg) (err error) {
	debug.Assert(strings.HasSuffix(msg.ArchName, cos.ExtTar)) // TODO: NIY
	lom := cluster.AllocLOM(msg.ArchName)
	if err = lom.Init(msg.ToBck); err != nil {
		return
	}
	debug.Assert(lom.FullName() == msg.FullName()) // relying on it

	work := &zwork{msg: msg, lom: lom}
	work.fqn = fs.CSM.GenContentFQN(work.lom, fs.WorkfileType, fs.WorkfileAppend)

	smap := r.t.Sowner().Get()
	work.tsi, err = cluster.HrwTarget(msg.ToBck.MakeUname(msg.ArchName), smap)
	if err != nil {
		return
	}

	// NOTE: creating archive at BEGIN time; TODO: cleanup upon ABORT
	if r.t.Snode().ID() == work.tsi.ID() {
		work.fh, err = work.lom.CreateFile(work.fqn)
		if err != nil {
			return
		}
		work.tw = tar.NewWriter(work.fh)
	}
	r.pending.Lock()
	r.pending.m[msg.FullName()] = work
	r.pending.Unlock()
	return
}

func (r *XactPutArchive) Do(msg *cmn.ArchiveMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactPutArchive) Run() {
	glog.Infoln(r.String())
	for {
		select {
		case msg := <-r.workCh:
			fullname := msg.FullName()
			r.pending.RLock()
			work := r.pending.m[fullname]
			r.pending.RUnlock()
			debug.Assert(work != nil)
			for _, objName := range msg.ListMsg.ObjNames {
				lom := cluster.AllocLOM(objName)
				if err := work.do(lom, r); err != nil {
					cluster.FreeLOM(lom)
				}
			}
			if r.t.Snode().ID() == work.tsi.ID() {
				go r.finalize(work, fullname)
			} else {
				r.DecPending()
			}
		case <-r.IdleTimer():
			r.XactDemandBase.Stop()
			r.Finish(nil)
			goto fin
		case <-r.ChanAbort():
			r.XactDemandBase.Stop()
			goto fin
		}
	}
fin:
	var (
		err    error
		config = cmn.GCO.Get()
	)
	if q := r.dm.Quiesce(config.Rebalance.Quiesce.D()); q == cluster.QuiAborted {
		err = cmn.NewAbortedError(r.String())
	}
	r.dm.Close(err)
	r.dm.UnregRecv()

	r.Finish(err)
}

func (r *XactPutArchive) doSend(lom *cluster.LOM, work *zwork, fh cos.ReadOpenCloser) {
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = work.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.Size = lom.Size()
		hdr.ObjAttrs.Atime = lom.AtimeUnix()
		if cksum := lom.Cksum(); cksum != nil {
			hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue = cksum.Get()
		}
		hdr.ObjAttrs.Version = lom.Version()
		hdr.Opaque = []byte(work.msg.FullName()) // NOTE
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	r.dm.Send(o, fh, work.tsi)
}

func (r *XactPutArchive) recvObjDM(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	defer cos.DrainReader(objReader)

	r.pending.RLock()
	work, ok := r.pending.m[string(hdr.Opaque)] // NOTE: fullname
	r.pending.RUnlock()
	debug.Assert(ok)
	debug.Assert(work.tsi.ID() == r.t.Snode().ID())

	work.addToArch(nil, &hdr, objReader)
}

func (r *XactPutArchive) finalize(work *zwork, fullname string) {
	if work == nil || work.tw == nil {
		r.DecPending()
		return // nothing to do
	}
	time.Sleep(3 * time.Second) // TODO -- FIXME: via [target => last] accounting

	errCode, err := r._fini(work)
	if err != nil {
		glog.Errorf("%s: %v(%d)", r.t.Snode(), err, errCode) // TODO
	}
	r.pending.Lock()
	delete(r.pending.m, fullname)
	r.pending.Unlock()
	r.DecPending()
}

func (r *XactPutArchive) _fini(work *zwork) (errCode int, err error) {
	work.tw.Close()

	size, err := work.fh.Seek(0, io.SeekCurrent)
	if err != nil {
		debug.AssertNoErr(err)
		return
	}
	work.lom.SetSize(size)
	work.lom.SetCksum(cos.NewCksum(cos.ChecksumNone, ""))
	cos.Close(work.fh)

	errCode, err = r.t.FinalizeObj(work.lom, work.fqn, size)
	cluster.FreeLOM(work.lom)
	return
}

func (r *XactPutArchive) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}

///////////
// zwork //
///////////
func (work *zwork) do(lom *cluster.LOM, r *XactPutArchive) (err error) {
	if err = lom.Init(r.bckFrom); err != nil {
		return
	}
	if err = lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		return
	}

	fh, err := cos.NewFileHandle(lom.FQN)
	debug.AssertNoErr(err)
	if err != nil {
		return err
	}

	debug.AssertNoErr(err)
	if r.t.Snode().ID() != work.tsi.ID() {
		r.doSend(lom, work, fh)
		return
	}
	debug.Assert(work.fh != nil) // see Begin
	work.addToArch(lom, nil, fh)
	cluster.FreeLOM(lom)
	cos.Close(fh)
	return
}

func (work *zwork) addToArch(lom *cluster.LOM, hdr *transport.ObjHdr, reader io.Reader) {
	tarhdr := new(tar.Header)
	tarhdr.Typeflag = tar.TypeReg

	if lom != nil { // local
		tarhdr.Size = lom.Size()
		tarhdr.ModTime = lom.Atime()
		tarhdr.Name = lom.FullName()
	} else { // recv
		tarhdr.Size = hdr.ObjAttrs.Size
		tarhdr.ModTime = time.Unix(0, hdr.ObjAttrs.Atime)
		tarhdr.Name = hdr.FullName()
	}

	// one at a time
	work.mu.Lock()
	err := work.tw.WriteHeader(tarhdr)
	debug.AssertNoErr(err)

	_, err = io.Copy(work.tw, reader)
	work.mu.Unlock()
	debug.AssertNoErr(err)
}
