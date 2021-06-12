// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
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
		args *xreg.PutArchiveArgs
	}
	XactPutArchive struct {
		xaction.XactDemandBase
		t       cluster.Target
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		dm      *bundle.DataMover
		workCh  chan *cmn.ArchiveMsg
		// TODO -- FIXME: per archive-msg context
		lom     *cluster.LOM
		workFQN string
		archfh  *os.File
		tw      *tar.Writer
		mu      sync.Mutex
	}
)

// interface guard
var (
	_ cluster.Xact    = (*XactPutArchive)(nil)
	_ xreg.BckFactory = (*archFactory)(nil)
)

func _fullname(bucket, obj string) string { return filepath.Join(bucket, obj) }

////////////////
// archFactory //
////////////////

func (*archFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	return &archFactory{
		t:    args.T,
		uuid: args.UUID,
		args: args.Custom.(*xreg.PutArchiveArgs),
	}
}

func (*archFactory) Kind() string        { return cmn.ActArchive }
func (p *archFactory) Get() cluster.Xact { return p.xact }

func (p *archFactory) Start(_ cmn.Bck) error {
	var (
		xargs       = xaction.Args{ID: xaction.BaseID(p.uuid), Kind: cmn.ActArchive, Bck: &p.args.BckFrom.Bck}
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
	)
	r := &XactPutArchive{
		XactDemandBase: *xaction.NewXDB(xargs, totallyIdle, likelyIdle),
		t:              p.t,
		bckFrom:        p.args.BckFrom,
		bckTo:          p.args.BckTo,
		workCh:         make(chan *cmn.ArchiveMsg, 16),
	}
	p.xact = r
	r.InitIdle()
	if err := p.newDM(p.uuid, r); err != nil {
		return err
	}
	r.dm.SetXact(r)
	r.dm.Open()

	go r.Run()
	return nil
}

func (p *archFactory) newDM(uuid string, r *XactPutArchive) error {
	dm, err := bundle.NewDataMover(p.t, "arch-"+uuid, r.recvObjDM, cluster.RegularPut, bundle.Extra{Multiplier: 1})
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	r.dm = dm
	return nil
}

/////////////////
// XactPutArchive //
/////////////////

func (r *XactPutArchive) Do(msg *cmn.ArchiveMsg) {
	smap := r.t.Sowner().Get()
	tsi, err := cluster.HrwTarget(r.bckTo.MakeUname(msg.ArchName), smap)
	debug.AssertNoErr(err)
	if r.t.Snode().ID() == tsi.ID() {
		r.lom = cluster.AllocLOM(msg.ArchName) // TODO -- FIXME: via pending
		err := r.lom.Init(r.bckTo.Bck)
		debug.AssertNoErr(err)
		r.workFQN = fs.CSM.GenContentFQN(r.lom, fs.WorkfileType, fs.WorkfileAppend) // ditto
		r.createArch(msg.ArchName, r.workFQN)
	}
	r.IncPending()
	r.workCh <- msg
}

func (r *XactPutArchive) Run() {
	glog.Infoln(r.String())
	for {
		select {
		case msg := <-r.workCh:
			smap := r.t.Sowner().Get()
			for _, objName := range msg.ListMsg.ObjNames {
				lom := cluster.AllocLOM(objName)
				if err := r.doOne(lom, msg, smap); err != nil {
					cluster.FreeLOM(lom)
				}
			}
			time.Sleep(3 * time.Second) // TODO -- FIXME: via [target => last] accounting
			r.finalize()                // TODO -- FIXME: use poi
			r.DecPending()
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

func (r *XactPutArchive) doOne(lom *cluster.LOM, msg *cmn.ArchiveMsg, smap *cluster.Smap) (err error) {
	if err = lom.Init(r.bckFrom.Bck); err != nil {
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

	tsi, err := cluster.HrwTarget(r.bckTo.MakeUname(msg.ArchName), smap)
	debug.AssertNoErr(err)
	if r.t.Snode().ID() != tsi.ID() {
		r.doSend(lom, fh, tsi)
		return
	}

	r.addToArch(lom, nil, fh)
	cluster.FreeLOM(lom)
	cos.Close(fh)
	return
}

func (r *XactPutArchive) doSend(lom *cluster.LOM, fh cos.ReadOpenCloser, tsi *cluster.Snode) {
	// TODO -- FIXME: copy/paste from ais _sendObjDM
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = r.bckTo.Bck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.Size = lom.Size()
		hdr.ObjAttrs.Atime = lom.AtimeUnix()
		if cksum := lom.Cksum(); cksum != nil {
			hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue = cksum.Get()
		}
		hdr.ObjAttrs.Version = lom.Version()
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	r.dm.Send(o, fh, tsi)
}

// TODO -- FIXME: err hdl
func (r *XactPutArchive) addToArch(lom *cluster.LOM, hdr *transport.ObjHdr, reader io.Reader) {
	header := new(tar.Header) // TODO -- FIXME: support all 3
	header.Typeflag = tar.TypeReg

	if lom != nil { // local
		header.Size = lom.Size()
		header.ModTime = lom.Atime()
		header.Name = _fullname(lom.BckName(), lom.ObjName)
	} else { // recv
		header.Size = hdr.ObjAttrs.Size
		header.ModTime = time.Unix(0, hdr.ObjAttrs.Atime)
		header.Name = _fullname(hdr.Bck.Name, hdr.ObjName)
	}

	// NOTE: one at a time
	r.mu.Lock()
	err := r.tw.WriteHeader(header)
	debug.AssertNoErr(err)

	_, err = io.Copy(r.tw, reader) // TODO -- FIXME: use slab/buffer
	debug.AssertNoErr(err)
	r.mu.Unlock()
}

func (r *XactPutArchive) createArch(archName, workFQN string) {
	debug.Assert(strings.HasSuffix(archName, cos.ExtTar)) // TODO -- FIXME: support all 3
	r.archfh, _ = r.lom.CreateFile(workFQN)
	r.tw = tar.NewWriter(r.archfh) // TODO -- FIXME: must be per message not xact
}

// TODO -- FIXME
func (r *XactPutArchive) recvObjDM(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	defer cos.DrainReader(objReader)
	r.addToArch(nil, &hdr, objReader)
}

// TODO -- FIXME: use poi.finalize()
func (r *XactPutArchive) finalize() {
	if r.tw == nil {
		return // nothing to do
	}
	r.tw.Close()
	r.archfh.Close()
	err := cos.Rename(r.workFQN, r.lom.FQN)
	debug.AssertNoErr(err)

	finfo, err := os.Stat(r.lom.FQN)
	debug.AssertNoErr(err)
	r.lom.SetSize(finfo.Size())

	err = r.lom.Persist(true)
	debug.AssertNoErr(err)
	glog.Infof("%s: new archive %s (size %d)", r.t.Snode(), r.lom, r.lom.Size())
}

func (r *XactPutArchive) Stats() cluster.XactStats {
	baseStats := r.XactDemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}
