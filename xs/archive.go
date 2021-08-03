// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

const (
	doneSendingOpcode = 31415

	delayUnregRecv = 200 * time.Millisecond
)

type (
	archFactory struct {
		xreg.RenewBase
		xact *XactPutArchive
	}
	archWriter interface {
		write(nameInArch string, oah cmn.ObjAttrsHolder, reader io.Reader) error
		fini()
	}
	baseW struct {
		wmul   *cos.WriterMulti
		archwi *archwi
	}
	tarWriter struct {
		baseW
		tw *tar.Writer
	}
	tgzWriter struct {
		tw  tarWriter
		gzw *gzip.Writer
	}
	zipWriter struct {
		baseW
		zw *zip.Writer
	}
	archwi struct { // archival work item; implements lrwi
		r   *XactPutArchive
		msg *cmn.ArchiveMsg
		lom *cluster.LOM // of the archive
		fqn string       // workFQN --/--
		fh  *os.File     // --/--
		tsi *cluster.Snode
		// writing
		wmu    sync.Mutex
		buf    []byte
		writer archWriter
		cksum  cos.CksumHashSize
		err    error
		errCnt atomic.Int32
		// finishing
		refc atomic.Int32
	}
	XactPutArchive struct {
		xaction.DemandBase
		t       cluster.Target
		bckFrom *cluster.Bck
		dm      *bundle.DataMover
		workCh  chan *cmn.ArchiveMsg
		pending struct {
			sync.RWMutex
			m map[string]*archwi
		}
		config   *cmn.Config
		stopping atomic.Bool
	}
)

const (
	maxNumInParallel = 256
)

// interface guard
var (
	_ cluster.Xact   = (*XactPutArchive)(nil)
	_ xreg.Renewable = (*archFactory)(nil)
	_ lrwi           = (*archwi)(nil)

	_ archWriter = (*tarWriter)(nil)
	_ archWriter = (*tgzWriter)(nil)
	_ archWriter = (*zipWriter)(nil)
)

////////////////
// archFactory //
////////////////

func (*archFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &archFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}}
	return p
}

func (*archFactory) Kind() string        { return cmn.ActArchive }
func (p *archFactory) Get() cluster.Xact { return p.xact }

func (p *archFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func (p *archFactory) Start() error {
	var (
		config      = cmn.GCO.Get()
		totallyIdle = config.Timeout.SendFile.D()
		likelyIdle  = config.Timeout.MaxKeepalive.D()
		workCh      = make(chan *cmn.ArchiveMsg, maxNumInParallel)
	)
	r := &XactPutArchive{t: p.T, bckFrom: p.Bck, workCh: workCh, config: config}
	r.pending.m = make(map[string]*archwi, maxNumInParallel)
	p.xact = r
	r.DemandBase.Init(p.UUID(), cmn.ActArchive, p.Bck, totallyIdle, likelyIdle)
	if err := p.newDM(p.Bck, r); err != nil {
		return err
	}
	r.dm.SetXact(r)
	r.dm.Open()

	xaction.GoRunW(r)
	return nil
}

func (p *archFactory) newDM(bckFrom *cluster.Bck, r *XactPutArchive) error {
	// transport endpoint name (given xaction, m.b. identical across cluster)
	trname := "arch-" + bckFrom.Provider + "-" + bckFrom.Name
	dm, err := bundle.NewDataMover(p.T, trname, r.recvObjDM, cluster.RegularPut, bundle.Extra{Multiplier: 1})
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		if strings.Contains(err.Error(), "duplicate trname") {
			glog.Errorf("retry reg-recv %s", trname)
			time.Sleep(2 * delayUnregRecv)
			err = dm.RegRecv()
		}
		if err != nil {
			return err
		}
	}
	r.dm = dm
	return nil
}

////////////////////
// XactPutArchive //
////////////////////

// limited pre-run abort
func (r *XactPutArchive) TxnAbort() {
	err := cmn.NewAbortedError(r.String())
	if r.dm.IsOpen() {
		r.dm.Close(err)
	}
	r.dm.UnregRecv()
	r.XactBase.Finish(err)
}

func (r *XactPutArchive) Begin(msg *cmn.ArchiveMsg) (err error) {
	lom := cluster.AllocLOM(msg.ArchName)
	if err = lom.Init(msg.ToBck); err != nil {
		return
	}
	debug.Assert(lom.FullName() == msg.FullName()) // relying on it

	wi := &archwi{r: r, msg: msg, lom: lom}
	wi.fqn = fs.CSM.GenContentFQN(wi.lom, fs.WorkfileType, fs.WorkfileAppend)
	wi.cksum.Init(lom.CksumConf().Type)

	smap := r.t.Sowner().Get()
	wi.refc.Store(int32(smap.CountTargets() - 1))
	wi.tsi, err = cluster.HrwTarget(msg.ToBck.MakeUname(msg.ArchName), smap)
	if err != nil {
		return
	}

	// NOTE: creating archive at BEGIN time; TODO: cleanup upon ABORT
	if r.t.Snode().ID() == wi.tsi.ID() {
		wi.fh, err = wi.lom.CreateFile(wi.fqn)
		if err != nil {
			return
		}
		wi.buf, _ = r.t.MMSA().Alloc()
		switch msg.Mime {
		case cos.ExtTar:
			tw := &tarWriter{}
			tw.init(wi)
		case cos.ExtTgz, cos.ExtTarTgz:
			tzw := &tgzWriter{}
			tzw.init(wi)
		case cos.ExtZip:
			zw := &zipWriter{}
			zw.init(wi)
		default:
			debug.AssertMsg(false, msg.Mime)
			return
		}
	}
	r.pending.Lock()
	r.pending.m[msg.FullName()] = wi
	r.pending.Unlock()
	return
}

func (r *XactPutArchive) Do(msg *cmn.ArchiveMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactPutArchive) Run(wg *sync.WaitGroup) {
	var err error
	glog.Infoln(r.String())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			fullname := msg.FullName()
			r.pending.RLock()
			wi := r.pending.m[fullname]
			r.pending.RUnlock()
			var (
				smap             = r.t.Sowner().Get()
				lrit             = &lriterator{}
				ignoreBackendErr = !msg.IsList() // list defaults to aborting on errors other than non-existence
				freeLOM          = false         // not delegating the responsibility - doing it
			)
			lrit.init(r, r.t, &msg.ListRangeMsg, ignoreBackendErr, freeLOM)
			if msg.IsList() {
				err = lrit.iterateList(wi, smap)
			} else {
				err = lrit.iterateRange(wi, smap)
			}
			if wi.errCnt.Load() > 0 {
				wi.wmu.Lock()
				err = wi.err
				wi.wmu.Unlock()
			}
			if r.Aborted() || err != nil {
				goto fin
			}
			if r.t.Snode().ID() == wi.tsi.ID() {
				go r.finalize(wi, fullname)
			} else {
				r.eoi(wi)
				r.DecPending()
			}
		case <-r.IdleTimer():
			goto fin
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	r.stopping.Store(true)
	r.DemandBase.Stop()

	r.dm.Close(err)
	go func() {
		time.Sleep(delayUnregRecv)
		r.dm.UnregRecv()
	}()
	r.Finish(err)
}

// send EOI (end of iteration) to the responsible target
func (r *XactPutArchive) eoi(wi *archwi) {
	o := transport.AllocSend()
	o.Hdr.Opcode = doneSendingOpcode
	o.Hdr.Opaque = []byte(wi.msg.FullName())
	r.dm.Send(o, nil, wi.tsi)
}

func (r *XactPutArchive) doSend(lom *cluster.LOM, wi *archwi, fh cos.ReadOpenCloser) {
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = wi.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
		hdr.Opaque = []byte(wi.msg.FullName()) // NOTE
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	r.dm.Send(o, fh, wi.tsi)
}

func (r *XactPutArchive) recvObjDM(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	defer cos.DrainReader(objReader)

	if r.stopping.Load() {
		return
	}
	r.pending.RLock()
	wi, ok := r.pending.m[string(hdr.Opaque)] // NOTE: fullname
	r.pending.RUnlock()
	debug.Assert(ok)
	debug.Assert(wi.tsi.ID() == r.t.Snode().ID())

	// NOTE: best-effort via ref-counting
	if hdr.Opcode == doneSendingOpcode {
		refc := wi.refc.Dec()
		debug.Assert(refc >= 0)
	} else {
		debug.Assert(hdr.Opcode == 0)
		wi.writer.write(hdr.ObjName, &hdr.ObjAttrs, objReader)
	}
}

func (r *XactPutArchive) finalize(wi *archwi, fullname string) {
	if wi == nil || wi.writer == nil {
		r.DecPending()
		return // nothing to do
	}
	if q := wi.quiesce(); q == cluster.QuiAborted {
		r.raiseErr(cmn.NewAbortedError(r.String()), 0)
	} else if q == cluster.QuiTimeout {
		r.raiseErr(fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout), 0)
	}

	errCode, err := r.fini(wi)
	r.pending.Lock()
	delete(r.pending.m, fullname)
	r.pending.Unlock()
	r.DecPending()

	if err != nil {
		r.raiseErr(err, errCode)
	}
}

// TODO -- FIXME: stateful
func (r *XactPutArchive) raiseErr(err error, errCode int) {
	glog.Errorf("%s: %v(%d)", r.t.Snode(), err, errCode)
}

func (r *XactPutArchive) fini(wi *archwi) (errCode int, err error) {
	var size int64
	wi.writer.fini()
	r.t.MMSA().Free(wi.buf)

	wi.cksum.Finalize()
	wi.lom.SetCksum(&wi.cksum.Cksum)
	size = wi.cksum.Size

	wi.lom.SetSize(size)
	cos.Close(wi.fh)

	errCode, err = r.t.FinalizeObj(wi.lom, wi.fqn)
	cluster.FreeLOM(wi.lom)

	r.ObjectsInc()
	r.BytesAdd(size)
	return
}

func (r *XactPutArchive) Stats() cluster.XactStats {
	baseStats := r.DemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}

////////////
// archwi //
////////////
func (wi *archwi) do(lom *cluster.LOM, lrit *lriterator) error {
	var (
		t       = lrit.t
		coldGet bool
	)
	debug.Assert(t == wi.r.t)
	debug.Assert(wi.r.bckFrom.Bck.Equal(lom.Bucket()))
	debug.Assert(lom.Bprops() != nil) // must be init-ed
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if !cmn.IsObjNotExist(err) {
			return err
		}
		coldGet = lom.Bck().IsRemote()
		if !coldGet {
			return err
		}
	}
	// cold
	if coldGet {
		if errCode, err := t.Backend(lom.Bck()).GetObj(lrit.ctx, lom); err != nil {
			if errCode == http.StatusNotFound || cmn.IsObjNotExist(err) {
				return nil
			}
			if lrit.ignoreBackendErr {
				glog.Warning(err)
				err = nil
			}
			return err
		}
	}
	fh, err := cos.NewFileHandle(lom.FQN)
	debug.AssertNoErr(err)
	if err != nil {
		return err
	}
	if t.Snode().ID() != wi.tsi.ID() {
		wi.r.doSend(lom, wi, fh)
		return nil
	}
	debug.Assert(wi.fh != nil) // see Begin
	err = wi.writer.write(lom.ObjName, lom, fh)
	cluster.FreeLOM(lom)
	cos.Close(fh)
	return err
}

func (wi *archwi) quiesce() cluster.QuiRes {
	return wi.r.Quiesce(wi.r.config.Timeout.MaxKeepalive.D(), func(total time.Duration) cluster.QuiRes {
		return xaction.RefcntQuiCB(&wi.refc, wi.r.config.Timeout.SendFile.D()/2, total)
	})
}

///////////////
// tarWriter //
///////////////
func (tw *tarWriter) init(wi *archwi) {
	tw.archwi = wi
	tw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	tw.tw = tar.NewWriter(tw.wmul)
	wi.writer = tw
}

func (tw *tarWriter) fini() { tw.tw.Close() }

func (tw *tarWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) (err error) {
	tarhdr := new(tar.Header)
	tarhdr.Typeflag = tar.TypeReg

	tarhdr.Size = oah.SizeBytes()
	tarhdr.ModTime = time.Unix(0, oah.AtimeUnix())
	tarhdr.Name = fullname

	// one at a time
	tw.archwi.wmu.Lock()
	if err = tw.tw.WriteHeader(tarhdr); err == nil {
		_, err = io.CopyBuffer(tw.tw, reader, tw.archwi.buf)
	}
	if err != nil {
		tw.archwi.err = err
		tw.archwi.errCnt.Inc()
	}
	tw.archwi.wmu.Unlock()
	return
}

///////////////
// tgzWriter //
///////////////
func (tzw *tgzWriter) init(wi *archwi) {
	tzw.tw.archwi = wi
	tzw.tw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	tzw.gzw = gzip.NewWriter(tzw.tw.wmul)
	tzw.tw.tw = tar.NewWriter(tzw.gzw)
	wi.writer = tzw
}

func (tzw *tgzWriter) fini() {
	tzw.tw.fini()
	tzw.gzw.Close()
}

func (tzw *tgzWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) error {
	return tzw.tw.write(fullname, oah, reader)
}

///////////////
// zipWriter //
///////////////

// wi.writer = &zipWriter{archwi: wi, w: zip.NewWriter(wi.fh)}
func (zw *zipWriter) init(wi *archwi) {
	zw.archwi = wi
	zw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	zw.zw = zip.NewWriter(zw.wmul)
	wi.writer = zw
}

func (zw *zipWriter) fini() { zw.zw.Close() }

func (zw *zipWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) error {
	ziphdr := new(zip.FileHeader)

	ziphdr.Name = fullname
	ziphdr.Comment = fullname
	ziphdr.UncompressedSize64 = uint64(oah.SizeBytes())
	ziphdr.Modified = time.Unix(0, oah.AtimeUnix())

	zw.archwi.wmu.Lock()
	zipw, err := zw.zw.CreateHeader(ziphdr)
	if err == nil {
		_, err = io.CopyBuffer(zipw, reader, zw.archwi.buf)
	}
	if err != nil {
		zw.archwi.err = err
		zw.archwi.errCnt.Inc()
	}
	zw.archwi.wmu.Unlock()
	return err
}
