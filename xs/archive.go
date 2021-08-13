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
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

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

	delayUnregRecv    = 200 * time.Millisecond
	delayUnregRecvMax = 10 * delayUnregRecv // NOTE: too aggressive?
)

type (
	archFactory struct {
		xreg.RenewBase
		xact *XactCreateArchMultiObj
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
		r   *XactCreateArchMultiObj
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
		// append to archive
		appendPos int64 // for calculate stats correctly on append operation
	}
	XactCreateArchMultiObj struct {
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
		err      atomic.Value
		stopping atomic.Bool
	}
)

const (
	maxNumInParallel = 256
)

// interface guard
var (
	_ cluster.Xact   = (*XactCreateArchMultiObj)(nil)
	_ xreg.Renewable = (*archFactory)(nil)
	_ lrwi           = (*archwi)(nil)

	_ archWriter = (*tarWriter)(nil)
	_ archWriter = (*tgzWriter)(nil)
	_ archWriter = (*zipWriter)(nil)
)

/////////////////
// archFactory //
/////////////////

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
	r := &XactCreateArchMultiObj{t: p.T, bckFrom: p.Bck, workCh: workCh, config: config}
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

func (p *archFactory) newDM(bckFrom *cluster.Bck, r *XactCreateArchMultiObj) error {
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

////////////////////////////
// XactCreateArchMultiObj //
////////////////////////////

// limited pre-run abort
func (r *XactCreateArchMultiObj) TxnAbort() {
	err := cmn.NewAbortedError(r.String())
	if r.dm.IsOpen() {
		r.dm.Close(err)
	}
	r.dm.UnregRecv()
	r.XactBase.Finish(err)
}

func (r *XactCreateArchMultiObj) Begin(msg *cmn.ArchiveMsg) (err error) {
	lom := cluster.AllocLOM(msg.ArchName)
	if err = lom.Init(msg.ToBck); err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}
	debug.Assert(lom.FullName() == msg.FullName()) // relying on it

	wi := &archwi{r: r, msg: msg, lom: lom}
	wi.fqn = fs.CSM.GenContentFQN(wi.lom, fs.WorkfileType, fs.WorkfileCreateArch)
	wi.cksum.Init(lom.CksumConf().Type)

	smap := r.t.Sowner().Get()
	wi.refc.Store(int32(smap.CountTargets() - 1))
	wi.tsi, err = cluster.HrwTarget(msg.ToBck.MakeUname(msg.ArchName), smap)
	if err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}

	// NOTE: creating archive at BEGIN time; TODO: cleanup upon ABORT
	if r.t.Snode().ID() == wi.tsi.ID() {
		if errExists := fs.Access(wi.lom.FQN); errExists != nil {
			wi.fh, err = wi.lom.CreateFile(wi.fqn)
		} else if wi.msg.AllowAppendToExisting {
			switch msg.Mime {
			case cos.ExtTar:
				err = wi.openTarForAppend()
			default:
				err = fmt.Errorf("unsupported archive type %s, only %s is supported", msg.Mime, cos.ExtTar)
			}
		} else {
			err = fmt.Errorf("%s: not allowed to append to an existing %s", r.t.Snode(), msg.FullName())
		}
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
	r.pending.m[msg.TxnUUID] = wi
	r.pending.Unlock()
	return
}

func (r *XactCreateArchMultiObj) Do(msg *cmn.ArchiveMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactCreateArchMultiObj) Run(wg *sync.WaitGroup) {
	var err error
	glog.Infoln(r.String())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			if r.err.Load() != nil { // see raiseErr()
				goto fin
			}
			r.pending.RLock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			debug.Assert(ok)
			var (
				smap    = r.t.Sowner().Get()
				lrit    = &lriterator{}
				freeLOM = false // not delegating to iterator
			)
			lrit.init(r, r.t, &msg.ListRangeMsg, freeLOM)
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
				wi.abort(err)
				goto fin
			}
			if r.t.Snode().ID() == wi.tsi.ID() {
				go r.finalize(wi)
			} else {
				r.eoi(wi)
				r.pending.Lock()
				delete(r.pending.m, msg.TxnUUID)
				r.pending.Unlock()
				r.DecPending()
			}
		case <-r.IdleTimer():
			goto fin
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	r.fin(err)
}

func (r *XactCreateArchMultiObj) fin(err error) {
	r.stopping.Store(true)
	r.DemandBase.Stop()

	if err == nil {
		if errDetailed := r.err.Load(); errDetailed != nil {
			err = errDetailed.(error)
		}
	}
	r.dm.Close(err)
	go func() {
		var (
			total time.Duration
			l     = 1
		)
		for total < delayUnregRecvMax && l > 0 {
			r.pending.RLock()
			l = len(r.pending.m)
			r.pending.RUnlock()
			time.Sleep(delayUnregRecv)
			total += delayUnregRecv
		}
		r.dm.UnregRecv()
	}()
	r.Finish(err)
}

// send EOI (end of iteration) to the responsible target
// TODO: see xs/tcobjs.go
func (r *XactCreateArchMultiObj) eoi(wi *archwi) {
	o := transport.AllocSend()
	o.Hdr.Opcode = doneSendingOpcode
	o.Hdr.Opaque = []byte(wi.msg.TxnUUID)
	r.dm.Send(o, nil, wi.tsi)
}

func (r *XactCreateArchMultiObj) doSend(lom *cluster.LOM, wi *archwi, fh cos.ReadOpenCloser) {
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = wi.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
		hdr.Opaque = []byte(wi.msg.TxnUUID)
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
		cluster.FreeLOM(lom)
	}
	r.dm.Send(o, fh, wi.tsi)
}

func (r *XactCreateArchMultiObj) recvObjDM(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	defer cos.DrainReader(objReader)

	if r.stopping.Load() {
		return
	}
	txnUUID := string(hdr.Opaque)
	r.pending.RLock()
	wi, ok := r.pending.m[txnUUID]
	r.pending.RUnlock()
	debug.Assert(ok)
	debug.Assert(wi.tsi.ID() == r.t.Snode().ID())
	debug.Assert(wi.msg.TxnUUID == txnUUID)

	// NOTE: best-effort via ref-counting
	if hdr.Opcode == doneSendingOpcode {
		refc := wi.refc.Dec()
		debug.Assert(refc >= 0)
		return
	}
	debug.Assert(hdr.Opcode == 0)
	wi.writer.write(wi.nameInArch(hdr.ObjName), &hdr.ObjAttrs, objReader)
}

func (r *XactCreateArchMultiObj) finalize(wi *archwi) {
	if q := wi.quiesce(); q == cluster.QuiAborted {
		r.raiseErr(cmn.NewAbortedError(r.String()), 0, wi.msg.ContinueOnError)
	} else if q == cluster.QuiTimeout {
		r.raiseErr(fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout), 0, wi.msg.ContinueOnError)
	}

	errCode, err := r.fini(wi)
	r.pending.Lock()
	delete(r.pending.m, wi.msg.TxnUUID)
	r.pending.Unlock()
	r.DecPending()

	if err != nil {
		wi.abort(err)
		r.raiseErr(err, errCode, wi.msg.ContinueOnError)
	}
}

func (r *XactCreateArchMultiObj) raiseErr(err error, errCode int, contOnErr bool) {
	errDetailed := fmt.Errorf("%s[%s]: %v(%d)", r.t.Snode(), r, err, errCode)
	if !contOnErr {
		glog.Errorf("%v - terminating...", errDetailed)
		r.err.Store(errDetailed)
	} else {
		glog.Warningf("%v - ingnoring, continuing to process archiving transactions", errDetailed)
	}
}

func (r *XactCreateArchMultiObj) fini(wi *archwi) (errCode int, err error) {
	var size int64

	wi.writer.fini()
	r.t.MMSA().Free(wi.buf)

	if size, err = wi.finalize(); err != nil {
		return http.StatusInternalServerError, err
	}

	wi.lom.SetSize(size)
	cos.Close(wi.fh)

	errCode, err = r.t.FinalizeObj(wi.lom, wi.fqn)
	cluster.FreeLOM(wi.lom)

	r.ObjectsInc()
	r.BytesAdd(size - wi.appendPos)
	return
}

func (r *XactCreateArchMultiObj) Stats() cluster.XactStats {
	baseStats := r.DemandBase.Stats().(*xaction.BaseXactStatsExt)
	baseStats.Ext = &xaction.BaseXactDemandStatsExt{IsIdle: r.Pending() == 0}
	return baseStats
}

////////////
// archwi //
////////////
func (wi *archwi) do(lom *cluster.LOM, lrit *lriterator) {
	var (
		t       = lrit.t
		coldGet bool
	)
	debug.Assert(t == wi.r.t)
	debug.Assert(wi.r.bckFrom.Bck.Equal(lom.Bucket()))
	debug.Assert(lom.Bprops() != nil) // must be init-ed
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if !cmn.IsObjNotExist(err) {
			wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
			return
		}
		coldGet = lom.Bck().IsRemote()
		if !coldGet {
			wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
			return
		}
	}
	// cold
	if coldGet {
		if errCode, err := t.Backend(lom.Bck()).GetObj(lrit.ctx, lom); err != nil {
			if errCode == http.StatusNotFound || cmn.IsObjNotExist(err) {
				return
			}
			wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
			return
		}
	}
	fh, err := cos.NewFileHandle(lom.FQN)
	debug.AssertNoErr(err)
	if err != nil {
		wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
		return
	}
	if t.Snode().ID() != wi.tsi.ID() {
		wi.r.doSend(lom, wi, fh)
		return
	}
	debug.Assert(wi.fh != nil) // see Begin
	err = wi.writer.write(wi.nameInArch(lom.ObjName), lom, fh)
	cluster.FreeLOM(lom)
	cos.Close(fh)
	if err != nil {
		wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
	}
}

func (wi *archwi) quiesce() cluster.QuiRes {
	return wi.r.Quiesce(wi.r.config.Timeout.MaxKeepalive.D(), func(total time.Duration) cluster.QuiRes {
		return xaction.RefcntQuiCB(&wi.refc, wi.r.config.Timeout.SendFile.D()/2, total)
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
	return *(*string)(unsafe.Pointer(&buf))
}

func (wi *archwi) openTarForAppend() (err error) {
	if err := os.Rename(wi.lom.FQN, wi.fqn); err != nil {
		return err
	}
	wi.fh, err = cos.OpenTarForAppend(wi.lom.ObjName, wi.fqn)
	if err == nil {
		wi.appendPos, err = wi.fh.Seek(0, io.SeekCurrent)
		if err != nil {
			cos.Close(wi.fh)
			wi.abort(err)
		}
	}
	return err
}

func (wi *archwi) abort(err error) {
	if wi.appendPos == 0 {
		return
	}
	if errRm := os.Rename(wi.fqn, wi.lom.FQN); errRm != nil {
		glog.Errorf("nested error: %v --> %v", err, errRm)
	}
}

func (wi *archwi) finalize() (size int64, err error) {
	var cksum *cos.Cksum
	if wi.appendPos > 0 {
		var st os.FileInfo
		if st, err = os.Stat(wi.fqn); err == nil {
			size = st.Size()
		}
		cksum = cos.NewCksum(cos.ChecksumNone, "")
	} else {
		wi.cksum.Finalize()
		cksum = &wi.cksum.Cksum
		size = wi.cksum.Size
	}
	wi.lom.SetCksum(cksum)
	return size, err
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
