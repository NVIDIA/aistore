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
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	archFactory struct {
		streamingF
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
		refc       atomic.Int32
		finalizing atomic.Bool
		// append to archive
		appendPos int64 // for calculate stats correctly on append operation
	}
	XactCreateArchMultiObj struct {
		streamingX
		bckFrom *cluster.Bck
		workCh  chan *cmn.ArchiveMsg
		pending struct {
			sync.RWMutex
			m map[string]*archwi
		}
		config *cmn.Config
	}
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
	p := &archFactory{streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: cmn.ActArchive}}
	return p
}

func (p *archFactory) Start() error {
	workCh := make(chan *cmn.ArchiveMsg, maxNumInParallel)
	r := &XactCreateArchMultiObj{streamingX: streamingX{p: &p.streamingF}, bckFrom: p.Bck, workCh: workCh, config: cmn.GCO.Get()}
	r.pending.m = make(map[string]*archwi, maxNumInParallel)
	p.xctn = r
	r.DemandBase.Init(p.UUID(), cmn.ActArchive, p.Bck, 0 /*use default*/)
	if err := p.newDM("arch", r.recv, 0); err != nil {
		return err
	}
	r.p.dm.SetXact(r)
	r.p.dm.Open()

	xact.GoRunW(r)
	return nil
}

////////////////////////////
// XactCreateArchMultiObj //
////////////////////////////

func (r *XactCreateArchMultiObj) Begin(msg *cmn.ArchiveMsg) (err error) {
	lom := cluster.AllocLOM(msg.ArchName)
	if err = lom.Init(msg.ToBck); err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}
	debug.Assert(lom.FullName() == msg.FullName()) // relying on it

	wi := &archwi{r: r, msg: msg, lom: lom}
	wi.fqn = fs.CSM.Gen(wi.lom, fs.WorkfileType, fs.WorkfileCreateArch)
	wi.cksum.Init(lom.CksumConf().Type)

	smap := r.p.T.Sowner().Get()
	wi.refc.Store(int32(smap.CountTargets() - 1))
	wi.tsi, err = cluster.HrwTarget(msg.ToBck.MakeUname(msg.ArchName), smap)
	if err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}

	// NOTE: creating archive at BEGIN time (see cleanup)
	if r.p.T.Snode().ID() == wi.tsi.ID() {
		if errExists := cos.Stat(wi.lom.FQN); errExists != nil {
			wi.fh, err = wi.lom.CreateFile(wi.fqn)
		} else if wi.msg.AllowAppendToExisting {
			switch msg.Mime {
			case cos.ExtTar:
				err = wi.openTarForAppend()
			default:
				err = fmt.Errorf("unsupported archive type %s, only %s is supported", msg.Mime, cos.ExtTar)
			}
		} else {
			err = fmt.Errorf("%s: not allowed to append to an existing %s", r.p.T.Snode(), msg.FullName())
		}
		if err != nil {
			return
		}
		wi.buf, _ = r.p.T.PageMM().Alloc()
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
	r.wiCnt.Inc()
	r.pending.Unlock()
	return
}

func (r *XactCreateArchMultiObj) Do(msg *cmn.ArchiveMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactCreateArchMultiObj) Run(wg *sync.WaitGroup) {
	var err error
	glog.Infoln(r.Name())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			r.pending.RLock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			if !ok {
				debug.Assert(!r.err.IsNil()) // see cleanup
				goto fin
			}
			var (
				smap    = r.p.T.Sowner().Get()
				lrit    = &lriterator{}
				freeLOM = false // not delegating to iterator
			)
			lrit.init(r, r.p.T, &msg.ListRangeMsg, freeLOM)
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
			if err == nil {
				err = r.AbortErr()
			}
			if err != nil {
				wi.abortAppend(err)
				goto fin
			}
			if r.p.T.Snode().ID() == wi.tsi.ID() {
				wi.finalizing.Store(true)
				go r.finalize(wi) // NOTE async
			} else {
				r.eoi(wi.msg.TxnUUID, wi.tsi)
				r.pending.Lock()
				delete(r.pending.m, msg.TxnUUID)
				r.wiCnt.Dec()
				r.pending.Unlock()
				r.DecPending()
			}
		case <-r.IdleTimer():
			goto fin
		case errCause := <-r.ChanAbort():
			if err == nil {
				err = errCause
			}
			goto fin
		}
	}
fin:
	err = r.streamingX.fin(err)
	if err != nil {
		// cleanup: close and rm unfinished archives (NOTE: see finalize)
		r.pending.Lock()
		for uuid, wi := range r.pending.m {
			if wi.finalizing.Load() {
				continue
			}
			if wi.fh != nil && wi.appendPos == 0 {
				cos.Close(wi.fh)
				cos.RemoveFile(wi.fqn)
			}
			delete(r.pending.m, uuid)
		}
		r.pending.Unlock()
	}
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
	r.p.dm.Send(o, fh, wi.tsi)
}

func (r *XactCreateArchMultiObj) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	r.IncPending()
	defer func() {
		r.DecPending()
		transport.DrainAndFreeReader(objReader)
	}()
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return err
	}

	txnUUID := string(hdr.Opaque)
	r.pending.RLock()
	wi, ok := r.pending.m[txnUUID]
	r.pending.RUnlock()
	if !ok {
		debug.Assert(!r.err.IsNil()) // see cleanup
		return r.err.Err()
	}
	debug.Assert(wi.tsi.ID() == r.p.T.Snode().ID() && wi.msg.TxnUUID == txnUUID)

	// NOTE: best-effort via ref-counting
	if hdr.Opcode == OpcTxnDone {
		refc := wi.refc.Dec()
		debug.Assert(refc >= 0)
		return nil
	}
	debug.Assert(hdr.Opcode == 0)
	wi.writer.write(wi.nameInArch(hdr.ObjName), &hdr.ObjAttrs, objReader)
	return nil
}

func (r *XactCreateArchMultiObj) finalize(wi *archwi) {
	if q := wi.quiesce(); q == cluster.QuiAborted {
		r.raiseErr(cmn.NewErrAborted(r.Name(), "", nil), 0, wi.msg.ContinueOnError)
	} else if q == cluster.QuiTimeout {
		r.raiseErr(fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout), 0, wi.msg.ContinueOnError)
	}

	r.pending.Lock()
	delete(r.pending.m, wi.msg.TxnUUID)
	r.wiCnt.Dec()
	r.pending.Unlock()

	errCode, err := r.fini(wi)
	r.DecPending()

	if err != nil {
		wi.abortAppend(err)
		r.raiseErr(err, errCode, wi.msg.ContinueOnError)
	}
}

func (r *XactCreateArchMultiObj) fini(wi *archwi) (errCode int, err error) {
	var size int64

	wi.writer.fini()
	r.p.T.PageMM().Free(wi.buf)

	if size, err = wi.finalize(); err != nil {
		return http.StatusInternalServerError, err
	}

	wi.lom.SetSize(size)
	wi.lom.SetAtimeUnix(time.Now().UnixNano())
	cos.Close(wi.fh)

	errCode, err = r.p.T.FinalizeObj(wi.lom, wi.fqn)
	cluster.FreeLOM(wi.lom)

	r.ObjsAdd(1, size-wi.appendPos)
	return
}

////////////
// archwi //
////////////
func (wi *archwi) do(lom *cluster.LOM, lrit *lriterator) {
	var (
		t       = lrit.t
		coldGet bool
	)
	debug.Assert(t == wi.r.p.T)
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
		if errCode, err := t.GetCold(lrit.ctx, lom, cmn.OwtGetLock); err != nil {
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
	return wi.r.Quiesce(cmn.Timeout.MaxKeepalive(), func(total time.Duration) cluster.QuiRes {
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
			wi.abortAppend(err)
		}
	}
	return err
}

func (wi *archwi) abortAppend(err error) {
	if wi.appendPos == 0 || wi.fh == nil {
		return
	}
	cos.Close(wi.fh)
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
