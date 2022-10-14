// Package xs contains most of the supported eXtended actions (xactions) with some
// exceptions that include certain storage services (mirror, EC) and extensions (downloader, lru).
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/vmihailenco/msgpack"
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
		buf    []byte
		slab   *memsys.Slab
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
	sglShard      map[string]*memsys.SGL
	msgpackWriter struct {
		baseW
		shard sglShard
	}
	archwi struct { // archival work item; implements lrwi
		writer    archWriter
		err       error
		r         *XactArch
		msg       *cmn.ArchiveMsg
		tsi       *cluster.Snode
		lom       *cluster.LOM // of the archive
		fqn       string       // workFQN --/--
		fh        *os.File     // --/--
		cksum     cos.CksumHashSize
		appendPos int64 // append to existing archive
		wmu       sync.Mutex
		errCnt    atomic.Int32
		// finishing
		refc       atomic.Int32
		finalizing atomic.Bool
	}
	XactArch struct {
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
	_ cluster.Xact   = (*XactArch)(nil)
	_ xreg.Renewable = (*archFactory)(nil)
	_ lrwi           = (*archwi)(nil)

	_ archWriter = (*tarWriter)(nil)
	_ archWriter = (*tgzWriter)(nil)
	_ archWriter = (*zipWriter)(nil)
	_ archWriter = (*msgpackWriter)(nil)
)

/////////////////
// archFactory //
/////////////////

func (*archFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	p := &archFactory{streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: apc.ActArchive}}
	return p
}

func (p *archFactory) Start() error {
	workCh := make(chan *cmn.ArchiveMsg, maxNumInParallel)
	r := &XactArch{streamingX: streamingX{p: &p.streamingF}, bckFrom: p.Bck, workCh: workCh, config: cmn.GCO.Get()}
	r.pending.m = make(map[string]*archwi, maxNumInParallel)
	p.xctn = r
	r.DemandBase.Init(p.UUID(), apc.ActArchive, p.Bck, 0 /*use default*/)

	bmd := p.Args.T.Bowner().Get()
	trname := fmt.Sprintf("arch-%s-%s-%d", p.Bck.Provider, p.Bck.Name, bmd.Version) // NOTE: (bmd.Version)
	if err := p.newDM(trname, r.recv, 0 /*pdu*/); err != nil {
		return err
	}
	r.p.dm.SetXact(r)
	r.p.dm.Open()

	xact.GoRunW(r)
	return nil
}

//////////////
// XactArch //
//////////////

func (r *XactArch) Begin(msg *cmn.ArchiveMsg) (err error) {
	lom := cluster.AllocLOM(msg.ArchName)
	if err = lom.InitBck(&msg.ToBck); err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}
	debug.Assert(lom.FullName() == msg.FullName()) // relying on it

	wi := &archwi{r: r, msg: msg, lom: lom}
	wi.fqn = fs.CSM.Gen(wi.lom, fs.WorkfileType, fs.WorkfileCreateArch)
	wi.cksum.Init(lom.CksumType())

	smap := r.p.T.Sowner().Get()
	wi.refc.Store(int32(smap.CountTargets() - 1))
	wi.tsi, err = cluster.HrwTarget(msg.ToBck.MakeUname(msg.ArchName), smap)
	if err != nil {
		r.raiseErr(err, 0, msg.ContinueOnError)
		return
	}

	// NOTE: creating archive at BEGIN time (see cleanup)
	if r.p.T.SID() == wi.tsi.ID() {
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
			err = fmt.Errorf("%s: not allowed to append to an existing %s", r.p.T, msg.FullName())
		}
		if err != nil {
			return
		}
		// construct format-specific writer
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
		case cos.ExtMsgpack:
			mpw := &msgpackWriter{}
			mpw.init(wi)
		default:
			debug.Assert(false, msg.Mime)
			return
		}
	}
	r.pending.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.Unlock()
	return
}

func (r *XactArch) Do(msg *cmn.ArchiveMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactArch) Run(wg *sync.WaitGroup) {
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
			lrit.init(r, r.p.T, &msg.SelectObjsMsg, freeLOM)
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
			if r.p.T.SID() == wi.tsi.ID() {
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
	if r.streamingX.fin(err) == nil {
		return
	}

	// [cleanup] close and rm unfinished archives (compare w/ `finalize`)
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

func (r *XactArch) doSend(lom *cluster.LOM, wi *archwi, fh cos.ReadOpenCloser) {
	o := transport.AllocSend()
	hdr := &o.Hdr
	{
		hdr.Bck = wi.msg.ToBck
		hdr.ObjName = lom.ObjName
		hdr.ObjAttrs.CopyFrom(lom.ObjAttrs())
		hdr.Opaque = []byte(wi.msg.TxnUUID)
	}
	o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ any, _ error) {
		cluster.FreeLOM(lom)
	}
	r.p.dm.Send(o, fh, wi.tsi)
}

func (r *XactArch) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
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
	debug.Assert(wi.tsi.ID() == r.p.T.SID() && wi.msg.TxnUUID == txnUUID)

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

func (r *XactArch) finalize(wi *archwi) {
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

func (r *XactArch) fini(wi *archwi) (errCode int, err error) {
	var size int64
	wi.writer.fini()
	if size, err = wi.finalize(); err != nil {
		return http.StatusInternalServerError, err
	}

	wi.lom.SetSize(size)
	wi.lom.SetAtimeUnix(time.Now().UnixNano())
	cos.Close(wi.fh)

	errCode, err = r.p.T.FinalizeObj(wi.lom, wi.fqn, r)
	cluster.FreeLOM(wi.lom)

	r.ObjsAdd(1, size-wi.appendPos)
	return
}

////////////
// archwi //
////////////

func (wi *archwi) do(lom *cluster.LOM, lrit *lriterator) {
	var coldGet bool
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
	t := lrit.t
	if coldGet {
		// cold
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
	if t.SID() != wi.tsi.ID() {
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
	return cos.UnsafeS(buf)
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
	tw.buf, tw.slab = memsys.PageMM().Alloc()
	tw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	tw.tw = tar.NewWriter(tw.wmul)
	wi.writer = tw
}

func (tw *tarWriter) fini() {
	tw.slab.Free(tw.buf)
	tw.tw.Close()
}

func (tw *tarWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) (err error) {
	tarhdr := new(tar.Header)
	tarhdr.Typeflag = tar.TypeReg

	tarhdr.Size = oah.SizeBytes()
	tarhdr.ModTime = time.Unix(0, oah.AtimeUnix())
	tarhdr.Name = fullname

	// one at a time
	tw.archwi.wmu.Lock()
	if err = tw.tw.WriteHeader(tarhdr); err == nil {
		_, err = io.CopyBuffer(tw.tw, reader, tw.buf)
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
	tzw.tw.buf, tzw.tw.slab = memsys.PageMM().Alloc()
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
	zw.buf, zw.slab = memsys.PageMM().Alloc()
	zw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	zw.zw = zip.NewWriter(zw.wmul)
	wi.writer = zw
}

func (zw *zipWriter) fini() {
	zw.slab.Free(zw.buf)
	zw.zw.Close()
}

func (zw *zipWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) error {
	ziphdr := new(zip.FileHeader)

	ziphdr.Name = fullname
	ziphdr.Comment = fullname
	ziphdr.UncompressedSize64 = uint64(oah.SizeBytes())
	ziphdr.Modified = time.Unix(0, oah.AtimeUnix())

	zw.archwi.wmu.Lock()
	zipw, err := zw.zw.CreateHeader(ziphdr)
	if err == nil {
		_, err = io.CopyBuffer(zipw, reader, zw.buf)
	}
	if err != nil {
		zw.archwi.err = err
		zw.archwi.errCnt.Inc()
	}
	zw.archwi.wmu.Unlock()
	return err
}

///////////////////
// msgpackWriter //
///////////////////

const dfltNumPerShard = 32

func (mpw *msgpackWriter) init(wi *archwi) {
	mpw.archwi = wi
	mpw.shard = make(sglShard, dfltNumPerShard)
	mpw.wmul = cos.NewWriterMulti(wi.fh, &wi.cksum)
	wi.writer = mpw
}

func (mpw *msgpackWriter) fini() {
	genShard := make(cmn.GenShard, len(mpw.shard))
	for fullname, sgl := range mpw.shard {
		genShard[fullname] = sgl.Bytes() // NOTE potential heap alloc
	}
	enc := msgpack.NewEncoder(mpw.wmul)
	err := enc.Encode(genShard)
	debug.AssertNoErr(err)

	// free
	for fullname, sgl := range mpw.shard {
		delete(mpw.shard, fullname)
		delete(genShard, fullname)
		sgl.Free()
	}
}

// NOTE: ***************** msgpack formatting limitation *******************
//
// The format we are supporting for msgpack archiving is the most generic, simplified
// to the point where there's no need for any schema-specific code on the client side.
// Inter-operating with existing clients comes at a cost, though:
//  1. `map[string][]byte` (aka `cmn.GenShard`) has no per-file headers where we could
//     store the attributes of packed files.
//  2. `map[string][]byte` certainly does not allow for same-name duplicates - in a given
//     shard the names of packed files must be unique.
func (mpw *msgpackWriter) write(fullname string, oah cmn.ObjAttrsHolder, reader io.Reader) error {
	// allocate max-size slab buffer to increase the chances
	// of _not_ allocating heap via `sgl.Bytes` (above)
	sgl := memsys.PageMM().NewSGL(memsys.MaxPageSlabSize, memsys.MaxPageSlabSize)

	mpw.archwi.wmu.Lock()
	if _, ok := mpw.shard[fullname]; ok {
		// warn and proceed
		glog.Errorf("Warning: no duplicates in a msgpack-formatted shard (%s, %s, %s)",
			mpw.archwi.r, mpw.archwi.lom, fullname)
	}
	mpw.shard[fullname] = sgl
	size, err := io.Copy(sgl, reader)                   // buffer's not needed since SGL is `io.ReaderFrom`
	debug.Assert(err == nil || size == oah.SizeBytes()) // other than this assert, `oah` is unused
	if err != nil {
		mpw.archwi.err = err
		mpw.archwi.errCnt.Inc()
	}
	mpw.archwi.wmu.Unlock()
	return err
}
