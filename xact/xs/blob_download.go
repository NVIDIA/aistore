// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// 1. load, latest-ver, checksum, write, finalize
// 2. track each chunk reader with 'started' timestamp; abort/retry individual chunks; timeout
// 3. validate `expCksum`

// default tunables (can override via apc.BlobMsg)
const (
	dfltChunkSize  = 2 * cos.MiB
	minChunkSize   = memsys.DefaultBufSize
	maxChunkSize   = 16 * cos.MiB
	dfltNumWorkers = 4

	maxInitialSizeSGL = 128           // vec length
	maxTotalChunks    = 128 * cos.MiB // max mem per blob downloader
)

type (
	XactBlobDl struct {
		writer   io.Writer
		args     *core.BlobParams
		readers  []*blobReader
		workCh   chan chunkWi
		doneCh   chan chunkDone
		nextRoff int64
		woff     int64
		xact.Base
		sgls  []*memsys.SGL
		cksum cos.CksumHash
		wg    sync.WaitGroup
		// not necessarily equal user-provided apc.BlobMsg values;
		// in particular, chunk size and num workers might be adjusted based on resources
		chunkSize  int64
		fullSize   int64
		numWorkers int
	}
)

// internal
type (
	blobReader struct {
		parent *XactBlobDl
	}
	chunkWi struct {
		sgl  *memsys.SGL
		roff int64
	}
	chunkDone struct {
		err  error
		sgl  *memsys.SGL
		roff int64
		code int
	}
	blobFactory struct {
		xreg.RenewBase
		pre  *XactBlobDl
		xctn *XactBlobDl
	}
)

// interface guard
var (
	_ core.Xact      = (*XactBlobDl)(nil)
	_ xreg.Renewable = (*blobFactory)(nil)
)

// NOTE: to optimize-out additional HEAD request (below), the caller must pass `oa` attrs (just lom is not enough)
func RenewBlobDl(xid string, params *core.BlobParams, oa *cmn.ObjAttrs) xreg.RenewRes {
	var (
		lom = params.Lom
		pre = &XactBlobDl{args: params} // preliminary ("keep filling" below)
	)
	pre.chunkSize = params.Msg.ChunkSize
	pre.numWorkers = params.Msg.NumWorkers
	if oa == nil {
		// backend.HeadObj(), unless already done via prior (e.g. latest-ver or prefetch-threshold) check
		// (in the latter case, oa.Size must be present)
		oah, ecode, err := core.T.Backend(lom.Bck()).HeadObj(context.Background(), lom, nil /*origReq*/)
		if err != nil {
			return xreg.RenewRes{Err: err}
		}
		debug.Assert(ecode == 0)
		oa = oah
	}
	// fill-in custom MD
	lom.SetCustomMD(oa.CustomMD)
	lom.SetVersion(oa.Ver)
	lom.SetAtimeUnix(oa.Atime)
	// and separately:
	debug.Assert(oa.Size > 0)
	pre.fullSize = oa.Size

	if params.Msg.FullSize > 0 && params.Msg.FullSize != pre.fullSize {
		name := xact.Cname(apc.ActBlobDl, xid) + "/" + lom.Cname()
		err := fmt.Errorf("%s: user-specified size %d, have %d", name, params.Msg.FullSize, pre.fullSize)
		return xreg.RenewRes{Err: err}
	}

	// validate, assign defaults (tune-up below)
	if pre.chunkSize == 0 {
		pre.chunkSize = dfltChunkSize
	} else if pre.chunkSize < minChunkSize {
		nlog.Infoln("Warning: chunk size", cos.ToSizeIEC(pre.chunkSize, 1), "is below permitted minimum",
			cos.ToSizeIEC(minChunkSize, 0))
		pre.chunkSize = minChunkSize
	} else if pre.chunkSize > maxChunkSize {
		nlog.Infoln("Warning: chunk size", cos.ToSizeIEC(pre.chunkSize, 1), "exceeds permitted maximum",
			cos.ToSizeIEC(maxChunkSize, 0))
		pre.chunkSize = maxChunkSize
	}
	if pre.numWorkers == 0 {
		pre.numWorkers = dfltNumWorkers
	}
	if int64(pre.numWorkers)*pre.chunkSize > pre.fullSize {
		pre.numWorkers = int((pre.fullSize + pre.chunkSize - 1) / pre.chunkSize)
	}
	if a := cmn.MaxParallelism(); a < pre.numWorkers {
		pre.numWorkers = a
	}
	return xreg.RenewBucketXact(apc.ActBlobDl, lom.Bck(), xreg.Args{UUID: xid, Custom: pre})
}

//
// blobFactory
//

func (*blobFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	debug.Assert(bck.IsRemote())
	p := &blobFactory{
		RenewBase: xreg.RenewBase{Args: args, Bck: bck},
		pre:       args.Custom.(*XactBlobDl),
	}
	return p
}

func (p *blobFactory) Start() error {
	// reuse the same args-carrying structure and keep filling-in
	r := p.pre
	r.InitBase(p.Args.UUID, p.Kind(), r.args.Lom.Bck())

	// 2nd (just in time) tune-up
	var (
		mm       = core.T.PageMM()
		slabSize = int64(memsys.MaxPageSlabSize)
		pressure = mm.Pressure()
	)
	if pressure >= memsys.PressureExtreme {
		return errors.New(r.Name() + ": extreme memory pressure - not starting")
	}
	switch pressure {
	case memsys.PressureHigh:
		slabSize = memsys.DefaultBufSize
		r.numWorkers = 1
		nlog.Warningln(r.Name() + ": high memory pressure detected...")
	case memsys.PressureModerate:
		slabSize >>= 1
		r.numWorkers = min(3, r.numWorkers)
	}

	cnt := max((r.chunkSize+slabSize-1)/slabSize, 1)
	r.chunkSize = min(cnt*slabSize, r.fullSize)

	if cnt > maxInitialSizeSGL {
		cnt = maxInitialSizeSGL
	}

	// add a reader, if possible
	nr := int64(r.numWorkers)
	if pressure == memsys.PressureLow && r.numWorkers < cmn.MaxParallelism() &&
		nr < (r.fullSize+r.chunkSize-1)/r.chunkSize &&
		nr*r.chunkSize < maxTotalChunks-r.chunkSize {
		r.numWorkers++
	}

	// open channels
	r.workCh = make(chan chunkWi, r.numWorkers)
	r.doneCh = make(chan chunkDone, r.numWorkers)

	// init and allocate
	r.readers = make([]*blobReader, r.numWorkers)
	r.sgls = make([]*memsys.SGL, r.numWorkers)
	for i := range r.readers {
		r.readers[i] = &blobReader{
			parent: r,
		}
		r.sgls[i] = mm.NewSGL(cnt*slabSize, slabSize)
	}

	p.xctn = r

	// deliver locally for custom processing
	if r.args.WriteSGL != nil {
		return nil
	}

	//
	// otherwise (normally), multi-writer that may also include remote send
	//

	ws := make([]io.Writer, 0, 3)
	if ty := r.args.Lom.CksumConf().Type; ty != cos.ChecksumNone {
		r.cksum.Init(ty)
		ws = append(ws, r.cksum.H)
	}
	ws = append(ws, r.args.Lmfh)
	if r.args.RspW != nil {
		// and transmit concurrently (alternatively,
		// could keep writing locally even after GET client goes away)
		ws = append(ws, r.args.RspW)

		whdr := r.args.RspW.Header()
		whdr.Set(cos.HdrContentLength, strconv.FormatInt(r.fullSize, 10))
		whdr.Set(cos.HdrContentType, cos.ContentBinary)
		if v, ok := r.args.Lom.GetCustomKey(cmn.ETag); ok {
			whdr.Set(cos.HdrETag, v)
		}
	}
	r.writer = cos.NewWriterMulti(ws...)
	return nil
}

func (*blobFactory) Kind() string     { return apc.ActBlobDl }
func (p *blobFactory) Get() core.Xact { return p.xctn }

func (p *blobFactory) WhenPrevIsRunning(prev xreg.Renewable) (xreg.WPR, error) {
	var (
		xprev   = prev.Get().(*XactBlobDl)
		lomPrev = xprev.args.Lom
		xcurr   = p.xctn
		lomCurr = xcurr.args.Lom
	)
	if lomPrev.Bucket().Equal(lomCurr.Bucket()) && lomPrev.ObjName == lomCurr.ObjName {
		return xreg.WprUse, cmn.NewErrXactUsePrev(prev.Get().String())
	}
	return xreg.WprKeepAndStartNew, nil
}

//
// XactBlobDl
//

func (r *XactBlobDl) Name() string { return r.Base.Name() + "/" + r.args.Lom.ObjName }

func (r *XactBlobDl) Run(*sync.WaitGroup) {
	var (
		err     error
		pending []chunkDone
		eof     bool
	)
	nlog.Infoln(r.Name()+": chunk-size", cos.ToSizeIEC(r.chunkSize, 0)+", num-concurrent-readers", r.numWorkers)
	r.start()
outer:
	for {
		select {
		case done := <-r.doneCh:
			sgl, sz := done.sgl, done.sgl.Size()
			if done.code == http.StatusRequestedRangeNotSatisfiable && r.fullSize > done.roff+sz {
				err = fmt.Errorf("%s: premature eof: expected size %d, have %d", r.Name(), r.fullSize, done.roff+sz)
				goto fin
			}
			if sz > 0 && r.fullSize < done.roff+sz {
				err = fmt.Errorf("%s: detected size increase during download: expected %d, have (%d + %d)", r.Name(),
					r.fullSize, done.roff, sz)
				goto fin
			}
			eof = r.fullSize <= done.roff+sz
			debug.Assert(sz > 0 || eof)

			// add pending in the offset-descending order
			if done.roff != r.woff {
				debug.Assert(done.roff > r.woff)
				debug.Assert((done.roff-r.woff)%r.chunkSize == 0)
				pending = append(pending, chunkDone{roff: -1})
				for i := range pending {
					if i == len(pending)-1 || (pending[i].roff >= 0 && pending[i].roff < done.roff) {
						copy(pending[i+1:], pending[i:])
						pending[i] = done
						continue outer
					}
				}
			}
			// type1 write
			if err = r.write(sgl); err != nil {
				goto fin
			}

			if r.nextRoff < r.fullSize {
				debug.Assert(sgl.Size() == 0)
				r.workCh <- chunkWi{sgl, r.nextRoff}
				r.nextRoff += r.chunkSize
			}

			// walk backwards and plug any holes
			for i := len(pending) - 1; i >= 0; i-- {
				done := pending[i]
				if done.roff > r.woff {
					break
				}
				debug.Assert(done.roff == r.woff)

				// type2 write: remove from pending and append
				sgl := done.sgl
				pending = pending[:i]
				if err = r.write(sgl); err != nil {
					goto fin
				}
				if r.nextRoff < r.fullSize {
					debug.Assert(sgl.Size() == 0)
					r.workCh <- chunkWi{sgl, r.nextRoff}
					r.nextRoff += r.chunkSize
				}
			}
			if r.woff >= r.fullSize {
				debug.Assertf(r.woff == r.fullSize, "%d > %d", r.woff, r.fullSize)
				goto fin
			}
			if eof && cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Errorf("%s eof w/pending: woff=%d, next=%d, size=%d", r.Name(), r.woff, r.nextRoff, r.fullSize)
				for i := len(pending) - 1; i >= 0; i-- {
					nlog.Errorf("   roff %d", pending[i].roff)
				}
			}
		case <-r.ChanAbort():
			err = cmn.ErrXactUserAbort
			goto fin
		}
	}
fin:
	close(r.workCh)

	if r.args.WriteSGL != nil {
		errN := r.args.WriteSGL(nil)
		debug.AssertNoErr(errN)
	} else {
		// finalize r.args.Lom
		if err == nil && r.args.Lom.IsFeatureSet(feat.FsyncPUT) {
			err = r.args.Lmfh.Sync()
		}
		cos.Close(r.args.Lmfh)

		if err == nil {
			if r.fullSize != r.woff {
				err = fmt.Errorf("%s: exp size %d != %d off", r.Name(), r.fullSize, r.woff)
				debug.AssertNoErr(err)
			} else {
				r.args.Lom.SetSize(r.woff)
				if r.cksum.H != nil {
					r.cksum.Finalize()
					r.args.Lom.SetCksum(r.cksum.Clone())
				}
				_, err = core.T.FinalizeObj(r.args.Lom, r.args.Wfqn, r, cmn.OwtGetPrefetchLock)
			}
		}
		if err == nil {
			r.ObjsAdd(1, 0)
		} else {
			if errRemove := cos.RemoveFile(r.args.Wfqn); errRemove != nil && !os.IsNotExist(errRemove) {
				nlog.Errorln("nested err:", errRemove)
			}
			if err != cmn.ErrXactUserAbort {
				r.Abort(err)
			}
		}
	}

	r.wg.Wait()
	close(r.doneCh)
	r.cleanup()
	r.Finish()
}

func (r *XactBlobDl) start() {
	r.wg.Add(len(r.readers))
	for i := range r.readers {
		go r.readers[i].run()
	}
	for i := range r.readers {
		r.workCh <- chunkWi{r.sgls[i], r.nextRoff}
		r.nextRoff += r.chunkSize
	}
}

func (r *XactBlobDl) write(sgl *memsys.SGL) (err error) {
	var (
		written int64
		size    = sgl.Size()
	)
	if r.args.WriteSGL != nil {
		err = r.args.WriteSGL(sgl)
		written = sgl.Size() - sgl.Len()
	} else {
		written, err = io.Copy(r.writer, sgl) // using sgl.ReadFrom
	}
	if err != nil {
		if cmn.Rom.FastV(4, cos.SmoduleXs) {
			nlog.Errorf("%s: failed to write (woff=%d, next=%d, sgl-size=%d): %v",
				r.Name(), r.woff, r.nextRoff, size, err)
		}
		return err
	}
	debug.Assertf(written == size, "%s: expected written size=%d, got %d (at woff %d)", r.Name(), size, written, r.woff)

	r.woff += size
	r.ObjsAdd(0, size)
	sgl.Reset()
	return nil
}

func (r *XactBlobDl) cleanup() {
	for i := range r.readers {
		r.sgls[i].Free()
	}
	clear(r.sgls)
	if r.args.RspW == nil { // not a GET
		core.FreeLOM(r.args.Lom)
	}
}

//
// blobReader
//

func (reader *blobReader) run() {
	var (
		err       error
		written   int64
		a         = reader.parent.args
		chunkSize = reader.parent.chunkSize
		ctx       = context.Background()
	)
	for {
		msg, ok := <-reader.parent.workCh
		if !ok {
			break
		}
		sgl := msg.sgl
		res := core.T.Backend(a.Lom.Bck()).GetObjReader(ctx, a.Lom, msg.roff, chunkSize)
		if reader.parent.IsAborted() {
			break
		}
		if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
			debug.Assert(res.Size == 0)
			reader.parent.doneCh <- chunkDone{nil, sgl, msg.roff, http.StatusRequestedRangeNotSatisfiable}
			break
		}
		if err = res.Err; err == nil {
			written, err = io.Copy(sgl, res.R)
		}
		if err != nil {
			reader.parent.doneCh <- chunkDone{err, sgl, msg.roff, res.ErrCode}
			break
		}
		debug.Assert(res.Size == written, res.Size, " ", written)
		debug.Assert(sgl.Size() == written, sgl.Size(), " ", written)
		debug.Assert(sgl.Size() == sgl.Len(), sgl.Size(), " ", sgl.Len())

		reader.parent.doneCh <- chunkDone{nil, sgl, msg.roff, res.ErrCode}
	}
	reader.parent.wg.Done()
}

func (r *XactBlobDl) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	// HACK shortcut to support progress bar
	snap.Stats.InBytes = r.fullSize
	return
}
