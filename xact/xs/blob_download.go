// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// 1. load, latest-ver, checksum, write, finalize
// 2. track each chunk reader with 'started' timestamp; abort/retry individual chunks; timeout
// 3. validate `expCksum`

// default tunables (can override via apc.BlobMsg)
const (
	dfltChunkSize  = 4 * cos.MiB
	minChunkSize   = memsys.DefaultBufSize
	maxChunkSize   = 16 * cos.MiB
	dfltNumWorkers = 4

	maxInitialSizeSGL = 128           // vec length
	maxTotalChunksMem = 128 * cos.MiB // max mem per blob downloader

	minBlobDlPrefetch = cos.MiB // size threshold for x-prefetch
)

type (
	XactBlobDl struct {
		bp      core.Backend
		writer  io.Writer
		doneCh  chan chunkDone
		args    *core.BlobParams
		vlabs   map[string]string
		xlabs   map[string]string
		workCh  chan chunkWi
		cksum   cos.CksumHash
		sgls    []*memsys.SGL
		readers []*blobReader
		xact.Base
		wg       sync.WaitGroup
		nextRoff int64
		woff     int64
		// not necessarily user-provided apc.BlobMsg values
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
		pre  *XactBlobDl
		xctn *XactBlobDl
		xreg.RenewBase
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
		oah, ecode, err := core.T.HeadCold(lom, nil /*origReq*/)
		if err != nil {
			return xreg.RenewRes{Err: err}
		}
		debug.Assert(ecode == 0)
		oa = oah
	}
	// fill-in custom MD
	lom.SetCustomMD(oa.CustomMD)
	lom.CopyVersion(oa)
	lom.SetAtimeUnix(oa.Atime)
	// and separately:
	debug.Assert(oa.Size > 0)
	pre.fullSize = oa.Size

	if params.Msg.FullSize > 0 && params.Msg.FullSize != pre.fullSize {
		name := xact.Cname(apc.ActBlobDl, xid) + "/" + lom.Cname()
		err := fmt.Errorf("%s: user-specified size %d, have %d", name, params.Msg.FullSize, pre.fullSize)
		return xreg.RenewRes{Err: err}
	}

	// validate, assign defaults (further tune-up below)
	switch {
	case pre.chunkSize == 0:
		pre.chunkSize = dfltChunkSize
	case pre.chunkSize < minChunkSize:
		nlog.Infoln("Warning: chunk size", cos.IEC(pre.chunkSize, 1), "is below permitted minimum",
			cos.IEC(minChunkSize, 0))
		pre.chunkSize = minChunkSize
	case pre.chunkSize > maxChunkSize:
		nlog.Infoln("Warning: chunk size", cos.IEC(pre.chunkSize, 1), "exceeds permitted maximum",
			cos.IEC(maxChunkSize, 0))
		pre.chunkSize = maxChunkSize
	}

	// [NOTE] factor in num CPUs and num chunks to tuneup num workers (heuristic)
	if pre.numWorkers == 0 {
		pre.numWorkers = dfltNumWorkers
	}
	if a := sys.MaxParallelism(); a > pre.numWorkers+4 {
		pre.numWorkers++
	}
	if int64(pre.numWorkers)*pre.chunkSize > pre.fullSize {
		pre.numWorkers = int((pre.fullSize + pre.chunkSize - 1) / pre.chunkSize)
	}
	if a := sys.MaxParallelism(); a < pre.numWorkers {
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
	// reuse the same args-carrying structure and keep initializing
	r := p.pre

	bck := r.args.Lom.Bck()
	r.InitBase(p.Args.UUID, p.Kind(), r.args.Lom.Cname(), bck)

	r.bp = core.T.Backend(bck)
	r.vlabs = map[string]string{
		stats.VlabBucket: bck.Cname(""),
	}
	r.xlabs = map[string]string{
		stats.VlabBucket: bck.Cname(""),
		stats.VlabXkind:  r.Kind(),
	}

	// 2nd (just in time) tune-up
	var (
		mm       = core.T.PageMM()
		slabSize = int64(memsys.MaxPageSlabSize)
		pressure = mm.Pressure()
	)
	if pressure >= memsys.PressureExtreme {
		oom.FreeToOS(true)
		if !cmn.Rom.TestingEnv() {
			return errors.New(r.Name() + ": " + memsys.FmtErrExtreme + " - not starting")
		}
	}
	switch pressure {
	case memsys.PressureHigh:
		slabSize = memsys.DefaultBufSize
		r.numWorkers = 1
		nlog.Warningln(r.Name(), "num-workers = 1")
	case memsys.PressureModerate:
		slabSize >>= 1
		r.numWorkers = min(3, r.numWorkers)
	}

	cnt := max((r.chunkSize+slabSize-1)/slabSize, 1)
	r.chunkSize = min(cnt*slabSize, r.fullSize)

	if cnt > maxInitialSizeSGL {
		cnt = maxInitialSizeSGL
	}

	nr := int64(r.numWorkers)
	nc := (r.fullSize + r.chunkSize - 1) / r.chunkSize
	if pressure == memsys.PressureLow && r.numWorkers < sys.MaxParallelism() {
		if nr < nc && nr*r.chunkSize < maxTotalChunksMem {
			r.numWorkers++ // add a reader
		}
		if r.numWorkers == 1 && r.chunkSize > minChunkSize<<1 {
			r.numWorkers = 2
			r.chunkSize >>= 1
		}
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
	ws = append(ws, r.args.Lmfh)
	if ty := r.args.Lom.CksumConf().Type; ty != cos.ChecksumNone {
		r.cksum.Init(ty)
		ws = append(ws, r.cksum.H)
	}
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
		xcurr   = p.pre
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
func (r *XactBlobDl) Size() int64  { return r.fullSize }

func (r *XactBlobDl) String() string {
	var sb strings.Builder
	sb.Grow(len(r.args.Lom.ObjName) + 3*16)
	sb.WriteString("-[")
	sb.WriteString(r.args.Lom.ObjName)
	sb.WriteByte('-')
	sb.WriteString(strconv.FormatInt(r.fullSize, 10))
	sb.WriteByte('-')
	sb.WriteString(strconv.FormatInt(r.chunkSize, 10))
	sb.WriteByte('-')
	sb.WriteString(strconv.Itoa(r.numWorkers))
	sb.WriteByte(']')
	return r.Base.String() + sb.String()
}

func (r *XactBlobDl) Run(*sync.WaitGroup) {
	var (
		err     error
		pending []chunkDone
		eof     bool
	)
	nlog.Infoln(r.String())
	r.start()
	now := mono.NanoTime()
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
			// type #1 write
			if err = r.write(sgl); err != nil {
				goto fin
			}

			if r.nextRoff < r.fullSize {
				debug.Assert(sgl.Size() == 0)
				r.workCh <- chunkWi{sgl, r.nextRoff}
				r.nextRoff += r.chunkSize
			}

			// walk backwards and plug any holes (type #2 write)
			if pending, err = r.plugholes(pending); err != nil {
				goto fin
			}

			if r.woff >= r.fullSize {
				debug.Assertf(r.woff == r.fullSize, "%d > %d", r.woff, r.fullSize)
				goto fin
			}
			if eof && cmn.Rom.V(5, cos.ModXs) {
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
			// stats
			tstats := core.T.StatsUpdater()
			tstats.IncWith(r.bp.MetricName(stats.GetCount), r.xlabs)
			tstats.AddWith(
				cos.NamedVal64{Name: r.bp.MetricName(stats.GetLatencyTotal), Value: mono.SinceNano(now), VarLabs: r.xlabs},
			)

			debug.Assert(r.args.Lom.Lsize() == r.woff)
			tstats.IncWith(stats.GetBlobCount, r.vlabs)
			tstats.AddWith(
				cos.NamedVal64{Name: stats.GetBlobSize, Value: r.args.Lom.Lsize(), VarLabs: r.vlabs},
			)

			r.ObjsAdd(1, 0)
		} else {
			tstats := core.T.StatsUpdater()

			// Increment global GET error count
			tstats.IncWith(stats.ErrGetCount, r.vlabs)
			// Increment blob-download specific GET error count
			tstats.IncWith(stats.ErrGetBlobCount, r.vlabs)

			if errRemove := cos.RemoveFile(r.args.Wfqn); errRemove != nil && !cos.IsNotExist(errRemove) {
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
		err = r.args.WriteSGL(sgl) // custom write
		written = sgl.Size() - sgl.Len()
	} else {
		written, err = io.Copy(r.writer, sgl) // utilizing sgl.ReadFrom
	}

	sgl.Reset()

	if err != nil {
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Errorf("%s: failed to write (woff=%d, next=%d, sgl-size=%d): %v",
				r.Name(), r.woff, r.nextRoff, size, err)
		}
		return err
	}
	debug.Assertf(written == size, "%s: expected written size=%d, got %d (at woff %d)", r.Name(), size, written, r.woff)

	r.woff += size

	// stats
	tstats := core.T.StatsUpdater()
	tstats.AddWith(
		cos.NamedVal64{Name: r.bp.MetricName(stats.GetSize), Value: size, VarLabs: r.xlabs},
	)
	r.ObjsAdd(0, size)

	return nil
}

func (r *XactBlobDl) plugholes(pending []chunkDone) ([]chunkDone, error) {
	for i := len(pending) - 1; i >= 0; i-- {
		done := pending[i]
		if done.roff > r.woff {
			break
		}
		debug.Assert(done.roff == r.woff)

		sgl := done.sgl
		pending = pending[:i]
		if err := r.write(sgl); err != nil { // type #2 write: remove from pending and append
			return pending, err
		}

		if r.nextRoff < r.fullSize {
			debug.Assert(sgl.Size() == 0)
			r.workCh <- chunkWi{sgl, r.nextRoff}
			r.nextRoff += r.chunkSize
		}
	}
	return pending, nil
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
