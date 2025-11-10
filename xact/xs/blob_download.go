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
	"sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		doneCh  chan chunkDone
		args    *core.BlobParams
		vlabs   map[string]string
		xlabs   map[string]string
		workCh  chan chunkWi
		sgls    []*memsys.SGL
		readers []*chunkDownloader
		xact.Base
		wg       sync.WaitGroup
		nextRoff int64
		woff     int64
		// not necessarily user-provided apc.BlobMsg values
		// in particular, chunk size and num workers might be adjusted based on resources
		chunkSize  int64
		fullSize   int64
		numWorkers int
		uploadID   string
		manifest   *core.Ufest
	}
)

// internal
type (
	chunkDownloader struct {
		parent *XactBlobDl
	}
	chunkWi struct {
		sgl  *memsys.SGL
		roff int64
	}
	chunkDone struct {
		err     error
		sgl     *memsys.SGL
		written int64
		roff    int64
		code    int
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

//	   Main Thread                        Worker Goroutines (N)           Backend
//	(XactBlobDl.Run())                   (chunkDownloader.run())              │
//	   │                                        │                             │
//	   │     chunkWi{sgl, roff} -> workCh       │                             │
//	   ├──────────────────────────────────────> │                             │
//	   │                                        ├─> GetObjReader(roff, size) ─┤
//	   │                                        │                             │
//	   │                                        |<── copy to a chunk file <───┤
//	   │                                        │
//	   │                                        ├── add chunk to manifest
//	   │     doneCh <- chunkDone{sgl, roff}     │
//	   │ <──────────────────────────────────────┤
//	   │                                        │
//	   ├── handle out-of-order (pending queue)  │
//	   │                                        │
//	   ├─── sequential write to RespWriter      │
//	   │       (for streaming GET)              │
//	   │                                        │
//	   │        schedule the next chunk         │
//	   │   chunkWi{sgl, nextRoff} -> workCh     │
//	   │──────────────────────────────────────> │
//	   │                                        │
//	   (loop until woff >= fullSize)
//
// =====================================================================================
// Consumer: `chunkDownloader.run()` - worker goroutines
// Each worker continuously:
// 1. Pull work item from workCh
//   - roff indicates byte range [roff, roff+chunkSize)
//   - SGL is a reusable buffer provided by main thread
//
// 2. Performs HTTP range request to remote backend
// 3. Persist chunk:
//   - Create chunk metadata and chunk file from LOM
//   - Copy data from backend reader to multi-writer: chunk file + checksum (+ SGL if RespWriter set)
//   - Add the chunk to the manifest
//
// 4. Notify completion: Send chunkDone{sgl, written, roff, err} to doneCh
// 5. Repeat: Wait for next work item on workCh
//
// Producer: XactBlobDl - main thread
// ------------------------------------------------------------------------------------------------
// PHASE 1: `blobFactory.Start()` and `XactBlobDl.start()`
// - Initialize readers and chunk manifest
// - Seed initial work items (chunks) to workCh (one per worker)
// ------------------------------------------------------------------------------------------------
// PHASE 2: `XactBlobDl.Run()` - coordination loop
// Main thread continuously:
// 1. Receive chunk completion from doneCh
// 2. Handle out-of-order: If chunk arrived early (roff > woff), add to pending queue
// 3. Sequential write: Copy SGL to RespWriter if streaming, reset SGL, advance woff
// 4. Schedule next chunk: Send workCh <- chunkWi{sgl, nextRoff} (reuse SGL)
// 5. Plug holes: Walk pending queue, write chunks that are now in-order (roff == woff)
// 6. Check completion: If woff >= fullSize, exit loop
// ------------------------------------------------------------------------------------------------
// PHASE 3: `XactBlobDl.Run()` fin label - finalization
// - Close workCh and wait for workers
// - On success: Compute checksum, persist manifest via lom.CompleteUfest(), update stats
// - On error/abort: Clean up via manifest.Abort(), update error stats
// =====================================================================================
func RenewBlobDl(xid string, params *core.BlobParams, oa *cmn.ObjAttrs) xreg.RenewRes {
	debug.Assert(oa != nil)
	var (
		lom = params.Lom
		pre = &XactBlobDl{args: params} // preliminary ("keep filling" below)
	)
	pre.chunkSize = params.Msg.ChunkSize
	pre.numWorkers = params.Msg.NumWorkers
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

func (p *blobFactory) Start() (err error) {
	// reuse the same args-carrying structure and keep initializing
	r := p.pre

	bck := r.args.Lom.Bck()
	r.InitBase(p.Args.UUID, p.Kind(), bck)

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
	r.readers = make([]*chunkDownloader, r.numWorkers)
	r.sgls = make([]*memsys.SGL, r.numWorkers)
	for i := range r.readers {
		r.readers[i] = &chunkDownloader{
			parent: r,
		}
		// TODO -- FIXME: optimze SGL usage and allocation
		// The reader's SGLs are now only required for streaming GETs (when RespWriter is set).
		// For regular cases where only chunk manifest is used, allocating SGLs is not needed,
		r.sgls[i] = mm.NewSGL(cnt*slabSize, slabSize)
	}

	p.xctn = r

	// Generate uploadID and initialize manifest
	lom := r.args.Lom
	r.uploadID = cos.GenUUID()
	r.manifest, err = core.NewUfest(r.uploadID, lom, false /*must-exist*/)
	if err != nil {
		return err
	}

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

func (r *XactBlobDl) Run(wg *sync.WaitGroup) {
	var (
		err     error
		pending []chunkDone
		eof     bool
		lom     = r.args.Lom
	)

	nlog.Infoln(r.String())
	r.start()
	if wg != nil {
		wg.Done() // signal that xaction has started
	}
	now := mono.NanoTime()
outer:
	for {
		select {
		case done := <-r.doneCh:
			sgl, written := done.sgl, done.written
			if done.err != nil {
				err = done.err
				goto fin
			}
			if done.code == http.StatusRequestedRangeNotSatisfiable && r.fullSize > done.roff+written {
				err = fmt.Errorf("%s: premature eof: expected size %d, have %d", r.Name(), r.fullSize, done.roff+written)
				goto fin
			}
			if written > 0 && r.fullSize < done.roff+written {
				err = fmt.Errorf("%s: detected size increase during download: expected %d, have (%d + %d)", r.Name(),
					r.fullSize, done.roff, written)
				goto fin
			}
			eof = r.fullSize <= done.roff+written
			debug.Assert(written > 0 || eof)

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
			if err = r.write(sgl, written); err != nil {
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
	r.SetStopping()
	close(r.workCh)

	if err == nil {
		if r.fullSize != r.woff {
			err = fmt.Errorf("%s: exp size %d != %d off", r.Name(), r.fullSize, r.woff)
			debug.AssertNoErr(err)
		} else {
			if ty := lom.CksumConf().Type; ty != cos.ChecksumNone {
				cksumH := cos.NewCksumHash(ty)
				if err = r.manifest.ComputeWholeChecksum(cksumH); err != nil {
					err = fmt.Errorf("%s: failed to compute whole checksum: %w", r.Name(), err)
					debug.AssertNoErr(err)
				}
				lom.SetCksum(&cksumH.Cksum)
			}
			if err == nil {
				err = lom.CompleteUfest(r.manifest, false /*locked*/)
			}
		}
	}

	r.wg.Wait()
	close(r.doneCh)

	if err == nil {
		// stats
		tstats := core.T.StatsUpdater()
		tstats.IncWith(r.bp.MetricName(stats.GetCount), r.xlabs)
		tstats.AddWith(
			cos.NamedVal64{Name: r.bp.MetricName(stats.GetLatencyTotal), Value: mono.SinceNano(now), VarLabs: r.xlabs},
		)

		debug.Assert(lom.Lsize() == r.woff)
		tstats.IncWith(stats.GetBlobCount, r.vlabs)
		tstats.AddWith(
			cos.NamedVal64{Name: stats.GetBlobSize, Value: lom.Lsize(), VarLabs: r.vlabs},
		)

		r.ObjsAdd(1, 0)
	} else {
		tstats := core.T.StatsUpdater()

		// Increment global GET error count
		tstats.IncWith(stats.ErrGetCount, r.vlabs)
		// Increment blob-download specific GET error count
		tstats.IncWith(stats.ErrGetBlobCount, r.vlabs)

		// Abort manifest: removes all chunks and the manifest itself
		r.manifest.Abort(lom)

		if err != cmn.ErrXactUserAbort {
			r.Abort(err)
		}
	}

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

func (r *XactBlobDl) write(sgl *memsys.SGL, size int64) (err error) {
	r.woff += size

	// Write to RespWriter (streaming GET)
	if r.args.RespWriter != nil {
		written, err := io.Copy(r.args.RespWriter, sgl) // utilizing sgl.ReadFrom
		if err != nil {
			if cmn.Rom.V(4, cos.ModXs) {
				nlog.Errorf("%s: failed to write to RespWriter (woff=%d, next=%d, sgl-size=%d): %v",
					r.Name(), r.woff, r.nextRoff, size, err)
			}
			return err
		}
		debug.Assertf(written == size, "%s: expected written size=%d, got %d (at woff %d)", r.Name(), size, written, r.woff)
	}

	sgl.Reset()

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

		sgl, written := done.sgl, done.written
		pending = pending[:i]
		if err := r.write(sgl, written); err != nil { // type #2 write: remove from pending and append
			return pending, err
		}

		if r.nextRoff < r.fullSize {
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
	if r.args.RespWriter == nil { // not a GET
		core.FreeLOM(r.args.Lom)
	}
}

func (r *XactBlobDl) ctlmsg() string {
	var sb strings.Builder
	sb.Grow(128)

	sb.WriteString(r.args.Lom.Cname())

	sb.WriteString(" chunk-size:")
	sb.WriteString(cos.IEC(r.chunkSize, 0))

	sb.WriteString(", workers:")
	sb.WriteString(strconv.FormatInt(int64(r.numWorkers), 10))

	// progress
	woff := atomic.LoadInt64(&r.woff)
	if woff > 0 && r.fullSize > 0 {
		pct := (woff * 100) / r.fullSize

		sb.WriteString(", downloaded:")
		sb.WriteString(cos.IEC(woff, 1))
		sb.WriteString("/")
		sb.WriteString(cos.IEC(r.fullSize, 1))
		sb.WriteString(" (")
		sb.WriteString(strconv.FormatInt(pct, 10))
		sb.WriteString("%)")
	}

	return sb.String()
}

func (r *XactBlobDl) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.AddBaseSnap(snap)

	snap.SetCtlMsg(r.Name(), r.ctlmsg())

	// HACK shortcut to support progress bar
	snap.Stats.InBytes = r.fullSize
	return
}

//
// chunkDownloader
//

func (reader *chunkDownloader) run() {
	var (
		a         = reader.parent.args
		lom       = a.Lom
		manifest  = reader.parent.manifest
		chunkSize = reader.parent.chunkSize
	)

	// Allocate buffer for copying
	buf, slab := core.T.PageMM().AllocSize(chunkSize)
	defer slab.Free(buf)

	for {
		msg, ok := <-reader.parent.workCh
		if !ok {
			break
		}
		sgl := msg.sgl
		partNum := int(msg.roff/chunkSize) + 1

		res := core.T.Backend(lom.Bck()).GetObjReader(context.Background(), lom, msg.roff, chunkSize)
		if reader.parent.IsAborted() {
			break
		}
		if res.Err != nil {
			reader.parent.doneCh <- chunkDone{res.Err, sgl, 0, msg.roff, res.ErrCode}
			break
		}
		if res.ErrCode == http.StatusRequestedRangeNotSatisfiable {
			debug.Assert(res.Size == 0)
			reader.parent.doneCh <- chunkDone{nil, sgl, 0, msg.roff, http.StatusRequestedRangeNotSatisfiable}
			break
		}

		// Create chunk in manifest
		chunk, err := manifest.NewChunk(partNum, lom)
		if err != nil {
			reader.parent.doneCh <- chunkDone{err, sgl, 0, msg.roff, http.StatusInternalServerError}
			break
		}

		// Create chunk file
		chunkPath := chunk.Path()
		chunkFh, err := lom.CreatePart(chunkPath)
		if err != nil {
			reader.parent.doneCh <- chunkDone{err, sgl, 0, msg.roff, http.StatusInternalServerError}
			break
		}

		// Setup writers: chunk file + SGL (for RespWriter if needed) + checksum
		writers := make([]io.Writer, 0, 3)
		writers = append(writers, chunkFh)

		// Calculate checksum for this chunk
		cksumH := cos.CksumHash{}
		if ty := lom.CksumConf().Type; ty != cos.ChecksumNone {
			cksumH.Init(ty)
			writers = append(writers, cksumH.H)
		}

		// If RespWriter is set, copy the data to the SGLs corresponding to each reader.
		// These SGLs will later be stitched together and sequentially written
		// to the RespWriter via the `doneCh` channel (see `XactBlobDl.write()`).
		if a.RespWriter != nil {
			writers = append(writers, sgl)
		}

		multiWriter := cos.NewWriterMulti(writers...)

		chwritten, err := cos.CopyBuffer(multiWriter, res.R, buf)
		cos.Close(res.R)
		cos.Close(chunkFh)
		if err != nil {
			if nerr := cos.RemoveFile(chunkPath); nerr != nil {
				nlog.Errorln("nested error removing chunk:", nerr)
			}
			reader.parent.doneCh <- chunkDone{err, sgl, 0, msg.roff, res.ErrCode}
			break
		}

		if cksumH.H != nil {
			cksumH.Finalize()
			chunk.SetCksum(&cksumH.Cksum)
		}

		// Add chunk to manifest with size and checksum
		if err := manifest.Add(chunk, chwritten, int64(partNum)); err != nil {
			if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
				nlog.Errorln("nested error removing chunk:", nerr)
			}
			reader.parent.doneCh <- chunkDone{err, sgl, msg.roff, 0, http.StatusInternalServerError}
			break
		}

		debug.Assertf(res.Size == chwritten, "%d != %d (at woff %d)", res.Size, chwritten, msg.roff)
		reader.parent.doneCh <- chunkDone{nil, sgl, chwritten, msg.roff, res.ErrCode}
	}
	reader.parent.wg.Done()
}
