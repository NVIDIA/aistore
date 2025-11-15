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

	maxTotalChunksMem = 128 * cos.MiB // max mem per blob worker

	minBlobDlPrefetch = cos.MiB // size threshold for x-prefetch
)

type (
	XactBlobDl struct {
		bp      core.Backend
		workCh  chan *chunkTask
		doneCh  chan *chunkTask
		args    *core.BlobParams
		vlabs   map[string]string
		xlabs   map[string]string
		workers []*worker
		pending map[int64]*chunkTask // map of pending tasks indexed by roff
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
	worker struct {
		parent *XactBlobDl
	}
	chunkTask struct {
		name    string // for logging and debug assertions
		roff    int64
		sgl     *memsys.SGL
		written int64
		err     error
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
// - Initialize worker and chunk manifest
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
	r.workCh = make(chan *chunkTask, r.numWorkers)
	r.doneCh = make(chan *chunkTask, r.numWorkers)

	r.pending = make(map[int64]*chunkTask, r.numWorkers)

	// init and allocate
	r.workers = make([]*worker, r.numWorkers)
	for i := range r.workers {
		r.workers[i] = &worker{
			parent: r,
		}
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
		err  error
		done *chunkTask
		eof  bool
		lom  = r.args.Lom
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
		case done = <-r.doneCh:
			written := done.written
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

			// out-of-order chunks: temporarily store in map and wait for the next sequential chunk
			if done.roff != r.woff {
				debug.Assertf(done.roff > r.woff, "out-of-order chunk's offset should be greater than the current write offset")
				debug.Assertf((done.roff-r.woff)%r.chunkSize == 0, "out-of-order chunk's offset should be a multiple of chunk size")

				debug.Assertf(r.pending[done.roff] == nil, "out-of-order chunk should not be already in the pending map")
				r.pending[done.roff] = done
				continue outer
			}

			// type #1 write: chunk arrived in order
			if err = r.write(done.sgl, written); err != nil {
				goto fin
			}

			r.scheduleNextChunk(done)

			// type #2 write: drain consecutive pending chunks that are now ready
			if err = r.drainPendingChunks(); err != nil {
				goto fin
			}

			if r.woff >= r.fullSize {
				debug.Assertf(r.woff == r.fullSize, "%d > %d", r.woff, r.fullSize)
				goto fin
			}
			if eof && cmn.Rom.V(5, cos.ModXs) {
				nlog.Errorf("%s eof w/pending: woff=%d, next=%d, size=%d", r.Name(), r.woff, r.nextRoff, r.fullSize)
				for roff := range r.pending {
					nlog.Errorf("   roff %d", roff)
				}
			}
		case <-r.ChanAbort():
			err = cmn.ErrXactUserAbort
			goto fin
		}
	}
fin:
	close(r.workCh)

	if err == nil {
		if r.fullSize != r.woff {
			err = fmt.Errorf("%s: exp size %d != %d off", r.Name(), r.fullSize, r.woff)
			debug.AssertNoErr(err)
		} else {
			debug.Assertf(len(r.pending) == 0, "%s: pending tasks should be all drained, got %d", r.Name(), len(r.pending))
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

		// non-abort error: cleanup the current task and abort the xaction
		if done != nil {
			done.cleanup()
		}
		if err != cmn.ErrXactUserAbort {
			if r.manifest != nil {
				r.manifest.Abort(lom)
			}
		}
	}

	r.cleanup()
	r.Finish()
}

func (r *XactBlobDl) Abort(err error) bool {
	if !r.Base.Abort(err) { // already aborted?
		return false
	}
	r.wg.Wait()
	if r.manifest != nil {
		r.manifest.Abort(r.args.Lom)
	}

	return true
}

func (r *XactBlobDl) start() {
	r.wg.Add(len(r.workers))
	for i := range r.workers {
		go r.workers[i].run()

		// Seed initial work items (chunks) to workCh (one per worker)
		tsk := &chunkTask{name: r.Name() + "_chunk_task_" + strconv.Itoa(i), roff: r.nextRoff}
		if r.args.RespWriter != nil {
			tsk.sgl = core.T.PageMM().NewSGL(r.chunkSize)
		}
		r.workCh <- tsk
		r.nextRoff += r.chunkSize
		debug.IncCounter(tsk.name)
	}
}

func (r *XactBlobDl) scheduleNextChunk(tsk *chunkTask) {
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln("scheduling next chunk:", tsk.name, r.nextRoff, r.fullSize)
	}
	if r.nextRoff < r.fullSize {
		r.workCh <- tsk.advance(r.nextRoff)
		r.nextRoff += r.chunkSize
	} else {
		// last chunk, cleanup the task struct
		tsk.cleanup()
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

	// stats
	tstats := core.T.StatsUpdater()
	tstats.AddWith(
		cos.NamedVal64{Name: r.bp.MetricName(stats.GetSize), Value: size, VarLabs: r.xlabs},
	)
	r.ObjsAdd(0, size)

	return nil
}

// drainPendingChunks processes consecutive pending chunks that are now ready to write.
// It continuously checks if the next expected chunk (at r.woff) is available in the
// pending map, and if so, writes it and schedules the next download.
func (r *XactBlobDl) drainPendingChunks() error {
	for {
		next, exists := r.pending[r.woff]
		if !exists {
			break // gap in sequence, wait for more chunks
		}

		delete(r.pending, r.woff)

		if err := r.write(next.sgl, next.written); err != nil {
			return err
		}

		r.scheduleNextChunk(next)
	}
	return nil
}

func (r *XactBlobDl) cleanup() {
	drainTaskCh(r.workCh)
	drainTaskCh(r.doneCh)

	for roff := range r.pending {
		r.pending[roff].cleanup()
	}

	// make sure all chunk tasks are cleaned up
	for i := range r.workers {
		debug.AssertCounterEquals(r.Name()+"_chunk_task_"+strconv.Itoa(i), 0)
	}
}

func (r *XactBlobDl) CtlMsg() string {
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
	snap = r.Base.NewSnap(r)

	// HACK shortcut to support progress bar
	snap.Stats.InBytes = r.fullSize
	return
}

//
// worker
//

// get range reader -> create chunk file -> copy data to chunk file -> add chunk to manifest -> notify completion | report error
func (w *worker) do(tsk *chunkTask, buf []byte) (int, error) {
	var (
		chunkSize  = w.parent.chunkSize
		lom        = w.parent.args.Lom
		manifest   = w.parent.manifest
		respWriter = w.parent.args.RespWriter
	)
	partNum := int(tsk.roff/chunkSize) + 1

	// Get object range reader
	res := core.T.Backend(lom.Bck()).GetObjReader(context.Background(), lom, tsk.roff, chunkSize)
	if res.Err != nil || res.ErrCode == http.StatusRequestedRangeNotSatisfiable || res.R == nil {
		return res.ErrCode, res.Err
	}

	// Create chunk in manifest
	chunk, chunkErr := manifest.NewChunk(partNum, lom)
	if chunkErr != nil {
		return 0, chunkErr
	}

	// Create chunk file
	chunkPath := chunk.Path()
	chunkFh, chunkFhErr := lom.CreatePart(chunkPath)
	if chunkFhErr != nil {
		return 0, chunkFhErr
	}

	// Setup writers: chunk file + SGL (for RespWriter if needed)
	writers := make([]io.Writer, 0, 3)
	writers = append(writers, chunkFh)

	// If RespWriter is set, copy the data to the task's SGL.
	// This SGL will later be stitched together and sequentially written
	// to the RespWriter via the `doneCh` channel (see `XactBlobDl.write()`).
	if respWriter != nil {
		writers = append(writers, tsk.sgl)
	}

	multiWriter := cos.NewWriterMulti(writers...)

	chwritten, cksum, copyErr := cos.CopyAndChecksum(multiWriter, res.R, buf, lom.CksumConf().Type)
	cos.Close(res.R)
	cos.Close(chunkFh)
	if copyErr != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return 0, copyErr
	}

	if cksum != nil {
		chunk.SetCksum(&cksum.Cksum)
	}

	// Add chunk to manifest with size and checksum
	if addErr := manifest.Add(chunk, chwritten, int64(partNum)); addErr != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil && !cos.IsNotExist(nerr) {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return 0, addErr
	}

	debug.Assertf(res.Size == chwritten, "%d != %d (at woff %d)", res.Size, chwritten, tsk.roff)
	tsk.written = chwritten

	return 0, nil
}

func (w *worker) run() {
	var (
		chunkSize      = w.parent.chunkSize
		workCh, doneCh = w.parent.workCh, w.parent.doneCh
		err            error
		ecode          int
	)

	defer w.parent.wg.Done()

	// Allocate buffer for copying
	buf, slab := core.T.PageMM().AllocSize(chunkSize)
	defer slab.Free(buf)

loop:
	for {
		select {
		case tsk, ok := <-workCh:
			if !ok {
				break loop
			}

			ecode, err = w.do(tsk, buf)

			tsk.err, tsk.code = err, ecode
			doneCh <- tsk

			if err != nil {
				break loop
			}
		case <-w.parent.ChanAbort():
			break loop
		}
	}

	// error handling: send failed task back for cleanup
	if err != nil {
		w.parent.AddErr(err, ecode)
	}
}

//
// chunkTask
//

func (ct *chunkTask) advance(nextRoff int64) *chunkTask {
	if ct.sgl != nil {
		ct.sgl.Reset() // re-use the SGL for the next chunk
	}
	ct.written = 0
	ct.roff = nextRoff
	return ct
}

func (ct *chunkTask) cleanup() {
	if ct.sgl != nil {
		ct.sgl.Free()
	}
	debug.DecCounter(ct.name)
}

func drainTaskCh(ch chan *chunkTask) {
	for {
		select {
		case tsk, ok := <-ch:
			if !ok {
				return
			}
			tsk.cleanup()
		default:
			return
		}
	}
}
