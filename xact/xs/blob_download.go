// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// TODO:
// 1. load, latest-ver, checksum, write, finalize
// 2. track each chunk reader with 'started' timestamp; abort/retry individual chunks; timeout
// 3. validate `expCksum`

// default tunables (can override via apc.BlobMsg)
const (
	dfltChunkSize = 4 * cos.MiB
	minChunkSize  = memsys.DefaultBufSize
	maxChunkSize  = 16 * cos.MiB

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
		config  *cmn.Config
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
		adv        load.Advice // load advisory
	}
)

// internal
type (
	worker struct {
		parent  *XactBlobDl
		adv     load.Advice
		nchunks int64 // per-worker sequential counter for ShouldCheck
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
//   - Create chunk manifest and chunk file from LOM
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
// Abort Flow:
// -----------
// Abort() signals via Base.Abort() (sends to ChanAbort) and returns immediately.
// It does NOT wait for workers - cleanup happens in the main thread:
//
// Worker mode (runWorkers):
//  1. Main thread receives <-ChanAbort(), jumps to cleanup
//  2. close(workCh) signals workers to exit
//  3. wg.Wait() blocks until all workers complete their current do() call
//  4. Returns error -> finalize() -> manifest.Abort() cleans up chunk files
//
// Serial mode (runSerial):
//  1. IsAborted() checked between chunks -> returns error
//  2. finalize() -> manifest.Abort() cleans up chunk files
//
// Note: manifest.Abort() is called only AFTER all I/O has stopped.
//
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

	// Store user preference (or default if unspecified)
	// Actual chunk size will be determined in factory.Start() based on load conditions
	if pre.chunkSize == 0 {
		pre.chunkSize = dfltChunkSize
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

// calcChunkSize determines optimal chunk size based on:
// - memory load level (from load advisory system)
// - user's preference (preferred chunk size if load permits)
// - blob size (can't exceed the full size)
func (r *XactBlobDl) calcChunkSize() {
	memLoad := r.adv.MemLoad()
	// Handle critical and high memory loads first
	if memLoad == load.Critical {
		nlog.Warningf("%s: critical memory load, using minimum chunk size: %s", r.Name(), cos.IEC(minChunkSize, 0))
		r.chunkSize = minChunkSize
	}
	if memLoad == load.High {
		nlog.Warningf("%s: high memory load, using conservative chunk size: %s", r.Name(), cos.IEC(2*minChunkSize, 0))
		r.chunkSize = 2 * minChunkSize
	}

	// Low/Moderate memory load: validate and adjust user preference
	switch {
	case r.chunkSize == 0:
		r.chunkSize = dfltChunkSize
	case r.chunkSize < minChunkSize:
		nlog.Warningf("chunk size", cos.IEC(r.chunkSize, 1), "is below permitted minimum", cos.IEC(minChunkSize, 0))
		r.chunkSize = minChunkSize
	case r.chunkSize > maxChunkSize:
		nlog.Warningf("chunk size", cos.IEC(r.chunkSize, 1), "exceeds permitted maximum", cos.IEC(maxChunkSize, 0))
		r.chunkSize = maxChunkSize
	}
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

	var (
		l = fs.NumAvail()
		n = max(r.numWorkers, l)
	)

	// Admission check: assess load to determine if we can start
	r.config = cmn.GCO.Get()
	r.adv.Init(load.FlMem|load.FlCla|load.FlDsk, &load.Extra{
		Cfg: &r.config.Disk,
		RW:  true, // data I/O operation
	})

	if r.adv.MemLoad() == load.Critical {
		err := fmt.Errorf("%s: rejected due to resource pressure (%s) - not starting", r.Name(), r.adv.String())
		return cmn.NewErrTooManyRequests(err, http.StatusTooManyRequests)
	}
	if r.adv.Sleep > 0 {
		time.Sleep(r.adv.Sleep)
	}

	if r.numWorkers >= nwpDflt {
		numWorkers, err := clampNumWorkers(r.Name(), n, l)
		if err != nil {
			return err
		}
		r.numWorkers = numWorkers
	}
	r.calcChunkSize()

	if r.numWorkers != nwpNone {
		r.workers = make([]*worker, r.numWorkers)
		r.adv.Refresh() // refresh advice one more time before copying to workers
		for i := range r.workers {
			r.workers[i] = r.newWorker()
		}
		// open channels
		r.workCh = make(chan *chunkTask, r.numWorkers)
		r.doneCh = make(chan *chunkTask, r.numWorkers)
		r.pending = make(map[int64]*chunkTask, r.numWorkers)
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
	var sb cos.SB
	sb.Init(len(r.args.Lom.ObjName) + 3*16)
	sb.WriteString("-[")
	sb.WriteString(r.args.Lom.ObjName)
	sb.WriteUint8('-')
	sb.WriteString(strconv.FormatInt(r.fullSize, 10))
	sb.WriteUint8('-')
	sb.WriteString(strconv.FormatInt(r.chunkSize, 10))
	sb.WriteUint8('-')
	sb.WriteString(strconv.Itoa(r.numWorkers))
	sb.WriteUint8(']')
	return r.Base.String() + sb.String()
}

func (r *XactBlobDl) Run(wg *sync.WaitGroup) {
	var (
		err error
		lom = r.args.Lom
		now = mono.NanoTime()
	)

	nlog.Infoln(r.String())
	if wg != nil {
		wg.Done() // signal that xaction has started
	}

	switch r.numWorkers {
	case nwpNone:
		err = r.runSerial()
	default:
		err = r.runWorkers()
	}

	r.finalize(err, lom, now)
}

// runSerial downloads chunks sequentially without using workers.
func (r *XactBlobDl) runSerial() error {
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infof("running serial download: %s, chunk size: %s", r.Name(), cos.IEC(r.chunkSize, 0))
	}
	buf, slab := core.T.PageMM().AllocSize(r.chunkSize)
	defer slab.Free(buf)

	worker := r.newWorker()
	tsk := &chunkTask{name: r.Name() + "_chunk_task", roff: r.nextRoff}
	defer tsk.cleanup()
	if r.args.RespWriter != nil {
		// in the serial case, we use a single chunkTask and a single SGL for the entire download
		// this SGL will be used to write the data to the RespWriter, and it will be freed upon completion
		debug.IncCounter(tsk.name)
		tsk.sgl = core.T.PageMM().NewSGL(r.chunkSize)
	}

	for r.nextRoff < r.fullSize {
		if r.IsAborted() {
			return r.AbortErr()
		}
		ecode, err := worker.do(tsk, buf)
		if err != nil {
			return fmt.Errorf("%s: failed to download chunk at offset %d (code %d): %w", r.Name(), r.nextRoff, ecode, err)
		}
		if ecode == http.StatusRequestedRangeNotSatisfiable && r.fullSize > r.nextRoff {
			return fmt.Errorf("%s: premature eof: expected size %d, have %d", r.Name(), r.fullSize, r.nextRoff)
		}

		if err = r.write(tsk.sgl, tsk.written); err != nil {
			return err
		}
		r.nextRoff += tsk.written
		tsk.advance(r.nextRoff)
	}
	debug.Assertf(r.nextRoff == r.fullSize, "%d != %d", r.nextRoff, r.fullSize)
	return nil
}

// runWorkers downloads chunks concurrently using multiple workers.
// Main thread coordinates: seeds work, receives completions, handles out-of-order chunks.
// Note: All chunk tasks are returned to the main thread through `doneCh` and are cleaned up either after `goto cleanup` or within `scheduleNextChunk`.
func (r *XactBlobDl) runWorkers() error {
	r.startWorkers()

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infof("started workers: %s, chunk size: %s, num workers: %d", r.Name(), cos.IEC(r.chunkSize, 0), r.numWorkers)
	}

	var (
		done *chunkTask
		err  error
	)
	for {
		select {
		case done = <-r.doneCh:
			written := done.written
			if done.err != nil {
				err = done.err
				goto cleanup
			}
			if done.code == http.StatusRequestedRangeNotSatisfiable && r.fullSize > done.roff+written {
				err = fmt.Errorf("%s: premature eof: expected size %d, have %d", r.Name(), r.fullSize, done.roff+written)
				goto cleanup
			}
			if written > 0 && r.fullSize < done.roff+written {
				err = fmt.Errorf("%s: detected size increase during download: expected %d, have (%d + %d)", r.Name(),
					r.fullSize, done.roff, written)
				goto cleanup
			}
			eof := r.fullSize <= done.roff+written
			debug.Assert(written > 0 || eof)

			// out-of-order chunks: temporarily store in map and wait for the next sequential chunk
			if done.roff != r.woff {
				debug.Assertf(done.roff > r.woff, "out-of-order chunk's offset should be greater than the current write offset")
				debug.Assertf((done.roff-r.woff)%r.chunkSize == 0, "out-of-order chunk's offset should be a multiple of chunk size")

				debug.Assertf(r.pending[done.roff] == nil, "out-of-order chunk should not be already in the pending map")
				r.pending[done.roff] = done
				continue
			}

			// type #1 write: chunk arrived in order
			if err = r.write(done.sgl, written); err != nil {
				goto cleanup
			}

			r.scheduleNextChunk(done)

			// type #2 write: drain consecutive pending chunks that are now ready
			if err = r.drainPendingChunks(); err != nil {
				goto cleanup
			}

			if r.woff >= r.fullSize {
				debug.Assertf(r.woff == r.fullSize, "%d > %d", r.woff, r.fullSize)
				goto cleanup
			}
			if eof && cmn.Rom.V(5, cos.ModXs) {
				nlog.Errorf("%s eof w/pending: woff=%d, next=%d, size=%d", r.Name(), r.woff, r.nextRoff, r.fullSize)
				for roff := range r.pending {
					nlog.Errorf("   roff %d", roff)
				}
			}
		case <-r.ChanAbort():
			err = cmn.ErrXactUserAbort
			goto cleanup
		}
	}

cleanup:
	if err != nil && done != nil {
		// runtime error in main thread: cleanup the leftover chunkTask
		done.cleanup()
	}
	close(r.workCh)
	r.wg.Wait()
	close(r.doneCh)

	return err
}

// finalize handles post-download tasks: checksum, stats, cleanup
func (r *XactBlobDl) finalize(err error, lom *core.LOM, startTime int64) {
	if err == nil {
		if r.fullSize != r.woff {
			err = fmt.Errorf("%s: exp size %d != %d off", r.Name(), r.fullSize, r.woff)
			debug.AssertNoErr(err)
		} else {
			debug.Assertf(len(r.pending) == 0, "%s: pending tasks should be all drained, got %d", r.Name(), len(r.pending))

			lom.Lock(true)
			err = r._fini(lom)
			lom.Unlock(true)
		}
	}

	if err == nil {
		// stats
		tstats := core.T.StatsUpdater()
		tstats.IncWith(r.bp.MetricName(stats.GetCount), r.xlabs)
		tstats.AddWith(
			cos.NamedVal64{Name: r.bp.MetricName(stats.GetLatencyTotal), Value: mono.SinceNano(startTime), VarLabs: r.xlabs},
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

		// cleanup the manifest for both user abort and runtime error abort
		if r.manifest != nil {
			r.manifest.Abort(lom)
		}
	}

	r.cleanup()
	r.Finish()
}

func (r *XactBlobDl) _fini(lom *core.LOM) (err error) {
	if ty := lom.CksumConf().Type; ty != cos.ChecksumNone {
		cksumH := cos.NewCksumHash(ty)
		if err = r.manifest.ComputeWholeChecksum(cksumH); err == nil {
			lom.SetCksum(&cksumH.Cksum)
		}
	}
	if err == nil {
		err = lom.CompleteUfest(r.manifest, true /*locked*/)
	}
	return err
}

func (r *XactBlobDl) newWorker() *worker {
	return &worker{parent: r, adv: r.adv} // note: advice is copied by value
}

func (r *XactBlobDl) startWorkers() {
	for i := range r.workers {
		if r.nextRoff >= r.fullSize {
			break
		}
		r.wg.Add(1)
		go r.workers[i].run()

		// Seed initial work items (chunks) to workCh (one per worker)
		tsk := &chunkTask{name: r.Name() + "_chunk_task_" + strconv.Itoa(i), roff: r.nextRoff}
		if r.args.RespWriter != nil {
			tsk.sgl = core.T.PageMM().NewSGL(r.chunkSize)
			debug.IncCounter(tsk.name)
		}
		r.workCh <- tsk
		r.nextRoff += r.chunkSize
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
	debug.AssertCounterEquals(r.Name()+"_chunk_task", 0)
}

func (r *XactBlobDl) CtlMsg() string {
	if r.args == nil || r.args.Lom == nil {
		return ""
	}

	var sb cos.SB
	sb.Init(128)

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

	partNum := tsk.roff/chunkSize + 1

	// Periodic load check using sequential counter
	w.nchunks++
	if w.adv.ShouldCheck(w.nchunks) {
		w.adv.Refresh()
		// Note: the disk load checks utilizations across all mountpaths
		if w.adv.DskLoad() >= load.High {
			debug.Assert(w.adv.Sleep > 0)
			time.Sleep(w.adv.Sleep)
		}
	}

	// Get object range reader
	res := core.T.Backend(lom.Bck()).GetObjReader(context.Background(), lom, tsk.roff, chunkSize)
	if res.Err != nil || res.ErrCode == http.StatusRequestedRangeNotSatisfiable || res.R == nil {
		return res.ErrCode, res.Err
	}

	// Create chunk in manifest
	chunk, chunkErr := manifest.NewChunk(int(partNum), lom)
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
	if addErr := manifest.Add(chunk, chwritten, partNum); addErr != nil {
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
		ct.sgl.Reset() // reuse the SGL for the next chunk
	}
	ct.written = 0
	ct.roff = nextRoff
	return ct
}

func (ct *chunkTask) cleanup() {
	if ct.sgl != nil {
		ct.sgl.Free()
		debug.DecCounter(ct.name)
	}
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
