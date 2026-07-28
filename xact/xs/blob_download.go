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
// 1. check that caller always checks remote non-existence (target does - via t.blobdl)
// 2. general error handling:
//    - support io-errors (higher severity)
//    - pass http status from backend => finalize
// 3. load, latest-ver, checksum, write, finalize
// 4. track each chunk reader with 'started' timestamp; abort/retry individual chunks; timeout
// 5. validate `expCksum`

// default tunables (can override via apc.BlobMsg)
const (
	dfltChunkSize = 4 * cos.MiB
	minChunkSize  = memsys.DefaultBufSize // 32KiB
	maxChunkSize  = 16 * cos.MiB

	dfltChunkReadTimeout = time.Minute // default for apc.BlobMsg.ChunkReadTimeout

	minBlobDlPrefetch = cos.MiB // size threshold for x-prefetch

	blobMinBytesPerWorker = 256 * cos.MiB // one blob worker should get at least this much object data
	blobMaxWorkers        = 32            // absolute max workers for a single blob-download job
)

type (
	// pairs a metric name with its variable-labels map so the hot path can
	// call .inc()/.add() without re-specifying both at every update site.
	// (replaceable when handle-based stats lands for xactions.)
	bdMetric struct {
		name  string
		vlabs map[string]string
	}
	bdMetrics struct {
		getCnt  bdMetric // per-backend GET count (e.g., aws.get.n)
		getLat  bdMetric // per-backend GET latency
		getSz   bdMetric // per-backend GET size
		blobCnt bdMetric // blob-download count
		blobSz  bdMetric // blob-download size
		errGet  bdMetric // generic GET error
		errBlob bdMetric // blob-download error
	}

	XactBlobDl struct {
		bp       core.Backend
		ctx      context.Context
		cancel   context.CancelFunc
		pending  blobPending      // map[roff => work item]
		args     *core.BlobParams // including the resulting LOM and control message (apc.BlobMsg)
		config   *cmn.Config
		workCh   chan *blobWI
		doneCh   chan *blobWI
		manifest *core.Ufest
		uploadID string
		cname    string
		workers  []*blobWorker
		adv      load.Advice
		xact.Base
		bdm        bdMetrics // resolved once in factory.Start
		wg         sync.WaitGroup
		nextRoff   int64
		fullSize   int64
		chunkSize  int64         // not necessarily user-provided values (chunk size & num workers might be adjusted based on resources)
		timeout    time.Duration // chunk read timeout
		woff       int64
		numWorkers int
	}
)

func (m *bdMetric) inc(tstats cos.StatsUpdater) {
	tstats.IncWith(m.name, m.vlabs)
}

func (m *bdMetric) add(tstats cos.StatsUpdater, val int64) {
	tstats.AddWith(cos.NamedVal64{Name: m.name, Value: val, VarLabs: m.vlabs})
}

// internal
type (
	blobWorker struct {
		parent  *XactBlobDl
		adv     load.Advice
		nchunks int64 // per-worker sequential counter for ShouldCheck
	}
	blobWI struct {
		err     error
		sgl     *memsys.SGL
		name    string // for logging and debug assertions
		roff    int64
		written int64
		code    int
	}
	blobPending map[int64]*blobWI // map[roff => work item]
)

type (
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

// Blob Download Flow =================================================================
//
// Main goroutine coordinates reusable blobWI instances over workCh/doneCh.
// Workers perform backend range reads, write chunk files, optionally fill SGLs
// for streaming, and add completed chunks to the manifest.
//
// Ordering is enforced by the coordinator:
//   - completed chunks with roff > woff are kept in pending;
//   - chunks at roff == woff advance the ordered stream;
//   - SGLs are written to RespWriter sequentially, when streaming;
//   - blobWI/SGL cleanup and reuse are coordinator-owned.
//
// Abort/cancelation:
//   - Abort() marks the xaction aborted and cancels the blob-download context;
//   - range-read contexts derive from that context, so abort interrupts active
//     GetObjReader/body-copy operations;
//   - finalize() runs only after range-read I/O has stopped;
//   - on success it completes the manifest; on error/abort it aborts it.
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

func (p *blobFactory) Start() (err error) {
	// reuse the same args-carrying structure and keep initializing
	r := p.pre

	lom := r.args.Lom
	r.cname = lom.Cname()
	bck := lom.Bck()
	r.InitBase(p.Args.UUID, p.Kind(), bck)

	r.bp = core.T.Backend(bck)

	// resolve (metric, vlabs) pairings once; reuse on the hot path
	vlabs := map[string]string{stats.VlabBucket: bck.Cname("")}
	xlabs := map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: r.Kind()}
	r.bdm = bdMetrics{
		getCnt:  bdMetric{r.bp.MetricName(stats.GetCount), xlabs},
		getLat:  bdMetric{r.bp.MetricName(stats.GetLatencyTotal), xlabs},
		getSz:   bdMetric{r.bp.MetricName(stats.GetSize), xlabs},
		blobCnt: bdMetric{stats.GetBlobCount, vlabs},
		blobSz:  bdMetric{stats.GetBlobSize, vlabs},
		errGet:  bdMetric{stats.ErrGetCount, vlabs},
		errBlob: bdMetric{stats.ErrGetBlobCount, vlabs},
	}

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

	// num-workers parallelism (nwp)
	l := fs.NumAvail()
	numWorkers, err := TuneBlobDlWorkers(r.Name(), r.numWorkers, l, r.fullSize)
	if err != nil {
		return err
	}
	r.numWorkers = numWorkers

	r.setChunkSize()

	// single-part: run inline in the request goroutine (no worker goroutines)
	if r.fullSize <= r.chunkSize {
		r.numWorkers = xact.NwpNone
	}

	// Generate uploadID and initialize manifest
	r.uploadID = cos.GenUUID()
	r.manifest, err = core.NewUfest(r.uploadID, lom, false /*must-exist*/)
	if err != nil {
		return err
	}

	if r.numWorkers != xact.NwpNone {
		r.workers = make([]*blobWorker, r.numWorkers)
		r.adv.Refresh() // refresh advice one more time before copying to workers
		for i := range r.workers {
			r.workers[i] = r.newWorker()
		}
		// open channels
		r.workCh = make(chan *blobWI, r.numWorkers)
		r.doneCh = make(chan *blobWI, r.numWorkers)
		r.pending = make(blobPending, r.numWorkers)
	}

	tout := r.args.Msg.ChunkReadTimeout.D()
	r.timeout = cos.Ternary(tout > 0, tout, dfltChunkReadTimeout)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	p.xctn = r
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

////////////////
// XactBlobDl //
////////////////

func (r *XactBlobDl) Name() string { return r.cname }
func (r *XactBlobDl) Size() int64  { return r.fullSize }

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
	case xact.NwpNone:
		err = r.runSerial()
	default:
		err = r.runWorkers()
	}

	if r.cancel != nil {
		r.cancel()
	}
	r.finalize(err, lom, now)
}

func (r *XactBlobDl) Abort(err error) bool {
	if !r.Base.Abort(err) {
		return false
	}
	if r.cancel != nil {
		r.cancel()
	}
	return true
}

// setChunkSize determines the effective chunk size based on memory pressure
// and user preference.
func (r *XactBlobDl) setChunkSize() {
	switch r.adv.MemLoad() {
	case load.Critical:
		r.chunkSize = minChunkSize
		nlog.Warningf("%s: critical memory load, using minimum chunk size: %s", r.Name(), cos.IEC(r.chunkSize, 0))
	case load.High:
		r.chunkSize = 2 * minChunkSize
		nlog.Warningf("%s: high memory load, reducing chunk size to %s", r.Name(), cos.IEC(r.chunkSize, 0))
	default:
		switch {
		case r.chunkSize == 0:
			r.chunkSize = dfltChunkSize
		case r.chunkSize < minChunkSize:
			nlog.Warningln("chunk size", cos.IEC(r.chunkSize, 1), "is below permitted minimum", cos.IEC(minChunkSize, 0))
			r.chunkSize = minChunkSize
		case r.chunkSize > maxChunkSize:
			nlog.Warningln("chunk size", cos.IEC(r.chunkSize, 1), "exceeds permitted maximum", cos.IEC(maxChunkSize, 0))
			r.chunkSize = maxChunkSize
		}
	}
}

const blwipref = "_wi_"

// runSerial downloads chunks sequentially without using workers.
func (r *XactBlobDl) runSerial() error {
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infof("running serial download: %s, chunk size: %s", r.Name(), cos.IEC(r.chunkSize, 0))
	}
	buf, slab := core.T.PageMM().AllocSize(r.chunkSize)
	defer slab.Free(buf)

	var (
		worker = r.newWorker()
		wi     = &blobWI{name: r.Name() + blwipref, roff: r.nextRoff}
	)
	defer wi.cleanup()
	if r.args.RespWriter != nil {
		// in the serial case, we use a single blobWI and a single SGL for the entire download
		// this SGL will be used to write the data to the RespWriter, and it will be freed upon completion
		debug.IncCounter(wi.name)
		wi.sgl = core.T.PageMM().NewSGL(r.chunkSize)
	}

	for r.nextRoff < r.fullSize {
		if r.IsAborted() {
			return r.AbortErr()
		}
		ecode, err := worker.do(wi, buf)
		if err != nil {
			return fmt.Errorf("%s: failed to download chunk at offset %d (code %d): %w", r.Name(), r.nextRoff, ecode, err)
		}
		if ecode == http.StatusRequestedRangeNotSatisfiable && r.fullSize > r.nextRoff {
			return fmt.Errorf("%s: premature eof: expected size %d, have %d", r.Name(), r.fullSize, r.nextRoff)
		}

		if err = r.write(wi.sgl, wi.written); err != nil {
			return err
		}
		r.nextRoff += wi.written
		wi.advance(r.nextRoff)
	}
	debug.Assertf(r.nextRoff == r.fullSize, "%d != %d", r.nextRoff, r.fullSize)
	return nil
}

// runWorkers downloads chunks concurrently using multiple workers.
// Main thread coordinates: seeds work, receives completions, handles out-of-order chunks.
// Note: All work-items are returned to the main thread through `doneCh` and are cleaned up either after `goto cleanup` or within `scheduleNextChunk`.
func (r *XactBlobDl) runWorkers() error {
	r.startWorkers()

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infof("started workers: %s, chunk size: %s, num workers: %d", r.Name(), cos.IEC(r.chunkSize, 0), r.numWorkers)
	}

	var (
		done *blobWI
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
				debug.Assertf(done.roff > r.woff, "out-of-order chunk's offset should be greater than the current write offset: %d vs %d",
					done.roff, r.woff)
				debug.Assertf((done.roff-r.woff)%r.chunkSize == 0, "out-of-order chunk's offset should be a multiple of chunk size: %d, %d",
					done.roff-r.woff, r.chunkSize)

				debug.Assert(r.pending[done.roff] == nil, "out-of-order chunk should not be already in the pending map")
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
	if err != nil {
		if done != nil {
			done.cleanup()
		}
		if r.cancel != nil {
			r.cancel()
		}
	}

	close(r.workCh)
	r.wg.Wait()
	close(r.doneCh)

	return err
}

// finalize handles post-download work-items: checksum, stats, cleanup
func (r *XactBlobDl) finalize(err error, lom *core.LOM, startTime int64) {
	if err == nil {
		if r.fullSize != r.woff {
			err = fmt.Errorf("%s: exp size %d != %d off", r.Name(), r.fullSize, r.woff)
			debug.AssertNoErr(err)
		} else {
			debug.Assertf(len(r.pending) == 0, "%s: pending work-items should be all drained, got %d", r.Name(), len(r.pending))

			lom.Lock(true)
			err = r._fini(lom)
			lom.Unlock(true)
		}
	}

	if err == nil {
		// stats
		tstats := core.T.StatsUpdater()
		r.bdm.getCnt.inc(tstats)
		r.bdm.getLat.add(tstats, mono.SinceNano(startTime))

		debug.Assert(lom.Lsize() == r.woff)
		r.bdm.blobCnt.inc(tstats)
		r.bdm.blobSz.add(tstats, lom.Lsize())

		r.ObjsAdd(1, 0)
	} else {
		r._errStats()

		// cleanup the manifest for both user abort and runtime error abort
		if r.manifest != nil {
			r.manifest.Abort(lom)
		}
	}

	r.cleanup()
	r.Finish()
}

// bump both generic and blob-downloader's own error counters
// (compare w/ ais/target.go getObject() but note: 404 here implies runtime race)
func (r *XactBlobDl) _errStats() {
	tstats := core.T.StatsUpdater()
	r.bdm.errGet.inc(tstats)
	r.bdm.errBlob.inc(tstats)
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

func (r *XactBlobDl) newWorker() *blobWorker {
	return &blobWorker{parent: r, adv: r.adv} // note: load-advice is copied by value
}

func (r *XactBlobDl) startWorkers() {
	for i := range r.workers {
		if r.nextRoff >= r.fullSize {
			break
		}
		r.wg.Add(1)
		go r.workers[i].run()

		// Seed initial work items (chunks) to workCh (one per worker)
		wi := &blobWI{name: r.Name() + blwipref + strconv.Itoa(i), roff: r.nextRoff}
		if r.args.RespWriter != nil {
			wi.sgl = core.T.PageMM().NewSGL(r.chunkSize)
			debug.IncCounter(wi.name)
		}
		r.workCh <- wi
		r.nextRoff += r.chunkSize
	}
}

func (r *XactBlobDl) scheduleNextChunk(wi *blobWI) {
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln("scheduling next chunk:", wi.name, r.nextRoff, r.fullSize)
	}
	if r.nextRoff < r.fullSize {
		r.workCh <- wi.advance(r.nextRoff)
		r.nextRoff += r.chunkSize
	} else {
		// last chunk, cleanup the work-item struct
		wi.cleanup()
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
	r.bdm.getSz.add(core.T.StatsUpdater(), size)
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
	_drainWich(r.workCh)
	_drainWich(r.doneCh)

	for roff := range r.pending {
		r.pending[roff].cleanup()
	}

	// make sure all chunk tasks are cleaned up
	for i := range r.workers {
		debug.AssertCounterEquals(r.Name()+blwipref+strconv.Itoa(i), 0)
	}
	debug.AssertCounterEquals(r.Name()+blwipref, 0)
}

func _drainWich(ch chan *blobWI) {
	for {
		select {
		case wi, ok := <-ch:
			if !ok {
				return
			}
			wi.cleanup()
		default:
			return
		}
	}
}

// TODO: remove; use CtlMsg() instead
func (r *XactBlobDl) String() string {
	var sb cos.SB
	sb.Init(len(r.cname) + 3*16)
	sb.WriteString("-[")
	sb.WriteString(r.cname)
	sb.WriteUint8('-')
	sb.WriteString(strconv.FormatInt(r.fullSize, 10))
	sb.WriteUint8('-')
	sb.WriteString(strconv.FormatInt(r.chunkSize, 10))
	sb.WriteUint8('-')
	sb.WriteString(strconv.Itoa(r.numWorkers))
	sb.WriteUint8(']')
	return r.Base.String() + sb.String()
}

func (r *XactBlobDl) CtlMsg() string {
	if r.args == nil {
		return ""
	}

	var sb cos.SB
	sb.Init(ctlMsgBufSize)

	sb.WriteString(r.cname)

	debug.Assert(r.args.Parent != "") // (convention)
	if r.args.Parent != "" {
		sb.WriteString(" parent:")
		sb.WriteString(r.args.Parent)
	}

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

////////////////
// blobWorker //
////////////////

func (w *blobWorker) do(wi *blobWI, buf []byte) (int, error) {
	var (
		chunkSize  = w.parent.chunkSize
		lom        = w.parent.args.Lom
		manifest   = w.parent.manifest
		respWriter = w.parent.args.RespWriter
	)

	partNum := wi.roff/chunkSize + 1

	// 1. Throttle under high disk pressure; reject under critical resource pressure.
	if err := w.throttle(); err != nil {
		return http.StatusTooManyRequests, err
	}

	// 2. Get object range reader. The context covers both reader acquisition and response-body copy.
	parentCtx := w.parent.ctx
	debug.Assert(parentCtx != nil)

	ctx, cancel := context.WithTimeout(parentCtx, w.parent.timeout)
	defer cancel()

	res := core.T.Backend(lom.Bck()).GetObjReader(ctx, lom, wi.roff, chunkSize)
	if res.Err != nil {
		if res.R != nil {
			cos.Close(res.R)
		}
		return res.ErrCode, res.Err
	}
	debug.Assert(res.R != nil)

	// 3. Create chunk in manifest
	chunk, chunkErr := manifest.NewChunk(int(partNum), lom)
	if chunkErr != nil {
		cos.Close(res.R)
		return 0, chunkErr
	}

	// 4. Create chunk file
	chunkPath := chunk.Path()
	chunkFh, chunkFhErr := lom.CreatePart(chunkPath)
	if chunkFhErr != nil {
		cos.Close(res.R)
		return 0, chunkFhErr
	}

	// Setup writers: chunk file + SGL (for RespWriter if needed)
	// If RespWriter is set, copy the data to the work-item's SGL.
	// This SGL will later be stitched together and sequentially written
	// to the RespWriter via the `doneCh` channel (see `XactBlobDl.write()`).
	writers := make([]io.Writer, 0, 3)
	writers = append(writers, chunkFh)
	if respWriter != nil {
		writers = append(writers, wi.sgl)
	}

	multiWriter := cos.NewWriterMulti(writers...)

	// 5. Read remote, write local
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

	// 6. Done
	if addErr := manifest.Add(chunk, chwritten, partNum); addErr != nil {
		if nerr := cos.RemoveFile(chunkPath); nerr != nil {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return 0, addErr
	}

	debug.Assertf(res.Size == chwritten, "%d != %d (at woff %d)", res.Size, chwritten, wi.roff)
	wi.written = chwritten

	return 0, nil
}

func (w *blobWorker) throttle() error {
	w.nchunks++
	if !w.adv.ShouldCheck(w.nchunks) {
		return nil
	}

	w.adv.Refresh()

	memLoad := w.adv.MemLoad()
	dskLoad := w.adv.DskLoad()

	if memLoad == load.Critical || (dskLoad == load.Critical && !cmn.Rom.TestingEnv()) {
		err := fmt.Errorf("%s: rejected due to resource pressure (%s)", w.parent.Name(), w.adv.String())
		return cmn.NewErrTooManyRequests(err, http.StatusTooManyRequests)
	}

	// Note: disk load checks utilization across all mountpaths.
	if dskLoad >= load.High {
		debug.Assert(w.adv.Sleep > 0)
		time.Sleep(w.adv.Sleep)
	}
	return nil
}

func (w *blobWorker) run() {
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
		case wi, ok := <-workCh:
			if !ok {
				break loop
			}

			ecode, err = w.do(wi, buf)

			wi.err, wi.code = err, ecode
			doneCh <- wi

			if err != nil {
				break loop
			}
		case <-w.parent.ChanAbort():
			break loop
		}
	}

	// failed work-item has already been returned via doneCh
	if err != nil {
		w.parent.AddErr(err, ecode)
	}
}

////////////
// blobWI //
////////////

func (wi *blobWI) advance(nextRoff int64) *blobWI {
	if wi.sgl != nil {
		wi.sgl.Reset() // reuse the SGL for the next chunk
	}
	wi.written = 0
	wi.roff = nextRoff
	return wi
}

func (wi *blobWI) cleanup() {
	if wi.sgl != nil {
		wi.sgl.Free()
		debug.DecCounter(wi.name)
	}
}

// Effective workers = min(tuned, objCap, blobMaxWorkers), where
//   - tuned  = TuneNumWorkers(requested, mpaths) -- folds in user request, media, system load;
//   - objCap = ceil(fullSize / blobMinBytesPerWorker) -- one worker per ~256MiB of data.
//
// `requested == -1` (NwpNone) and extreme load short-circuit to -1 (serial).
// Examples (auto request, 4 mountpaths):
//   - 64 MiB, SSD : tuned=16, objCap=1  -> 1   (object too small to fan out)
//   - 2 GiB, NVMe : tuned=32, objCap=8  -> 8   (object-size cap binds)
//   - 8 GiB, NVMe : tuned=32, objCap=32 -> 32  (hits hard cap)
//   - 16 GiB, NVMe: tuned=32, objCap=64 -> 32  (clamped by blobMaxWorkers)
func TuneBlobDlWorkers(xname string, requestedWorkers, numMpaths int, fullSize int64) (int, error) {
	tunedWorkers, err := xact.TuneNumWorkers(xname, requestedWorkers, numMpaths)
	if err != nil || tunedWorkers == xact.NwpNone {
		return tunedWorkers, err
	}
	debug.Assert(fullSize > 0)
	workerSizeCap := int(cos.DivCeil(fullSize, blobMinBytesPerWorker))   // ceil(object_size / 256MiB)
	return max(1, min(tunedWorkers, workerSizeCap, blobMaxWorkers)), nil // min(tuned_capacity, object-size cap, hard cap)
}
