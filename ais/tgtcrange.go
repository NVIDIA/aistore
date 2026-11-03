// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	// increase if requested or justified by concurrent-read measurements
	coldRangeMaxWorkers = 4
	coldRangeMinSize    = 16 * cos.MiB // amortize setup and additional backend requests
)

// per-target cap on extra range readers (all cold GETs): available mountpaths times a per-media multiplier
// - current heuristic; HDD and NVMe are an order of magnitude apart; SSD counts as NVMe
const (
	coldRangeMultHDD  = 2  // rotational or slow (FUSE, overlay, AUFS)
	coldRangeMultNVMe = 16 // NVMe and SSD
)

type (
	coldRangeGet struct {
		goi       *getOI
		bp        core.Backend
		manifest  *core.Ufest
		ctx       context.Context
		cancel    context.CancelFunc
		stopFirst context.CancelFunc
		pin       core.ObjPin
		next      atomic.Int64
		once      sync.Once
		wg        sync.WaitGroup
		err       error
		size      int64
		chunkSize int64
		code      int
		checksum  bool
		fsync     bool
	}
	coldRangeWorker struct {
		r     *coldRangeGet
		fh    *os.File
		cksum cos.CksumHash
	}
	// target-wide; buckets with rate_limit.backend use the configured limiter
	// TODO: consider per-bucket granularity
	coldRange struct {
		backoffUntil atomic.Int64 // mono time: use sequential cold GETs until then
		inflight     atomic.Int32 // extra range readers, target-wide (see coldRangeMult*)
	}
)

// record remote 429/503 and return true
func (s *coldRange) throttle(bprops *cmn.Bprops, config *cmn.Config, code int, err error) bool {
	if bprops.RateLimit.Backend.Enabled {
		return false
	}
	if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable && !cmn.IsErrTooManyRequests(err) {
		return false
	}
	until := mono.NanoTime() + int64(config.Timeout.ColdRangeBackoff())
	for prev := s.backoffUntil.Load(); until > prev; prev = s.backoffUntil.Load() {
		if s.backoffUntil.CompareAndSwap(prev, until) {
			break
		}
	}
	return true
}

func (s *coldRange) backingOff(bprops *cmn.Bprops) bool {
	until := s.backoffUntil.Load()
	return until != 0 && !bprops.RateLimit.Backend.Enabled && mono.NanoTime() < until
}

// reserve up to `want` extra readers; return zero to cold GET sequentially
func (s *coldRange) reserve(want int, limit int32) int {
	for {
		cur := s.inflight.Load()
		n := min(int32(want), limit-cur)
		if n <= 0 {
			return 0
		}
		if s.inflight.CompareAndSwap(cur, cur+n) {
			return int(n)
		}
	}
}

func (s *coldRange) release(n int) { s.inflight.Add(-int32(n)) }

// initial full read
// - when parallel and qualified (chunked bucket, etc.), make it cancelable
// - non-nil stop: the caller must call it
func (goi *getOI) coldReader(bp core.Backend, parallel bool) (res core.GetReaderResult, stop context.CancelFunc) {
	ctx := goi.ctx
	// latest-version comparison retains the configured whole-object checksum
	if parallel && goi.lom.Bprops().Chunks.AutoEnabled() && !goi.latestVer && !goi.lom.ValidateColdGet() &&
		!goi.lom.ECEnabled() && bp.Provider() != apc.OCI && !goi.t.crange.backingOff(goi.lom.Bprops()) {
		ctx, stop = context.WithCancel(ctx)
	}
	res = bp.GetObjReader(ctx, goi.lom, 0, 0)
	return res, stop
}

// check before allocating workers; false leaves the initial reader untouched
// - retain goi's write lock through completion, then downgrade for local transmission
func (goi *getOI) coldRangePut(bp core.Backend, first *core.GetReaderResult, stopFirst context.CancelFunc) (bool, int, error) {
	if first.Size < coldRangeMinSize || first.Pin == (core.ObjPin{}) {
		return false, 0, nil
	}
	chunkSize := goi.lom.Bprops().Chunks.ChunkSizeFor(first.Size)
	if chunkSize <= 0 || first.Size <= chunkSize {
		return false, 0, nil
	}
	nchunks := cos.DivCeil(first.Size, chunkSize)
	if nchunks > core.MaxChunkCount {
		return false, 0, nil
	}
	// the initial GET may have been waiting for headers when another request was throttled
	if goi.t.crange.backingOff(goi.lom.Bprops()) {
		return false, 0, nil
	}
	var adv load.Advice
	flags := uint64(load.FlMem | load.FlCla | load.FlGor)
	if !cmn.Rom.TestingEnv() { // emulated mountpaths may have no disk-utilization sample
		flags |= load.FlDsk
	}
	adv.Init(flags, &load.Extra{Cfg: cmn.GCO.Get().Disk, RW: true})
	if adv.Load >= load.High {
		return false, 0, nil
	}
	// the caller reads chunk zero; others are extra, subject to the per-target cap
	mult := int32(coldRangeMultNVMe)
	if fs.IsRotational() || fs.IsSlow() {
		mult = coldRangeMultHDD
	}
	nextra := goi.t.crange.reserve(int(min(nchunks, coldRangeMaxWorkers))-1, int32(fs.NumAvail())*mult)
	if nextra == 0 {
		return false, 0, nil
	}
	defer goi.t.crange.release(nextra)

	manifest, err := core.NewUfest("", goi.lom, false /*must-exist*/)
	if err != nil {
		stopFirst()
		cos.Close(first.R)
		return true, http.StatusInternalServerError, err
	}
	r := &coldRangeGet{
		goi: goi, bp: bp, manifest: manifest, stopFirst: stopFirst, pin: first.Pin,
		size: first.Size, chunkSize: chunkSize,
		checksum: goi.lom.CksumType() != cos.ChecksumNone,
		fsync:    goi.lom.IsFeatureSet(feat.FsyncPUT),
	}
	r.ctx, r.cancel = context.WithCancel(goi.ctx)
	defer r.cancel()
	r.next.Store(chunkSize)

	// the caller handles chunk zero while the other readers run concurrently
	r.wg.Add(nextra)
	for range nextra {
		go func() {
			defer r.wg.Done()
			w := coldRangeWorker{r: r}
			w.run(nil)
		}()
	}
	w := coldRangeWorker{r: r}
	w.run(first)
	r.wg.Wait()
	if r.err != nil {
		manifest.Abort(goi.lom)
		return true, r.code, r.err
	}

	// combine only: no payload reread and no arrival-order bookkeeping
	var cksum *cos.Cksum
	if manifest.Size() != r.size || manifest.Count() != int(nchunks) {
		err = fmt.Errorf("%s: incomplete range fill (%d/%d bytes)", goi.lom.Cname(), manifest.Size(), r.size)
	} else if r.checksum {
		cksum, err = manifest.CombineCRC32C()
	}
	if err != nil {
		manifest.Abort(goi.lom)
		return true, http.StatusInternalServerError, err
	}
	goi.lom.SetCksum(cksum)
	if err = goi.lom.CompleteUfest(manifest, true /*locked*/); err != nil {
		return true, http.StatusInternalServerError, err
	}
	if err = goi.lom.Load(true /*cache it*/, true /*locked*/); err != nil {
		return true, http.StatusInternalServerError, err
	}
	goi.lom.DowngradeLock()
	return true, 0, nil
}

func (r *coldRangeGet) fail(code int, err error) {
	r.once.Do(func() {
		r.code, r.err = code, err
		r.cancel()
		r.stopFirst()
	})
}

// reuse one copy buffer and hash state per reader; obtain offsets without a work queue
func (w *coldRangeWorker) run(first *core.GetReaderResult) {
	r := w.r
	buf, slab := r.goi.t.gmm.AllocSize(memsys.DefaultBufSize)
	defer slab.Free(buf)
	if r.checksum {
		w.cksum.Init(cos.ChecksumCRC32C)
	}
	if first != nil {
		limited := io.LimitedReader{R: first.R, N: r.chunkSize}
		err := w.writeChunk(0, &limited, r.chunkSize, buf)
		// cancel the unused tail before closing the initial full GET
		r.stopFirst()
		cos.Close(first.R)
		if err != nil {
			r.fail(http.StatusInternalServerError, err)
			return
		}
	}
	for r.ctx.Err() == nil {
		off := r.next.Add(r.chunkSize) - r.chunkSize
		if off >= r.size {
			return
		}
		size := min(r.chunkSize, r.size-off)
		res := r.bp.GetObjReader(r.ctx, r.goi.lom, off, size, &r.pin)
		if res.Err != nil {
			r.fail(res.ErrCode, res.Err)
			if res.R != nil {
				cos.Close(res.R)
			}
			return
		}
		if res.Size != size {
			r.fail(http.StatusBadGateway, fmt.Errorf("%s: range at %d: expected %d bytes, got %d", r.goi.lom.Cname(), off, size, res.Size))
			cos.Close(res.R)
			return
		}
		err := w.writeChunk(off, res.R, size, buf)
		if err != nil {
			r.fail(http.StatusInternalServerError, err)
		}
		cos.Close(res.R)
		if err != nil {
			return
		}
	}
}

// write directly to the manifest's chunk location
func (w *coldRangeWorker) writeChunk(off int64, reader io.Reader, size int64, buf []byte) error {
	r := w.r
	part := off/r.chunkSize + 1
	chunk, err := r.manifest.NewChunk(int(part), r.goi.lom)
	if err != nil {
		return err
	}
	w.fh, err = r.goi.lom.CreatePart(chunk.Path())
	if err != nil {
		return err
	}
	var writer io.Writer = w.fh
	if r.checksum {
		w.cksum.H.Reset()
		writer = w
	}
	written, err := cos.CopyBuffer(writer, reader, buf)
	if err == nil && written != size {
		err = fmt.Errorf("%s: range at %d: expected %d bytes, read %d", r.goi.lom.Cname(), off, size, written)
	}
	if err == nil && r.fsync {
		err = w.fh.Sync()
	}
	if closeErr := w.fh.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		if r.checksum {
			w.cksum.Finalize()
			chunk.SetCksum(cos.NewCksum(cos.ChecksumCRC32C, w.cksum.Value()))
		}
		err = r.manifest.Add(chunk, written, part)
	}
	if err != nil {
		cos.RemoveFile(chunk.Path())
	}
	return err
}

func (w *coldRangeWorker) Write(p []byte) (int, error) {
	n, err := w.fh.Write(p)
	if n > 0 {
		w.cksum.H.Write(p[:n])
	}
	return n, err
}
