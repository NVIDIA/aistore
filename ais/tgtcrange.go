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
	"sync"
	"sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"
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
	}
	coldRangeWorker struct {
		r  *coldRangeGet
		cw core.ChunkWriter
	}
)

// initial full read
// - when parallel and qualified (chunked bucket, etc.), make it cancelable
// - non-nil stop: the caller must call it
func (goi *getOI) coldReader(bp core.Backend, parallel bool) (res core.GetReaderResult, stop context.CancelFunc) {
	ctx := goi.ctx
	chunks := &goi.lom.Bprops().Chunks
	if parallel && chunks.AutoEnabled() && chunks.NumColdReaders() > 1 && !goi.lom.ValidateColdGet() &&
		!goi.lom.ECEnabled() && bp.Provider() != apc.OCI && !core.RR.BackingOff(goi.lom.Bprops()) {
		ctx, stop = context.WithCancel(ctx)
	}
	res = bp.GetObjReader(ctx, goi.lom, 0, 0)
	return res, stop
}

// check before allocating workers; false leaves the initial reader untouched
// - retain goi's write lock through completion, then downgrade for local transmission
func (goi *getOI) coldRangePut(bp core.Backend, first *core.GetReaderResult, stopFirst context.CancelFunc) (bool, int, error) {
	if first.Size < core.ColdRangeMinSize || first.Pin == (core.ObjPin{}) {
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
	if core.RR.BackingOff(goi.lom.Bprops()) {
		return false, 0, nil
	}
	var adv load.Advice
	flags := uint64(load.FlMem | load.FlCla | load.FlSch)
	if !cmn.Rom.TestingEnv() { // emulated mountpaths may have no disk-utilization sample
		flags |= load.FlDsk
	}
	adv.Init(flags, &load.Extra{Cfg: cmn.GCO.Get().Disk, RW: true})
	if adv.Load >= load.High {
		return false, 0, nil
	}
	// the caller reads chunk zero; others are extra, subject to the per-target cap (see core.RR)
	nreaders := goi.lom.Bprops().Chunks.NumColdReaders()
	nextra := core.RR.Reserve(int(min(nchunks, int64(nreaders)))-1, core.RR.Limit())
	if nextra == 0 {
		return false, 0, nil
	}
	defer core.RR.Release(nextra)

	manifest, err := core.NewUfest("", goi.lom, false /*must-exist*/)
	if err != nil {
		stopFirst()
		cmn.CloseRemote(first.R, goi.lom, 4, cos.ModAIS)
		return true, http.StatusInternalServerError, err
	}
	r := &coldRangeGet{
		goi: goi, bp: bp, manifest: manifest, stopFirst: stopFirst, pin: first.Pin,
		size: first.Size, chunkSize: chunkSize,
		checksum: goi.lom.CksumType() != cos.ChecksumNone,
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

	// preserve the checksum supplied by the initial GET (same revision as the pin);
	// otherwise, combine per-chunk CRC32C without rereading the payload
	var cksum *cos.Cksum
	if manifest.Size() != r.size || manifest.Count() != int(nchunks) {
		err = fmt.Errorf("%s: incomplete range fill (%d/%d bytes)", goi.lom.Cname(), manifest.Size(), r.size)
	} else if r.checksum {
		cksum = first.ExpCksum
		if cos.NoneC(cksum) {
			cksum, err = manifest.CombineCRC32C()
		}
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
	w.cw.Init(r.goi.lom, r.manifest)
	if first != nil {
		limited := io.LimitedReader{R: first.R, N: r.chunkSize}
		err := w.writeChunk(0, &limited, r.chunkSize, buf)
		// cancel the unused tail before closing the initial full GET
		r.stopFirst()
		cmn.CloseRemote(first.R, r.goi.lom, 4, cos.ModAIS)
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
				cmn.CloseRemote(res.R, r.goi.lom, 4, cos.ModAIS)
			}
			return
		}
		if res.Size != size {
			r.fail(http.StatusBadGateway, fmt.Errorf("%s: range at %d: expected %d bytes, got %d", r.goi.lom.Cname(), off, size, res.Size))
			cmn.CloseRemote(res.R, r.goi.lom, 4, cos.ModAIS)
			return
		}
		err := w.writeChunk(off, res.R, size, buf)
		cmn.CloseRemote(res.R, r.goi.lom, 4, cos.ModAIS)
		if err != nil {
			r.fail(http.StatusInternalServerError, err)
			return
		}
	}
}

// write directly to the manifest's chunk location
func (w *coldRangeWorker) writeChunk(off int64, reader io.Reader, size int64, buf []byte) error {
	_, err := w.cw.WriteChunk(int(off/w.r.chunkSize)+1, reader, size, buf)
	return err
}
