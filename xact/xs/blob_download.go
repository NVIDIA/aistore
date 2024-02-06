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
	"os"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
// 3. tune-up: (chunk size, slab size, full size) vs memory pressure

// tunables (via apc.BlobMsg)
const (
	dfltChunkSize  = 2 * cos.MiB
	minChunkSize   = memsys.DefaultBufSize
	maxChunkSize   = 16 * cos.MiB
	dfltNumReaders = 4

	maxInitialSizeSGL = 128 // vec length
	maxTotalChunks    = 128 * cos.MiB
)

type (
	blobArgs struct {
		lom        *core.LOM
		lmfh       *os.File
		wfqn       string
		chunkSize  int64
		fullSize   int64
		numReaders int
	}
	blobReader struct {
		parent *XactBlobDl
	}
	blobWork struct {
		sgl  *memsys.SGL
		roff int64
	}
	blobDone struct {
		err  error
		sgl  *memsys.SGL
		roff int64
	}
	blobFactory struct {
		xreg.RenewBase
		args *blobArgs
		xctn *XactBlobDl
	}
	XactBlobDl struct {
		xact.Base
		p        *blobFactory
		readers  []*blobReader
		workCh   chan blobWork
		doneCh   chan blobDone
		nextRoff int64
		woff     int64
		sgls     []*memsys.SGL
		wg       *sync.WaitGroup
	}
)

// interface guard
var (
	_ core.Xact      = (*XactBlobDl)(nil)
	_ xreg.Renewable = (*blobFactory)(nil)
)

func RenewBlobDl(uuid string, lom *core.LOM, wfqn string, lmfh *os.File, msg *apc.BlobMsg) xreg.RenewRes {
	fullSize := msg.FullSize
	if fullSize == 0 {
		oa, errCode, err := core.T.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			return xreg.RenewRes{Err: err}
		}
		debug.Assert(errCode == 0)
		fullSize = oa.Size
	}
	args := &blobArgs{
		lom:        lom,
		lmfh:       lmfh,
		wfqn:       wfqn,
		chunkSize:  msg.ChunkSize,
		fullSize:   fullSize,
		numReaders: msg.NumWorkers,
	}

	// validate, assign defaults (tune-up below)
	if args.chunkSize == 0 {
		args.chunkSize = dfltChunkSize
	} else if args.chunkSize < minChunkSize {
		nlog.Infoln("Warning: chunk size", cos.ToSizeIEC(args.chunkSize, 1), "is below permitted minimum",
			cos.ToSizeIEC(minChunkSize, 0))
		args.chunkSize = minChunkSize
	} else if args.chunkSize > maxChunkSize {
		nlog.Infoln("Warning: chunk size", cos.ToSizeIEC(args.chunkSize, 1), "exceeds permitted maximum",
			cos.ToSizeIEC(maxChunkSize, 0))
		args.chunkSize = maxChunkSize
	}
	if args.numReaders == 0 {
		args.numReaders = dfltNumReaders
	}
	if int64(args.numReaders)*args.chunkSize > fullSize {
		args.numReaders = int((fullSize + args.chunkSize - 1) / args.chunkSize)
	}
	if a := cmn.MaxParallelism(); a < args.numReaders {
		args.numReaders = a
	}

	return xreg.RenewBucketXact(apc.ActBlobDl, lom.Bck(), xreg.Args{UUID: uuid, Custom: args})
}

//
// blobFactory
//

func (*blobFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	debug.Assert(bck.IsRemote())
	p := &blobFactory{
		RenewBase: xreg.RenewBase{Args: args, Bck: bck},
		args:      args.Custom.(*blobArgs),
	}
	return p
}

func (p *blobFactory) Start() error {
	r := &XactBlobDl{
		p:      p,
		workCh: make(chan blobWork, p.args.numReaders),
		doneCh: make(chan blobDone, p.args.numReaders),
	}
	r.InitBase(p.Args.UUID, p.Kind(), p.args.lom.Bck())

	// tune-up
	var (
		mm       = core.T.PageMM()
		slabSize = int64(memsys.MaxPageSlabSize)
		pre      = mm.Pressure()
	)
	if pre >= memsys.PressureExtreme {
		return errors.New(r.Name() + ": extreme memory pressure - not starting")
	}
	switch pre {
	case memsys.PressureHigh:
		slabSize = memsys.DefaultBufSize
		p.args.numReaders = 1
		nlog.Warningln(r.Name() + ": high memory pressure detected...")
	case memsys.PressureModerate:
		slabSize >>= 1
		p.args.numReaders = min(3, p.args.numReaders)
	}

	cnt := (p.args.chunkSize + slabSize - 1) / slabSize
	if cnt > maxInitialSizeSGL {
		cnt = maxInitialSizeSGL
	}

	// add a reader, if possible
	nr := int64(p.args.numReaders)
	if pre == memsys.PressureLow && p.args.numReaders < cmn.MaxParallelism() &&
		nr < (p.args.fullSize+p.args.chunkSize-1)/p.args.chunkSize &&
		nr*p.args.chunkSize < maxTotalChunks-p.args.chunkSize {
		p.args.numReaders++
	}

	// init and allocate
	r.readers = make([]*blobReader, p.args.numReaders)
	r.sgls = make([]*memsys.SGL, p.args.numReaders)
	for i := range r.readers {
		r.readers[i] = &blobReader{
			parent: r,
		}
		r.sgls[i] = mm.NewSGL(cnt*slabSize, slabSize)
	}
	r.wg = &sync.WaitGroup{}
	p.xctn = r
	return nil
}

func (*blobFactory) Kind() string     { return apc.ActBlobDl }
func (p *blobFactory) Get() core.Xact { return p.xctn }

func (p *blobFactory) WhenPrevIsRunning(prev xreg.Renewable) (xreg.WPR, error) {
	xprev := prev.Get().(*XactBlobDl)
	if xprev.p.args.lom.Bucket().Equal(p.args.lom.Bucket()) && xprev.p.args.lom.ObjName == p.args.lom.ObjName {
		return xreg.WprUse, cmn.NewErrXactUsePrev(prev.Get().String())
	}
	return xreg.WprKeepAndStartNew, nil
}

//
// XactBlobDl
//

func (r *XactBlobDl) Name() string { return r.Base.Name() + "/" + r.p.args.lom.ObjName }

func (r *XactBlobDl) Run(*sync.WaitGroup) {
	var (
		err     error
		pending []blobDone
		eof     bool
	)
	nlog.Infoln(r.Name()+": chunk-size", cos.ToSizeIEC(r.p.args.chunkSize, 0)+
		", num-concurrent-readers", r.p.args.numReaders)
	r.start()
outer:
	for {
		select {
		case done := <-r.doneCh:
			sgl := done.sgl
			if r.p.args.fullSize == done.roff+sgl.Size() || done.err == io.EOF {
				eof = true
				if r.p.args.fullSize > done.roff+sgl.Size() {
					err = fmt.Errorf("%s: premature EOF: expected size %d, have %d",
						r.Name(), r.p.args.fullSize, done.roff+sgl.Size())
					goto fin
				}
			} else if done.err != nil {
				err = done.err
				goto fin
			}
			debug.Assert(sgl.Size() > 0)
			debug.Assertf(eof == (r.nextRoff >= r.p.args.fullSize), "%t, %d, %d", eof, r.nextRoff, r.p.args.fullSize)

			// add pending in the offset-descending order
			if done.roff != r.woff {
				debug.Assert(done.roff > r.woff)
				debug.Assert((done.roff-r.woff)%r.p.args.chunkSize == 0)
				pending = append(pending, blobDone{roff: -1})
				for i := range pending {
					if i == len(pending)-1 || (pending[i].roff >= 0 && pending[i].roff < done.roff) {
						copy(pending[i+1:], pending[i:])
						pending[i] = done
						continue outer
					}
				}
			}
			// write (type 1)
			if err = r.write(sgl); err != nil {
				goto fin
			}

			if r.nextRoff < r.p.args.fullSize {
				r.workCh <- blobWork{sgl, r.nextRoff}
				r.nextRoff += r.p.args.chunkSize
			}

			// walk backwards and plug any holes
			for i := len(pending) - 1; i >= 0; i-- {
				done := pending[i]
				if done.roff > r.woff {
					break
				}
				debug.Assert(done.roff == r.woff)

				// remove from pending and write (type 2)
				sgl := done.sgl
				pending = pending[:i]
				if err = r.write(sgl); err != nil {
					goto fin
				}
				if r.nextRoff < r.p.args.fullSize {
					r.workCh <- blobWork{sgl, r.nextRoff}
					r.nextRoff += r.p.args.chunkSize
				}
			}
			if r.woff >= r.p.args.fullSize {
				debug.Assertf(r.woff == r.p.args.fullSize, "%d > %d", r.woff, r.p.args.fullSize)
				goto fin
			}
			if eof && cmn.Rom.FastV(5, cos.SmoduleXs) {
				nlog.Errorf("%s eof w/pending: woff=%d, next=%d, size=%d",
					r.Name(), r.woff, r.nextRoff, r.p.args.fullSize)
				for i := len(pending) - 1; i >= 0; i-- {
					nlog.Errorf("   roff %d", pending[i].roff)
				}
			}
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	close(r.workCh)
	r.p.args.lom.SetSize(r.woff)
	cos.Close(r.p.args.lmfh)

	if err == nil {
		_, err = core.T.FinalizeObj(r.p.args.lom, r.p.args.wfqn, r, cmn.OwtGetPrefetchLock)
	}
	if err == nil {
		r.ObjsAdd(1, 0)
	} else {
		if errRemove := cos.RemoveFile(r.p.args.wfqn); errRemove != nil && !os.IsNotExist(errRemove) {
			nlog.Errorln("nested err:", errRemove)
		}
		r.Abort(err)
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
		r.workCh <- blobWork{r.sgls[i], r.nextRoff}
		r.nextRoff += r.p.args.chunkSize
	}
}

func (r *XactBlobDl) write(sgl *memsys.SGL) error {
	size := sgl.Size()

	written, err := io.Copy(r.p.args.lmfh, sgl)
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

	core.FreeLOM(r.p.args.lom)
}

//
// blobReader
//

func (reader *blobReader) run() {
	var (
		err     error
		written int64
		a       = reader.parent.p.args
		ctx     = context.Background()
	)
	for {
		msg, ok := <-reader.parent.workCh
		if !ok {
			break
		}
		sgl := msg.sgl
		res := core.T.Backend(a.lom.Bck()).GetObjReader(ctx, a.lom, msg.roff, a.chunkSize)
		if reader.parent.IsAborted() {
			break
		}
		if err = res.Err; err == nil {
			written, err = io.Copy(sgl, res.R)
		}
		if err != nil {
			reader.parent.doneCh <- blobDone{err, sgl, msg.roff}
			break
		}
		debug.Assert(res.Size == written, res.Size, " ", written)
		debug.Assert(sgl.Size() == written, sgl.Size(), " ", written)
		debug.Assert(sgl.Size() == sgl.Len(), sgl.Size(), " ", sgl.Len())

		reader.parent.doneCh <- blobDone{nil, sgl, msg.roff}
	}
	reader.parent.wg.Done()
}

func (r *XactBlobDl) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	// HACK shortcut to support progress bar
	snap.Stats.InBytes = r.p.args.fullSize
	return
}
