// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// tunables
const (
	blobChunkSize   = 16 * cos.MiB
	numChunkReaders = 3
)

type (
	blobArgs struct {
		lom        *core.LOM
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
		eof      bool
	}
)

// interface guard
var (
	_ core.Xact      = (*XactBlobDl)(nil)
	_ xreg.Renewable = (*blobFactory)(nil)
)

func RenewBlobDl(uuid string, lom *core.LOM, chunkSize int64, numReaders ...int) xreg.RenewRes {
	oa, errCode, err := core.T.Backend(lom.Bck()).HeadObj(context.Background(), lom)
	if err != nil {
		return xreg.RenewRes{Err: err}
	}
	debug.Assert(errCode == 0)
	args := &blobArgs{
		lom:        lom,
		chunkSize:  chunkSize,
		fullSize:   oa.Size,
		numReaders: numChunkReaders,
	}
	if chunkSize == 0 {
		args.chunkSize = blobChunkSize
	}
	if len(numReaders) > 0 {
		args.numReaders = numReaders[0]
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

	mm := core.T.PageMM()
	cnt := min(p.args.chunkSize/memsys.MaxPageSlabSize, 16)
	r.readers = make([]*blobReader, p.args.numReaders)
	r.sgls = make([]*memsys.SGL, p.args.numReaders)
	for i := range r.readers {
		r.readers[i] = &blobReader{
			parent: r,
		}
		r.sgls[i] = mm.NewSGL(cnt*memsys.MaxPageSlabSize, memsys.MaxPageSlabSize)
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
		return xreg.WprUse, nil
	}
	return xreg.WprKeepAndStartNew, nil
}

//
// XactBlobDl
//

func (r *XactBlobDl) Run(*sync.WaitGroup) {
	var (
		err     error
		pending []blobDone
	)
	r.start()
outer:
	for {
		select {
		case done := <-r.doneCh:
			sgl := done.sgl
			if r.p.args.fullSize == done.roff+sgl.Size() || done.err == io.EOF {
				r.eof = true
				if r.p.args.fullSize > done.roff+sgl.Size() {
					err = fmt.Errorf("%s: premature EOF downloading %s: %d > %d", r, r.p.args.lom.Cname(),
						r.p.args.fullSize, done.roff+sgl.Size())
				}
			} else if done.err != nil {
				err = done.err
				goto fin
			}
			debug.Assert(sgl.Size() > 0)

			// add pending in offset-descending order
			if done.roff != r.woff {
				debug.Assert(done.roff > r.woff)
				debug.Assert((done.roff-r.woff)%r.p.args.chunkSize == 0)
				pending = append(pending, blobDone{roff: -1})
				var inserted bool
				for i := range pending {
					if pending[i].roff >= 0 && pending[i].roff < done.roff {
						copy(pending[i+1:], pending[i:])
						pending[i] = done
						inserted = true
						break
					}
				}
				if !inserted {
					pending[len(pending)-1] = done
				}
				break
			}

			// TODO -- FIXME: write here...

			r.woff += sgl.Size()
			r.ObjsAdd(0, sgl.Size())
			sgl.Reset()

			if r.eof {
				if len(pending) == 0 {
					goto fin
				}
			} else if r.nextRoff < r.p.args.fullSize {
				r.workCh <- blobWork{sgl, r.nextRoff}
				r.nextRoff += r.p.args.chunkSize
			}

			// walk backwards and plug any holes
			for i := len(pending) - 1; i >= 0; i-- {
				done := pending[i]
				debug.Assert(done.roff > 0)
				if done.roff > r.woff {
					continue outer
				}
				pending = pending[:i]

				// TODO -- FIXME: and here...

				sgl := done.sgl
				r.woff += sgl.Size()
				r.ObjsAdd(0, sgl.Size())
				sgl.Reset()

				if !r.eof {
					debug.Assert(r.nextRoff <= r.p.args.fullSize)
					r.workCh <- blobWork{sgl, r.nextRoff}
					r.nextRoff += r.p.args.chunkSize
				}
			}
			if r.eof && len(pending) == 0 {
				goto fin
			}
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	close(r.workCh)
	if err != nil {
		r.Abort(err)
	}
	r.ObjsAdd(1, 0)
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
			// TODO -- FIXME: checksum(s)
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
	snap.IdleX = r.IsIdle()
	return
}
