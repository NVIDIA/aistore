// Package dload implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dload

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

const (
	DiffResolverSend = iota
	DiffResolverRecv
	DiffResolverDelete
	DiffResolverSkip
	DiffResolverErr
	DiffResolverEOF
)

type (
	DiffResolverCtx interface {
		CompareObjects(*core.LOM, *DstElement) (bool, error)
		IsObjFromRemote(*core.LOM) (bool, error)
	}

	defaultDiffResolverCtx struct{}

	// DiffResolver is entity that computes difference between two streams
	// of objects. The streams are expected to be in sorted order.
	DiffResolver struct {
		ctx      DiffResolverCtx
		srcCh    chan *core.LOM
		dstCh    chan *DstElement
		resultCh chan DiffResolverResult
		err      cos.Errs
		stopped  atomic.Bool
	}

	BackendResource struct {
		ObjName string
	}

	WebResource struct {
		ObjName string
		Link    string
	}

	DstElement struct {
		ObjName string
		Version string
		Link    string
	}

	DiffResolverResult struct {
		Err    error
		Src    *core.LOM
		Dst    *DstElement
		Action uint8
	}
)

//////////////////
// DiffResolver //
//////////////////

// TODO: configurable burst size of the channels, plus `chanFull` check
func NewDiffResolver(ctx DiffResolverCtx) *DiffResolver {
	return &DiffResolver{
		ctx:      ctx,
		srcCh:    make(chan *core.LOM, 128),
		dstCh:    make(chan *DstElement, 128),
		resultCh: make(chan DiffResolverResult, 128),
		err:      cos.NewErrs(),
	}
}

func (dr *DiffResolver) Start() {
	defer close(dr.resultCh)
	src, srcOk := <-dr.srcCh
	dst, dstOk := <-dr.dstCh
	for {
		if !srcOk && !dstOk {
			// Both src & dst streams exhausted
			dr.resultCh <- DiffResolverResult{
				Action: DiffResolverEOF,
			}
			return
		}

		switch {
		case !srcOk || (dstOk && src.ObjName > dst.ObjName):
			// Either extra item at dst or dst item missing from sorted src stream
			dr.resultCh <- DiffResolverResult{
				Action: DiffResolverRecv,
				Dst:    dst,
			}
			dst, dstOk = <-dr.dstCh
		case !dstOk || (srcOk && src.ObjName < dst.ObjName):
			// Either extra item at src or item missing from sorted dst stream
			remote, err := dr.ctx.IsObjFromRemote(src)
			if err != nil {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverErr,
					Src:    src,
					Dst:    dst,
					Err:    err,
				}
				return
			}
			if remote {
				debug.Assert(!dstOk || dst.Link == "") // destination must be remote as well
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverDelete,
					Src:    src,
				}
			} else {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverSend,
					Src:    src,
				}
			}
			src, srcOk = <-dr.srcCh
		default:
			// src.ObjName == dst.ObjName
			equal, err := dr.ctx.CompareObjects(src, dst)
			if err != nil {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverErr,
					Src:    src,
					Dst:    dst,
					Err:    err,
				}
				return
			}
			if equal {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverSkip,
					Src:    src,
					Dst:    dst,
				}
			} else {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverRecv,
					Dst:    dst,
				}
			}
			src, srcOk = <-dr.srcCh
			dst, dstOk = <-dr.dstCh
		}
	}
}

func (dr *DiffResolver) PushSrc(v any) {
	switch x := v.(type) {
	case *core.LOM:
		dr.srcCh <- x
	default:
		debug.FailTypeCast(v)
	}
}

func (dr *DiffResolver) CloseSrc() { close(dr.srcCh) }

func (dr *DiffResolver) PushDst(v any) {
	var d *DstElement
	switch x := v.(type) {
	case *BackendResource:
		d = &DstElement{
			ObjName: x.ObjName,
		}
	case *WebResource:
		d = &DstElement{
			ObjName: x.ObjName,
			Link:    x.Link,
		}
	default:
		debug.FailTypeCast(v)
	}

	dr.dstCh <- d
}

func (dr *DiffResolver) CloseDst() { close(dr.dstCh) }

func (dr *DiffResolver) Next() (DiffResolverResult, error) {
	if cnt, err := dr.err.JoinErr(); cnt > 0 {
		return DiffResolverResult{}, err
	}
	r, ok := <-dr.resultCh
	if !ok {
		return DiffResolverResult{Action: DiffResolverEOF}, nil
	}
	return r, nil
}

func (dr *DiffResolver) Stop()           { dr.stopped.Store(true) }
func (dr *DiffResolver) Stopped() bool   { return dr.stopped.Load() }
func (dr *DiffResolver) Abort(err error) { dr.err.Add(err) }

func (dr *DiffResolver) walk(job jobif) {
	defer dr.CloseSrc()
	opts := &fs.WalkBckOpts{
		WalkOpts: fs.WalkOpts{CTs: []string{fs.ObjCT}, Sorted: true},
	}
	opts.WalkOpts.Bck.Copy(job.Bck())
	opts.Callback = func(fqn string, _ fs.DirEntry) error { return dr.cb(fqn, job) }

	err := fs.WalkBck(opts)
	if err != nil && !cmn.IsErrAborted(err) {
		dr.Abort(err)
	}
}

func (dr *DiffResolver) cb(fqn string, job jobif) error {
	if dr.Stopped() {
		return cmn.NewErrAborted(job.String(), "diff-resolver stopped", nil)
	}
	lom := &core.LOM{}
	if err := lom.InitFQN(fqn, job.Bck()); err != nil {
		return err
	}
	if job.checkObj(lom.ObjName) {
		dr.PushSrc(lom)
	}
	return nil
}

func (dr *DiffResolver) push(job jobif, d *dispatcher) {
	defer func() {
		dr.CloseDst()
		if !job.Sync() {
			dr.CloseSrc()
		}
	}()

	for {
		objs, ok, err := job.genNext()
		if err != nil {
			dr.Abort(err)
			return
		}
		if !ok || dr.Stopped() {
			return
		}
		for _, obj := range objs {
			if d.checkAborted() {
				err := cmn.NewErrAborted(job.String(), "", nil)
				dr.Abort(err)
				return
			}
			if d.checkAbortedJob(job) {
				dr.Stop()
				return
			}
			if !job.Sync() {
				// When it is not a sync job, push LOM for a given object
				// because we need to check if it exists.
				lom := &core.LOM{ObjName: obj.objName}
				if err := lom.InitBck(job.Bck()); err != nil {
					dr.Abort(err)
					return
				}
				dr.PushSrc(lom)
			}
			if obj.link != "" {
				dr.PushDst(&WebResource{
					ObjName: obj.objName,
					Link:    obj.link,
				})
			} else {
				dr.PushDst(&BackendResource{
					ObjName: obj.objName,
				})
			}
		}
	}
}

////////////////////////////
// defaultDiffResolverCtx //
////////////////////////////

func (*defaultDiffResolverCtx) CompareObjects(src *core.LOM, dst *DstElement) (bool, error) {
	src.Lock(false)
	defer src.Unlock(false)
	if err := src.Load(true /*cache it*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return CompareObjects(src, dst)
}

func (*defaultDiffResolverCtx) IsObjFromRemote(src *core.LOM) (bool, error) {
	if err := src.Load(true /*cache it*/, false /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	objSrc, ok := src.GetCustomKey(cmn.SourceObjMD)
	if !ok {
		return false, nil
	}
	return objSrc != cmn.WebObjMD, nil
}
