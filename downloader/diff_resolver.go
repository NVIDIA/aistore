// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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
	// DiffResolver is entity that computes difference between two streams
	// of objects. The streams are expected to be in sorted order.
	DiffResolver struct {
		srcCh    chan *cluster.LOM
		dstCh    chan *DstElement
		resultCh chan DiffResolverResult

		err     atomic.Value
		stopped atomic.Bool
	}

	CloudResource struct {
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
		Action uint8
		Src    *cluster.LOM
		Dst    *DstElement
		Err    error
	}
)

func NewDiffResolver() *DiffResolver {
	return &DiffResolver{
		srcCh:    make(chan *cluster.LOM, 1000),
		dstCh:    make(chan *DstElement, 1000),
		resultCh: make(chan DiffResolverResult, 1000),
	}
}

func (dr *DiffResolver) Start() {
	go func() {
		defer close(dr.resultCh)
		src, srcOk := <-dr.srcCh
		dst, dstOk := <-dr.dstCh
		for {
			if !srcOk && !dstOk {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverEOF,
				}
				return
			} else if !srcOk {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverRecv,
					Dst:    dst,
				}
				dst, dstOk = <-dr.dstCh
			} else if !dstOk {
				dr.resultCh <- DiffResolverResult{
					Action: DiffResolverSend,
					Src:    src,
				}
				src, srcOk = <-dr.srcCh
			} else {
				if src.ObjName > dst.ObjName {
					dr.resultCh <- DiffResolverResult{
						Action: DiffResolverRecv,
						Dst:    dst,
					}
					dst, dstOk = <-dr.dstCh
				} else if src.ObjName < dst.ObjName {
					// TODO: Check if source is cloud object. If it is and
					//  destination is cloud as well then we should delete
					//  the source!
					dr.resultCh <- DiffResolverResult{
						Action: DiffResolverSend,
						Src:    src,
					}
					src, srcOk = <-dr.srcCh
				} else { /* s.ObjName == d.ObjName */
					equal, err := CompareObjects(src, dst)
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
	}()
}

func (dr *DiffResolver) PushSrc(v interface{}) {
	switch x := v.(type) {
	case *cluster.LOM:
		dr.srcCh <- x
	default:
		cmn.AssertMsg(false, fmt.Sprintf("%T", x))
	}
}

func (dr *DiffResolver) CloseSrc() { close(dr.srcCh) }

func (dr *DiffResolver) PushDst(v interface{}) {
	var d *DstElement
	switch x := v.(type) {
	case *CloudResource:
		d = &DstElement{
			ObjName: x.ObjName,
		}
	case *WebResource:
		d = &DstElement{
			ObjName: x.ObjName,
			Link:    x.Link,
		}
	default:
		cmn.AssertMsg(false, fmt.Sprintf("%T", x))
	}

	dr.dstCh <- d
}

func (dr *DiffResolver) CloseDst() { close(dr.dstCh) }

func (dr *DiffResolver) Next() (DiffResolverResult, error) {
	if err := dr.err.Load(); err != nil {
		return DiffResolverResult{}, err.(error)
	}

	r, ok := <-dr.resultCh
	if !ok {
		return DiffResolverResult{Action: DiffResolverEOF}, nil
	}
	return r, nil
}

func (dr *DiffResolver) Stop()           { dr.stopped.Store(true) }
func (dr *DiffResolver) Stopped() bool   { return dr.stopped.Load() }
func (dr *DiffResolver) Abort(err error) { dr.err.Store(err) }

func CompareObjects(src *cluster.LOM, dst *DstElement) (bool, error) {
	if err := src.Load(); err != nil {
		if cmn.IsObjNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return compareObjects(src, dst)
}
