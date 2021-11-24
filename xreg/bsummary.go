// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"
	"errors"
	"sync"
	gatomic "sync/atomic"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/xaction"
	"golang.org/x/sync/errgroup"
)

type (
	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}
	bsummFactory struct {
		xact *bsummXact
		ctx  context.Context
		t    cluster.Target
		uuid string
		msg  *cmn.BucketSummaryMsg
	}
	bsummXact struct {
		xaction.XactBase
		ctx context.Context
		t   cluster.Target
		msg *cmn.BucketSummaryMsg
		res atomic.Pointer
	}
)

// interface guard
var (
	_ Renewable = (*bsummFactory)(nil)
)

func RenewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	return defaultReg.renewBckSummary(ctx, t, bck, msg)
}

func (r *registry) renewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	r.entries.mtx.Lock()
	err := r.entries.del(msg.UUID)
	r.entries.mtx.Unlock()
	if err != nil {
		return err
	}
	e := &bsummFactory{ctx: ctx, t: t, uuid: msg.UUID, msg: msg}
	xact := &bsummXact{t: e.t, msg: e.msg, ctx: e.ctx}
	xact.InitBase(e.uuid, cmn.ActSummary, bck)
	e.xact = xact
	e.Start()
	r.add(e)
	return nil
}

//////////////////
// bsummFactory //
//////////////////

func (*bsummFactory) New(Args, *cluster.Bck) Renewable { debug.Assert(false); return nil }

func (e *bsummFactory) Start() error {
	go e.xact.Run(nil)
	return nil
}

func (*bsummFactory) Kind() string        { return cmn.ActSummary }
func (e *bsummFactory) Get() cluster.Xact { return e.xact }

// never called
func (*bsummFactory) Bucket() *cluster.Bck                         { debug.Assert(false); return nil }
func (*bsummFactory) UUID() string                                 { debug.Assert(false); return "" }
func (*bsummFactory) WhenPrevIsRunning(Renewable) (w WPR, e error) { debug.Assert(false); return }

///////////////
// bsummXact //
///////////////

func (t *bsummXact) Run(*sync.WaitGroup) {
	var (
		buckets []*cluster.Bck
		bmd     = t.t.Bowner().Get()
	)
	if t.Bck().Name != "" {
		buckets = append(buckets, t.Bck())
	} else {
		if !t.Bck().HasProvider() || t.Bck().IsAIS() {
			provider := cmn.ProviderAIS
			bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
				buckets = append(buckets, bck)
				return false
			})
		}
		if t.Bck().HasProvider() && !t.Bck().IsAIS() {
			var (
				provider  = t.Bck().Provider
				namespace = t.Bck().Ns
			)
			bmd.Range(&provider, &namespace, func(bck *cluster.Bck) bool {
				buckets = append(buckets, bck)
				return false
			})
		}
	}

	var (
		mtx       sync.Mutex
		wg        = &sync.WaitGroup{}
		errCh     = make(chan error, len(buckets))
		summaries = make(cmn.BucketsSummaries, 0, len(buckets))
	)
	wg.Add(len(buckets))

	totalDisksSize, err := fs.GetTotalDisksSize()
	if err != nil {
		t.UpdateResult(nil, err)
		return
	}

	si, err := cluster.HrwTargetTask(t.msg.UUID, t.t.Sowner().Get())
	if err != nil {
		t.UpdateResult(nil, err)
		return
	}

	// Check if we are the target that needs to list cloud bucket (we only want
	// single target to do it).
	shouldListCB := t.msg.Cached || (si.ID() == t.t.SID() && !t.msg.Cached)

	for _, bck := range buckets {
		go func(bck *cluster.Bck) {
			defer wg.Done()

			if err := bck.Init(t.t.Bowner()); err != nil {
				errCh <- err
				return
			}

			var (
				msg     = cmn.BucketSummaryMsg{}
				summary = cmn.BucketSummary{
					Bck:            bck.Bck,
					TotalDisksSize: totalDisksSize,
				}
			)

			// Each bucket should have it's own copy of msg (we may update it).
			cos.CopyStruct(&msg, t.msg)
			if bck.IsHTTP() {
				msg.Cached = true
			}

			if msg.Fast && (bck.IsAIS() || msg.Cached) {
				objCount, size, err := t.doBckSummaryFast(bck)
				if err != nil {
					errCh <- err
					return
				}
				summary.ObjCount = objCount
				summary.Size = size
			} else { // slow path
				var (
					list *cmn.BucketList
					err  error
				)

				if !shouldListCB {
					// When we are not the target which should list CB then
					// we should only list cached objects.
					msg.Cached = true
				}

				smsg := &cmn.SelectMsg{Props: cmn.GetPropsSize}
				if msg.Cached {
					smsg.Flags = cmn.SelectCached
				}
				for {
					walk := objwalk.NewWalk(context.Background(), t.t, bck, smsg)
					if bck.IsAIS() {
						list, err = walk.DefaultLocalObjPage(smsg)
					} else {
						list, err = walk.RemoteObjPage()
					}
					if err != nil {
						errCh <- err
						return
					}

					for _, v := range list.Entries {
						summary.Size += uint64(v.Size)

						// We should not include object count for cloud buckets
						// as other target will do that for us. We just need to
						// report the size on the disk.
						if bck.IsAIS() || (!bck.IsAIS() && shouldListCB) {
							summary.ObjCount++
						}

						t.ObjsInc()
						t.BytesAdd(v.Size)
					}

					if list.ContinuationToken == "" {
						break
					}

					list.Entries = nil
					smsg.ContinuationToken = list.ContinuationToken
				}
			}

			mtx.Lock()
			summaries = append(summaries, summary)
			mtx.Unlock()
		}(bck)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.UpdateResult(nil, err)
		return
	}

	t.UpdateResult(summaries, nil)
}

func (t *bsummXact) doBckSummaryFast(bck *cluster.Bck) (objCount, size uint64, err error) {
	var (
		availablePaths = fs.GetAvail()
		group, _       = errgroup.WithContext(context.Background())
	)

	for _, mpathInfo := range availablePaths {
		group.Go(func(mpathInfo *fs.MountpathInfo) func() error {
			return func() error {
				path := mpathInfo.MakePathCT(bck.Bck, fs.ObjectType)
				dirSize, err := ios.GetDirSize(path)
				if err != nil {
					return err
				}
				fileCount, err := ios.GetFileCount(path)
				if err != nil {
					return err
				}

				if bck.Props.Mirror.Enabled {
					copies := int(bck.Props.Mirror.Copies)
					dirSize /= uint64(copies)
					fileCount = fileCount/copies + fileCount%copies
				}

				gatomic.AddUint64(&objCount, uint64(fileCount))
				gatomic.AddUint64(&size, dirSize)

				t.ObjsAdd(int64(fileCount))
				t.BytesAdd(int64(dirSize))
				return nil
			}
		}(mpathInfo))
	}
	return objCount, size, group.Wait()
}

func (t *bsummXact) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	t.res.Store(unsafe.Pointer(res))
	t.Finish(err)
}

func (t *bsummXact) Result() (interface{}, error) {
	ts := (*taskState)(t.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
