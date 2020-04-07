// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/stats"
	"golang.org/x/sync/errgroup"
)

//
// baseTaskEntry
//

type baseTaskEntry struct{}

func (e *baseTaskEntry) Stats(xact cmn.Xact) stats.XactStats {
	return stats.NewXactStats(xact)
}

//
// bckListTaskEntry
//

type bckListTaskEntry struct {
	baseTaskEntry
	ctx  context.Context
	xact *bckListTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
}

func (e *bckListTaskEntry) Start(bck cmn.Bck) error {
	xact := &bckListTask{
		XactBase: *cmn.NewXactBaseWithBucket(e.id, cmn.ActListObjects, bck),
		ctx:      e.ctx,
		t:        e.t,
		msg:      e.msg,
	}
	e.xact = xact
	go xact.Run()
	return nil
}

func (e *bckListTaskEntry) Kind() string  { return cmn.ActListObjects }
func (e *bckListTaskEntry) Get() cmn.Xact { return e.xact }

//
// bckSummaryTaskEntry
//

type bckSummaryTaskEntry struct {
	baseTaskEntry
	ctx  context.Context
	xact *bckSummaryTask
	t    cluster.Target
	id   string
	msg  *cmn.SelectMsg
}

func (e *bckSummaryTaskEntry) Start(bck cmn.Bck) error {
	xact := &bckSummaryTask{
		XactBase: *cmn.NewXactBaseWithBucket(e.id, cmn.ActSummaryBucket, bck),
		t:        e.t,
		msg:      e.msg,
		ctx:      e.ctx,
	}
	e.xact = xact
	go xact.Run()
	return nil
}

func (e *bckSummaryTaskEntry) Kind() string  { return cmn.ActSummaryBucket }
func (e *bckSummaryTaskEntry) Get() cmn.Xact { return e.xact }

//
// bckListTask
//

func (t *bckListTask) IsMountpathXact() bool { return false }

func (t *bckListTask) Run() {
	ctx := context.WithValue(
		t.ctx,
		objwalk.CtxPostCallbackKey,
		objwalk.PostCallbackFunc(func(lom *cluster.LOM) {
			t.ObjectsInc()
			t.BytesAdd(lom.Size())
		}),
	)
	walk := objwalk.NewWalk(ctx, t.t, t.Bck(), t.msg)
	if t.Bck().IsAIS() || t.msg.Cached {
		t.UpdateResult(walk.LocalObjPage())
	} else {
		t.UpdateResult(walk.CloudObjPage())
	}
}

func (t *bckListTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	t.res.Store(unsafe.Pointer(res))
	t.EndTime(time.Now())
}

func (t *bckListTask) Result() (interface{}, error) {
	ts := (*taskState)(t.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}

//
// bckSummaryTask
//

func (t *bckSummaryTask) IsMountpathXact() bool { return false }

func (t *bckSummaryTask) Run() {
	var (
		buckets []*cluster.Bck
		bmd     = t.t.GetBowner().Get()
		cfg     = cmn.GCO.Get()
	)
	if t.Bck().Name != "" {
		buckets = append(buckets, cluster.NewBckEmbed(t.Bck()))
	} else {
		if !t.Bck().HasProvider() || t.Bck().IsAIS() {
			provider := cmn.ProviderAIS
			bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
				buckets = append(buckets, bck)
				return false
			})
		}
		if !t.Bck().HasProvider() || cmn.IsProviderCloud(t.Bck(), true /*acceptAnon*/) {
			var (
				provider  = cfg.Cloud.Provider
				namespace = cfg.Cloud.Ns
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

	si, err := cluster.HrwTargetTask(t.msg.TaskID, t.t.GetSowner().Get())
	if err != nil {
		t.UpdateResult(nil, err)
		return
	}

	// When we are target which should not list CB we should only list cached objects.
	shouldListCB := si.ID() == t.t.Snode().ID() && !t.msg.Cached
	if !shouldListCB {
		t.msg.Cached = true
	}

	for _, bck := range buckets {
		go func(bck *cluster.Bck) {
			defer wg.Done()

			if err := bck.Init(t.t.GetBowner(), t.t.Snode()); err != nil {
				errCh <- err
				return
			}

			summary := cmn.BucketSummary{
				Bck:            bck.Bck,
				TotalDisksSize: totalDisksSize,
			}

			if t.msg.Fast && (bck.IsAIS() || t.msg.Cached) {
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

				for {
					walk := objwalk.NewWalk(context.Background(), t.t, bck.Bck, t.msg)
					if bck.IsAIS() {
						list, err = walk.LocalObjPage()
					} else {
						list, err = walk.CloudObjPage()
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
						if bck.IsAIS() || (!bck.IsAIS() && (shouldListCB || t.msg.Cached)) {
							summary.ObjCount++
						}

						t.ObjectsInc()
						t.BytesAdd(v.Size)
					}

					if list.PageMarker == "" {
						break
					}

					list.Entries = nil
					t.msg.PageMarker = list.PageMarker
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
	}

	t.UpdateResult(summaries, nil)
}

func (t *bckSummaryTask) doBckSummaryFast(bck *cluster.Bck) (objCount, size uint64, err error) {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		group, _          = errgroup.WithContext(context.Background())
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

				atomic.AddUint64(&objCount, uint64(fileCount))
				atomic.AddUint64(&size, dirSize)

				t.ObjectsAdd(int64(fileCount))
				t.BytesAdd(int64(dirSize))
				return nil
			}
		}(mpathInfo))
	}
	return objCount, size, group.Wait()
}

func (t *bckSummaryTask) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	t.res.Store(unsafe.Pointer(res))
	t.EndTime(time.Now())
}

func (t *bckSummaryTask) Result() (interface{}, error) {
	ts := (*taskState)(t.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
