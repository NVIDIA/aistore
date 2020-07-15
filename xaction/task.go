// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/NVIDIA/aistore/bcklist"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"golang.org/x/sync/errgroup"
)

//
// baseTaskEntry
//

type baseTaskEntry struct {
	uuid string
}

//
// bckListTaskEntry
//

type bckListTaskEntry struct {
	baseBckEntry
	ctx  context.Context
	xact *bcklist.BckListTask
	t    cluster.Target
	msg  *cmn.SelectMsg
}

func (e *bckListTaskEntry) Start(bck cmn.Bck) error {
	xact := bcklist.NewBckListTask(e.ctx, e.t, bck, e.msg, e.uuid)
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
	msg  *cmn.SelectMsg
}

func (e *bckSummaryTaskEntry) Start(bck cmn.Bck) error {
	xact := &bckSummaryTask{
		XactBase: *cmn.NewXactBaseBck(e.uuid, cmn.ActSummaryBucket, bck),
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
// bckSummaryTask
//

func (t *bckSummaryTask) IsMountpathXact() bool { return false }

func (t *bckSummaryTask) Run() error {
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
		if !t.Bck().HasProvider() || t.Bck().IsCloud(cmn.AnyCloud) {
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
		return err
	}

	si, err := cluster.HrwTargetTask(t.msg.UUID, t.t.GetSowner().Get())
	if err != nil {
		t.UpdateResult(nil, err)
		return err
	}

	// Check if we are the target that needs to list cloud bucket (we only want
	// single target to do it).
	shouldListCB := t.msg.Cached || (si.ID() == t.t.Snode().ID() && !t.msg.Cached)

	for _, bck := range buckets {
		go func(bck *cluster.Bck) {
			defer wg.Done()

			if err := bck.Init(t.t.GetBowner(), t.t.Snode()); err != nil {
				errCh <- err
				return
			}

			var (
				msg     = &cmn.SelectMsg{}
				summary = cmn.BucketSummary{
					Bck:            bck.Bck,
					TotalDisksSize: totalDisksSize,
				}
			)

			// Each bucket should have it's own copy of msg (we may update it).
			cmn.CopyStruct(msg, t.msg)

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

				for {
					walk := objwalk.NewWalk(context.Background(), t.t, bck, msg)
					if bck.IsAIS() {
						wi := walkinfo.NewWalkInfo(t.ctx, t.t, msg)
						list, err = walk.DefaultLocalObjPage(msg.WantObjectsCnt(), wi)
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
						if bck.IsAIS() || (!bck.IsAIS() && shouldListCB) {
							summary.ObjCount++
						}

						t.ObjectsInc()
						t.BytesAdd(v.Size)
					}

					if list.PageMarker == "" {
						break
					}

					list.Entries = nil
					msg.PageMarker = list.PageMarker
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
		return err
	}

	t.UpdateResult(summaries, nil)
	return nil
}

func (t *bckSummaryTask) doBckSummaryFast(bck *cluster.Bck) (objCount, size uint64, err error) {
	var (
		availablePaths, _ = fs.Get()
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
	t.Finish(err)
}

func (t *bckSummaryTask) Result() (interface{}, error) {
	ts := (*taskState)(t.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
