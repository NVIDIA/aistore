// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"errors"
	"sync"
	gatomic "sync/atomic"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"golang.org/x/sync/errgroup"
)

type (
	taskState struct {
		Result interface{} `json:"res"`
		Err    error       `json:"error"`
	}
	bsummFactory struct {
		xreg.RenewBase
		xctn *bsummXact
		ctx  context.Context
		msg  *apc.BckSummMsg
	}
	bsummXact struct {
		xact.Base
		ctx context.Context
		t   cluster.Target
		msg *apc.BckSummMsg
		res atomic.Pointer
	}
)

// interface guard
var (
	_ xreg.Renewable = (*bsummFactory)(nil)
	_ cluster.Xact   = (*bsummXact)(nil)
)

//////////////////
// bsummFactory //
//////////////////

func (*bsummFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	summArgs := args.Custom.(*xreg.BckSummaryArgs)
	p := &bsummFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, ctx: summArgs.Ctx, msg: summArgs.Msg}
	return p
}

func (p *bsummFactory) Start() error {
	xctn := &bsummXact{t: p.T, msg: p.msg, ctx: p.ctx}
	xctn.InitBase(p.UUID(), apc.ActSummaryBck, p.Bck)
	p.xctn = xctn
	go p.xctn.Run(nil)
	return nil
}

func (*bsummFactory) Kind() string        { return apc.ActSummaryBck }
func (p *bsummFactory) Get() cluster.Xact { return p.xctn }

func (*bsummFactory) WhenPrevIsRunning(xreg.Renewable) (w xreg.WPR, e error) {
	return xreg.WprUse, nil
}

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
			provider := apc.ProviderAIS
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
		summaries = make(cmn.BckSummaries, 0, len(buckets))
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
				msg     = apc.BckSummMsg{}
				summary = cmn.BckSumm{
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

				lsmsg := &apc.ListObjsMsg{Props: apc.GetPropsSize}
				if msg.Cached {
					lsmsg.Flags = apc.LsPresent
				}
				for {
					walk := objwalk.NewWalk(context.Background(), t.t, bck, lsmsg)
					if bck.IsAIS() {
						list, err = walk.DefaultLocalObjPage()
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
						t.ObjsAdd(1, v.Size)
					}

					if list.ContinuationToken == "" {
						break
					}

					list.Entries = nil
					lsmsg.ContinuationToken = list.ContinuationToken
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
				t.ObjsAdd(fileCount, int64(dirSize))
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
