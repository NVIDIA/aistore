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
	"github.com/NVIDIA/aistore/cmn/debug"
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
		msg  *apc.BckSummMsg
	}
	bsummXact struct {
		xact.Base
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
	msg := args.Custom.(*apc.BckSummMsg)
	p := &bsummFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return p
}

func (p *bsummFactory) Start() error {
	xctn := &bsummXact{t: p.T, msg: p.msg}
	xctn.InitBase(p.UUID(), apc.ActSummaryBck, p.Bck)
	p.xctn = xctn
	xact.GoRunW(xctn)
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

func (r *bsummXact) Run(rwg *sync.WaitGroup) {
	debug.AssertNoErr(r.Bck().Validate())
	rwg.Done()

	totalDisksSize, err := fs.GetTotalDisksSize()
	if err != nil {
		r.UpdateResult(nil, err)
		return
	}
	si, err := cluster.HrwTargetTask(r.msg.UUID, r.t.Sowner().Get())
	if err != nil {
		r.UpdateResult(nil, err)
		return
	}

	// Check if we are the target that needs to list remote bucket
	// (we only want a single target doing it).
	shouldListCB := r.msg.Cached || (si.ID() == r.t.SID() && !r.msg.Cached)

	if err := r.Bck().Init(r.t.Bowner()); err != nil {
		r.UpdateResult(nil, err)
		return
	}
	summaries := make(cmn.BckSummaries, 1)
	summaries[0].TotalDisksSize = totalDisksSize
	err = r.runBck(r.Bck(), &summaries[0], shouldListCB)
	r.UpdateResult(summaries, err)
}

func (r *bsummXact) runBck(bck *cluster.Bck, summ *cmn.BckSumm, shouldListCB bool) error {
	msg := apc.BckSummMsg{}
	summ.Bck.Copy(bck.Bucket())

	// Each bucket should have it's own copy of the msg (we may update it).
	cos.CopyStruct(&msg, r.msg)
	if bck.IsHTTP() {
		msg.Cached = true
	}

	// fast path
	if msg.Fast && (bck.IsAIS() || msg.Cached) {
		objCount, size, err := r.doBckSummaryFast(bck)
		summ.ObjCount = objCount
		summ.Size = size
		return err
	}

	// slow path
	var (
		list *cmn.BucketList
		err  error
	)
	if !shouldListCB {
		msg.Cached = true
	}
	lsmsg := &apc.ListObjsMsg{Props: apc.GetPropsSize}
	if msg.Cached {
		lsmsg.Flags = apc.LsPresent
	}
	for {
		walk := objwalk.NewWalk(context.Background(), r.t, bck, lsmsg)
		if bck.IsAIS() {
			list, err = walk.DefaultLocalObjPage()
		} else {
			list, err = walk.RemoteObjPage()
		}
		if err != nil {
			return err
		}
		for _, v := range list.Entries {
			summ.Size += uint64(v.Size)

			// for remote backends, not updating obj-count to avoid double counting
			// (by other targets).
			if bck.IsAIS() || (!bck.IsAIS() && shouldListCB) {
				summ.ObjCount++
			}
			r.ObjsAdd(1, v.Size)
		}
		if list.ContinuationToken == "" {
			break
		}
		list.Entries = nil
		lsmsg.ContinuationToken = list.ContinuationToken
	}
	return nil
}

func (r *bsummXact) doBckSummaryFast(bck *cluster.Bck) (objCount, size uint64, err error) {
	var (
		availablePaths = fs.GetAvail()
		group, _       = errgroup.WithContext(context.Background())
	)

	for _, mpathInfo := range availablePaths {
		group.Go(func(mpathInfo *fs.MountpathInfo) func() error {
			return func() error {
				path := mpathInfo.MakePathCT(bck.Bucket(), fs.ObjectType)
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
				r.ObjsAdd(fileCount, int64(dirSize))
				return nil
			}
		}(mpathInfo))
	}
	return objCount, size, group.Wait()
}

func (r *bsummXact) UpdateResult(result interface{}, err error) {
	res := &taskState{Err: err}
	if err == nil {
		res.Result = result
	}
	r.res.Store(unsafe.Pointer(res))
	r.Finish(err)
}

func (r *bsummXact) Result() (interface{}, error) {
	ts := (*taskState)(r.res.Load())
	if ts == nil {
		return nil, errors.New("no result to load")
	}
	return ts.Result, ts.Err
}
