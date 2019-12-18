// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	localJogger struct {
		joggerBase
		slab              *memsys.Slab2
		buf               []byte
		skipGlobMisplaced bool
	}
)

// TODO: support non-object content types
func (reb *Manager) RunLocalReb(skipGlobMisplaced bool, buckets ...string) {
	var (
		xreb              *xaction.LocalReb
		availablePaths, _ = fs.Mountpaths.Get()
		pmarker           = cmn.PersistentMarker(cmn.ActLocalReb)
		file, err         = cmn.CreateFile(pmarker)
		bucket            string
		wg                = &sync.WaitGroup{}
	)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}
	if len(buckets) > 0 {
		bucket = buckets[0] // special case: ais bucket
	}
	if bucket != "" {
		xreb = xaction.Registry.RenewLocalReb(len(availablePaths))
		xreb.SetBucket(bucket)
	} else {
		xreb = xaction.Registry.RenewLocalReb(len(availablePaths) * 2)
	}
	slab, err := reb.t.GetMem2().GetSlab2(memsys.MaxSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)

	for _, mpathInfo := range availablePaths {
		var mpathL string
		if bucket == "" {
			mpathL = mpathInfo.MakePath(fs.ObjectType, cmn.AIS)
		} else {
			mpathL = mpathInfo.MakePathBucket(fs.ObjectType, bucket, cmn.AIS)
		}
		jogger := &localJogger{joggerBase: joggerBase{m: reb, mpath: mpathL, xreb: &xreb.RebBase, wg: wg},
			slab:              slab,
			skipGlobMisplaced: skipGlobMisplaced,
		}
		wg.Add(1)
		go jogger.jog()
	}
	if bucket != "" {
		goto wait
	}
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, cmn.Cloud)
		jogger := &localJogger{joggerBase: joggerBase{m: reb, mpath: mpathC, xreb: &xreb.RebBase, wg: wg},
			slab:              slab,
			skipGlobMisplaced: skipGlobMisplaced,
		}
		wg.Add(1)
		go jogger.jog()
	}
wait:
	glog.Infoln(xreb.String())
	wg.Wait()

	if pmarker != "" {
		if !xreb.Aborted() {
			if err := cmn.RemoveFile(pmarker); err != nil {
				glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.t.Snode().Name(), pmarker, err)
			}
		}
	}
	reb.t.GetGFN(cluster.GFNLocal).Deactivate()
	xreb.EndTime(time.Now())
}

//
// localJogger
//

func (rj *localJogger) jog() {
	rj.buf = rj.slab.Alloc()
	opts := &fs.Options{
		Callback: rj.walk,
		Sorted:   false,
	}
	if err := fs.Walk(rj.mpath, opts); err != nil {
		if rj.xreb.Aborted() {
			glog.Infof("Aborting %s traversal", rj.mpath)
		} else {
			glog.Errorf("%s: failed to traverse %s, err: %v", rj.m.t.Snode().Name(), rj.mpath, err)
		}
	}
	rj.xreb.NotifyDone()
	rj.slab.Free(rj.buf)
	rj.wg.Done()
}

func (rj *localJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var t = rj.m.t
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s aborted, path %s", rj.xreb, rj.mpath)
	}
	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: t, FQN: fqn}
	if err = lom.Init("", ""); err != nil {
		return nil
	}
	// optionally, skip those that must be globally rebalanced
	if rj.skipGlobMisplaced {
		smap := t.GetSowner().Get()
		if tsi, err := cluster.HrwTarget(lom.Uname(), smap); err == nil {
			if tsi.DaemonID != t.Snode().DaemonID {
				return nil
			}
		}
	}
	// skip those that are _not_ locally misplaced
	if lom.IsHRW() {
		return nil
	}

	copied, err := t.CopyObject(lom, lom.Bck(), rj.buf, true)
	if err != nil {
		glog.Warningf("%s: %v", lom, err)
		return nil
	}
	if !copied {
		return nil
	}
	if lom.HasCopies() { // TODO: punt replicated and erasure copied to LRU
		return nil
	}
	// misplaced with no copies? remove right away
	lom.Lock(true)
	if err = cmn.RemoveFile(lom.FQN); err != nil {
		glog.Warningf("%s: %v", lom, err)
	}
	lom.Unlock(true)
	return nil
}
