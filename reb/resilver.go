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
		slab              *memsys.Slab
		buf               []byte
		skipGlobMisplaced bool
	}
)

// TODO: support non-object content types
func (reb *Manager) RunLocalReb(skipGlobMisplaced bool, buckets ...string) {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		cfg               = cmn.GCO.Get()
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

	xreb := xaction.Registry.RenewLocalReb()
	defer xreb.MarkDone()

	if len(buckets) > 0 {
		bucket = buckets[0] // special case: ais bucket
		cmn.Assert(bucket != "")
		xreb.SetBucket(bucket)
	}
	slab, err := reb.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)

	for _, mpathInfo := range availablePaths {
		var (
			bck    = cmn.Bck{Name: bucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
			jogger = &localJogger{
				joggerBase:        joggerBase{m: reb, xreb: &xreb.RebBase, wg: wg},
				slab:              slab,
				skipGlobMisplaced: skipGlobMisplaced,
			}
		)
		wg.Add(1)
		go jogger.jog(mpathInfo, bck)
	}

	if bucket != "" || !cfg.Cloud.Supported {
		goto wait
	}

	for _, mpathInfo := range availablePaths {
		var (
			bck    = cmn.Bck{Name: bucket, Provider: cfg.Cloud.Provider, Ns: cfg.Cloud.Ns}
			jogger = &localJogger{
				joggerBase:        joggerBase{m: reb, xreb: &xreb.RebBase, wg: wg},
				slab:              slab,
				skipGlobMisplaced: skipGlobMisplaced,
			}
		)
		wg.Add(1)
		go jogger.jog(mpathInfo, bck)
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

func (rj *localJogger) jog(mpathInfo *fs.MountpathInfo, bck cmn.Bck) {
	// the jogger is running in separate goroutine, so use defer to be
	// sure that `Done` is called even if the jogger crashes to avoid hang up
	defer rj.wg.Done()
	rj.buf = rj.slab.Alloc()
	opts := &fs.Options{
		Mpath:    mpathInfo,
		Bck:      bck,
		CTs:      []string{fs.ObjectType},
		Callback: rj.walk,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		if rj.xreb.Aborted() {
			glog.Infof("aborting traversal")
		} else {
			glog.Errorf("%s: failed to traverse err: %v", rj.m.t.Snode().Name(), err)
		}
	}
	rj.slab.Free(rj.buf)
}

func (rj *localJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var t = rj.m.t
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s aborted traversal", rj.xreb)
	}
	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: t, FQN: fqn}
	if err = lom.Init(cmn.Bck{}); err != nil {
		return nil
	}
	// optionally, skip those that must be globally rebalanced
	if rj.skipGlobMisplaced {
		smap := t.GetSowner().Get()
		if tsi, err := cluster.HrwTarget(lom.Uname(), smap); err == nil {
			if tsi.ID() != t.Snode().ID() {
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
