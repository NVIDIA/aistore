// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	resilverJogger struct {
		joggerBase
		slab              *memsys.Slab
		buf               []byte
		skipGlobMisplaced bool
	}
)

func (reb *Manager) RunResilver(id string, skipGlobMisplaced bool, notifs ...cmn.Notif) {
	cmn.Assert(id != "")
	var (
		availablePaths, _ = fs.Get()
		err               = fs.PutMarker(xaction.GetMarkerName(cmn.ActResilver))
	)
	if err != nil {
		glog.Errorln("failed to create resilver marker", err)
	}

	xreb := xaction.Registry.RenewResilver(id)
	defer xreb.MarkDone()

	if len(notifs) != 0 {
		xreb.AddNotif(notifs[0])
	}

	glog.Infoln(xreb.String())

	slab, err := reb.t.GetMMSA().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)

	wg := &sync.WaitGroup{}
	for _, mpathInfo := range availablePaths {
		var (
			jogger = &resilverJogger{
				joggerBase:        joggerBase{m: reb, xreb: &xreb.RebBase, wg: wg},
				slab:              slab,
				skipGlobMisplaced: skipGlobMisplaced,
			}
		)
		wg.Add(1)
		go jogger.jog(mpathInfo)
	}
	wg.Wait()

	if !xreb.Aborted() {
		if err := fs.RemoveMarker(xaction.GetMarkerName(cmn.ActResilver)); err != nil {
			glog.Errorf("%s: failed to remove in-progress mark, err: %v", reb.t.Snode(), err)
		}
	}
	reb.t.GetGFN(cluster.GFNLocal).Deactivate()
	xreb.Finish()
}

//
// resilverJogger
//

func (rj *resilverJogger) jog(mpathInfo *fs.MountpathInfo) {
	// the jogger is running in separate goroutine, so use defer to be
	// sure that `Done` is called even if the jogger crashes to avoid hang up
	rj.buf = rj.slab.Alloc()
	defer func() {
		rj.slab.Free(rj.buf)
		rj.wg.Done()
	}()

	opts := &fs.Options{
		Mpath:    mpathInfo,
		CTs:      []string{fs.ObjectType, ec.SliceType},
		Callback: rj.walk,
		Sorted:   false,
	}
	rj.m.t.GetBowner().Get().Range(nil, nil, func(bck *cluster.Bck) bool {
		opts.ErrCallback = nil
		opts.Bck = bck.Bck
		if err := fs.Walk(opts); err != nil {
			if rj.xreb.Aborted() {
				glog.Infof("aborting traversal")
			} else {
				glog.Errorf("%s: failed to traverse err: %v", rj.m.t.Snode(), err)
			}
			return true
		}
		return rj.xreb.Aborted()
	})
}

// Copies a slice and its metafile (if exists) to the current mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func (rj *resilverJogger) moveSlice(fqn string, ct *cluster.CT) {
	uname := ct.Bck().MakeUname(ct.ObjName())
	destMpath, _, err := cluster.HrwMpath(uname)
	if err != nil {
		glog.Warning(err)
		return
	}
	if destMpath.Path == ct.ParsedFQN().MpathInfo.Path {
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bck().Bck, ec.SliceType, ct.ObjName())
	srcMetaFQN, destMetaFQN, err := rj.moveECMeta(ct, ct.ParsedFQN().MpathInfo, destMpath)
	if err != nil {
		return
	}
	// TODO: a slice without metafile - skip it as unusable, let LRU clean it up
	if srcMetaFQN == "" {
		return
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("resilver moving %q -> %q", fqn, destFQN)
	}
	if _, _, err = cmn.CopyFile(fqn, destFQN, rj.buf, cmn.ChecksumNone); err != nil {
		glog.Errorf("failed to copy %q -> %q: %v. Rolling back", fqn, destFQN, err)
		if err = os.Remove(destMetaFQN); err != nil {
			glog.Warningf("failed to cleanup metafile copy %q: %v", destMetaFQN, err)
		}
	}
	errMeta := os.Remove(srcMetaFQN)
	errSlice := os.Remove(fqn)
	if errMeta != nil || errSlice != nil {
		glog.Warningf("failed to cleanup %q: %v, %v", fqn, errSlice, errMeta)
	}
}

// Copies EC metafile to correct mpath. It returns FQNs of the source and
// destination for a caller to do proper cleanup. Empty values means: either
// the source FQN does not exist(err==nil), or copying failed
func (rj *resilverJogger) moveECMeta(ct *cluster.CT, srcMpath, dstMpath *fs.MountpathInfo) (
	string, string, error) {
	src := srcMpath.MakePathFQN(ct.Bck().Bck, ec.MetaType, ct.ObjName())
	// If metafile does not exist it may mean that EC has not processed the
	// object yet (e.g, EC was enabled after the bucket was filled), or
	// the metafile has gone
	if err := fs.Access(src); os.IsNotExist(err) {
		return "", "", nil
	}
	dst := dstMpath.MakePathFQN(ct.Bck().Bck, ec.MetaType, ct.ObjName())
	_, _, err := cmn.CopyFile(src, dst, rj.buf, cmn.ChecksumNone)
	if err == nil {
		return src, dst, err
	}
	if os.IsNotExist(err) {
		err = nil
	}
	return "", "", err
}

// Copies an object and its metafile (if exists) to the resilver mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func (rj *resilverJogger) moveObject(fqn string, ct *cluster.CT) {
	var (
		t                        = rj.m.t
		lom                      = &cluster.LOM{T: t, FQN: fqn}
		metaOldPath, metaNewPath string
		err                      error
	)
	if err = lom.Init(cmn.Bck{}); err != nil {
		return
	}
	// skip those that are _not_ locally misplaced
	if lom.IsHRW() {
		return
	}

	// First, copy metafile if EC is enables. Copy the object only if the
	// metafile has been copies successfully
	if lom.Bprops().EC.Enabled {
		newMpath, _, err := cluster.ResolveFQN(lom.HrwFQN)
		if err != nil {
			glog.Warningf("%s: %v", lom, err)
			return
		}
		metaOldPath, metaNewPath, err = rj.moveECMeta(ct, lom.ParsedFQN.MpathInfo, newMpath.MpathInfo)
		if err != nil {
			glog.Warningf("%s: failed to move metafile %q -> %q: %v",
				lom, lom.ParsedFQN.MpathInfo.Path, newMpath.MpathInfo.Path, err)
			return
		}
	}
	copied, err := t.CopyObject(lom, lom.Bck(), rj.buf, true)
	if err != nil || !copied {
		if err != nil {
			glog.Errorf("%s: %v", lom, err)
		}
		// EC: cleanup new copy of the metafile
		if metaNewPath != "" {
			if err = os.Remove(metaNewPath); err != nil {
				glog.Warningf("%s: nested (%s: %v)", lom, metaNewPath, err)
			}
		}
		return
	}
	// EC: remove the original metafile
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup old metafile %q: %v", lom, metaOldPath, err)
		}
	}
	// NOTE: rely on LRU to remove "misplaced"
}

func (rj *resilverJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var t = rj.m.t
	if rj.xreb.Aborted() {
		return cmn.NewAbortedErrorDetails("traversal", rj.xreb.String())
	}
	if de.IsDir() {
		return nil
	}

	ct, err := cluster.NewCTFromFQN(fqn, t.GetBowner())
	if err != nil {
		if cmn.IsErrBucketLevel(err) {
			return err
		}
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Warningf("CT for %q: %v", fqn, err)
		}
		return nil
	}
	// optionally, skip those that must be globally rebalanced
	if rj.skipGlobMisplaced {
		uname := ct.Bck().MakeUname(ct.ObjName())
		tsi, err := cluster.HrwTarget(uname, t.GetSowner().Get())
		if err != nil {
			return err
		}
		if tsi.ID() != t.Snode().ID() {
			return nil
		}
	}
	if ct.ContentType() == ec.SliceType {
		if !ct.Bprops().EC.Enabled {
			// Since %ec directory is inside a bucket, it is safe to skip
			// the entire %ec directory when EC is disabled for the bucket
			return filepath.SkipDir
		}
		rj.moveSlice(fqn, ct)
		return nil
	}
	cmn.Assert(ct.ContentType() == fs.ObjectType)
	rj.moveObject(fqn, ct)
	return nil
}
