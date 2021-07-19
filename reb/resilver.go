// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
	"github.com/NVIDIA/aistore/xs"
)

type (
	joggerCtx struct {
		xact cluster.Xact
		t    cluster.Target
	}
)

func (reb *Manager) RunResilver(id string, skipGlobMisplaced bool, notifs ...*xaction.NotifXact) {
	debug.Assert(id != "")

	availablePaths, _ := fs.Get()
	if len(availablePaths) < 2 {
		glog.Errorf("Cannot run resilver with less than 2 mountpaths (%d)", len(availablePaths))
		return
	}
	if err := fs.PersistMarker(cmn.ResilverMarker); err != nil {
		glog.Errorf("Failed to create resilver marker, err: %v", err)
	}

	xact := xreg.RenewResilver(id).(*xs.Resilver)
	if len(notifs) != 0 {
		notifs[0].Xact = xact
		xact.AddNotif(notifs[0])
	}

	glog.Infoln(xact.String())

	slab, err := reb.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	debug.AssertNoErr(err)

	jctx := &joggerCtx{xact: xact, t: reb.t}
	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:                     reb.t,
		CTs:                   []string{fs.ObjectType, fs.ECSliceType},
		VisitObj:              jctx.visitObj,
		VisitCT:               jctx.visitCT,
		Slab:                  slab,
		SkipGloballyMisplaced: skipGlobMisplaced,
	})
	jg.Run()

	// Wait for abort or joggers to finish.
	select {
	case <-xact.ChanAbort():
		if err := jg.Stop(); err != nil {
			glog.Errorf("Resilver (id=%q) aborted, stopped with err: %v", id, err)
		} else {
			glog.Infof("Resilver (id=%q) aborted", id)
		}
	case <-jg.ListenFinished():
		fs.RemoveMarker(cmn.ResilverMarker)
	}

	reb.t.GFN(cluster.GFNLocal).Deactivate()
	xact.Finish(nil)
}

// Copies a slice and its metafile (if exists) to the current mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func _mvSlice(ct *cluster.CT, buf []byte) {
	uname := ct.Bck().MakeUname(ct.ObjectName())
	destMpath, _, err := cluster.HrwMpath(uname)
	if err != nil {
		glog.Warning(err)
		return
	}
	if destMpath.Path == ct.MpathInfo().Path {
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bucket(), fs.ECSliceType, ct.ObjectName())
	srcMetaFQN, destMetaFQN, err := _moveECMeta(ct, ct.MpathInfo(), destMpath, buf)
	if err != nil {
		return
	}
	// TODO: a slice without metafile - skip it as unusable, let LRU clean it up
	if srcMetaFQN == "" {
		return
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("Resilver moving %q -> %q", ct.FQN(), destFQN)
	}
	if _, _, err = cos.CopyFile(ct.FQN(), destFQN, buf, cos.ChecksumNone); err != nil {
		glog.Errorf("Failed to copy %q -> %q: %v. Rolling back", ct.FQN(), destFQN, err)
		if err = os.Remove(destMetaFQN); err != nil {
			glog.Warningf("Failed to cleanup metafile copy %q: %v", destMetaFQN, err)
		}
	}
	errMeta := os.Remove(srcMetaFQN)
	errSlice := os.Remove(ct.FQN())
	if errMeta != nil || errSlice != nil {
		glog.Warningf("Failed to cleanup %q: %v, %v", ct.FQN(), errSlice, errMeta)
	}
}

// Copies EC metafile to correct mpath. It returns FQNs of the source and
// destination for a caller to do proper cleanup. Empty values means: either
// the source FQN does not exist(err==nil), or copying failed
func _moveECMeta(ct *cluster.CT, srcMpath, dstMpath *fs.MountpathInfo, buf []byte) (string, string, error) {
	src := srcMpath.MakePathFQN(ct.Bucket(), fs.ECMetaType, ct.ObjectName())
	// If metafile does not exist it may mean that EC has not processed the
	// object yet (e.g, EC was enabled after the bucket was filled), or
	// the metafile has gone
	if err := fs.Access(src); os.IsNotExist(err) {
		return "", "", nil
	}
	dst := dstMpath.MakePathFQN(ct.Bucket(), fs.ECMetaType, ct.ObjectName())
	_, _, err := cos.CopyFile(src, dst, buf, cos.ChecksumNone)
	if err == nil {
		return src, dst, nil
	}
	if os.IsNotExist(err) {
		err = nil
	}
	return "", "", err
}

// Copies an object and its metafile (if exists) to the resilver mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func (rj *joggerCtx) moveObject(lom *cluster.LOM, buf []byte) {
	var (
		metaOldPath string
		metaNewPath string
		err         error
	)
	// Skip those that are _not_ locally misplaced.
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
		ct := cluster.NewCTFromLOM(lom, fs.ObjectType)
		metaOldPath, metaNewPath, err = _moveECMeta(ct, lom.MpathInfo(), newMpath.MpathInfo, buf)
		if err != nil {
			glog.Warningf("%s: failed to move metafile %q -> %q: %v",
				lom, lom.MpathInfo().Path, newMpath.MpathInfo.Path, err)
			return
		}
	}
	size, err := rj.t.CopyObject(lom, &cluster.CopyObjectParams{BckTo: lom.Bck(), Buf: buf}, true /*local*/)
	if err != nil {
		glog.Errorf("%s: %v", lom, err)
		// EC: Cleanup new copy of the metafile.
		if metaNewPath != "" {
			if err = os.Remove(metaNewPath); err != nil {
				glog.Warningf("%s: nested (%s: %v)", lom, metaNewPath, err)
			}
		}
		return
	}
	// EC: Remove the original metafile.
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup old metafile %q: %v", lom, metaOldPath, err)
		}
	}

	rj.xact.BytesAdd(size)
	rj.xact.ObjectsInc()
	// NOTE: Rely on LRU to remove "misplaced".
}

func (rj *joggerCtx) visitObj(lom *cluster.LOM, buf []byte) (err error) {
	rj.moveObject(lom, buf)
	return nil
}

func (*joggerCtx) visitCT(ct *cluster.CT, buf []byte) (err error) {
	debug.Assert(ct.ContentType() == fs.ECSliceType)
	if !ct.Bck().Props.EC.Enabled {
		// Since `%ec` directory is inside a bucket, it is safe to skip
		// the entire `%ec` directory when EC is disabled for the bucket.
		return filepath.SkipDir
	}
	_mvSlice(ct, buf)
	return nil
}
