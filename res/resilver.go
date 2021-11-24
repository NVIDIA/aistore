// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
	"github.com/NVIDIA/aistore/xs"
)

const timedDuration = time.Minute / 2 // see also: timedDuration in tgtgfn.go

type (
	Res struct {
		t cluster.Target
		// last or current resilver's time interval
		begin atomic.Int64
		end   atomic.Int64
	}
	Args struct {
		UUID              string
		Notif             *xaction.NotifXact
		Mpath             string
		SkipGlobMisplaced bool
	}
	joggerCtx struct {
		xact cluster.Xact
		t    cluster.Target
	}
)

func New(t cluster.Target) *Res {
	return &Res{t: t}
}

func (res *Res) IsActive() (yes bool) {
	begin := res.begin.Load()
	if begin == 0 {
		return
	}
	now := mono.NanoTime()
	if time.Duration(now-begin) < timedDuration {
		yes = true
	} else {
		end := res.end.Load()
		yes = end == 0 || time.Duration(now-end) < timedDuration
	}
	return
}

func (res *Res) _begin() {
	res.begin.Store(mono.NanoTime())
	res.end.Store(0)
}

func (res *Res) _end() {
	res.end.Store(mono.NanoTime())
}

func (res *Res) RunResilver(args Args) {
	res._begin()
	defer res._end()
	if fatalErr, writeErr := fs.PersistMarker(cmn.ResilverMarker); fatalErr != nil || writeErr != nil {
		glog.Errorf("FATAL: %v, WRITE: %v", fatalErr, writeErr)
		return
	}
	availablePaths, _ := fs.Get()
	if len(availablePaths) < 1 {
		glog.Error(cmn.ErrNoMountpaths)
		return
	}

	xact := xreg.RenewResilver(args.UUID).(*xs.Resilver)
	if args.Notif != nil {
		args.Notif.Xact = xact
		xact.AddNotif(args.Notif)
	}
	glog.Infoln(xact.Name())

	// jogger group
	var jg *mpather.JoggerGroup
	slab, err := res.t.PageMM().GetSlab(memsys.MaxPageSlabSize)
	debug.AssertNoErr(err)
	jctx := &joggerCtx{xact: xact, t: res.t}
	opts := &mpather.JoggerGroupOpts{
		T:                     res.t,
		CTs:                   []string{fs.ObjectType, fs.ECSliceType},
		VisitObj:              jctx.visitObj,
		VisitCT:               jctx.visitCT,
		Slab:                  slab,
		SkipGloballyMisplaced: args.SkipGlobMisplaced,
	}
	if args.Mpath == "" {
		jg = mpather.NewJoggerGroup(opts)
	} else {
		jg = mpather.NewJoggerGroup(opts, args.Mpath)
	}

	// run
	res.end.Store(0)
	jg.Run()

	// Wait for abort or joggers to finish.
	select {
	case <-xact.ChanAbort():
		if err := jg.Stop(); err != nil {
			glog.Errorf("Resilver (uuid=%q) aborted, stopped with err: %v", args.UUID, err)
		} else {
			glog.Infof("Resilver (uuid=%q) aborted", args.UUID)
		}
	case <-jg.ListenFinished():
		fs.RemoveMarker(cmn.ResilverMarker)
	}

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
	// Slice without metafile - skip it as unusable, let LRU clean it up
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

	// If EC is enabled, first copy the metafile and then, if successful, copy the object
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
		// EC cleanup and return
		if metaNewPath != "" {
			if err = os.Remove(metaNewPath); err != nil {
				glog.Warningf("%s: nested (%s: %v)", lom, metaNewPath, err)
			}
		}
		return
	}
	debug.Assert(size != cos.ContentLengthUnknown)
	// EC: Remove the original metafile.
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup old metafile %q: %v", lom, metaOldPath, err)
		}
	}

	rj.xact.BytesAdd(size)
	rj.xact.ObjsInc()

	// NOTE: not deleting _misplaced_ and copied - delegating to `storage cleanup` and/or LRU
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
