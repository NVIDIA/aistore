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
		xres *xs.Resilver
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

	xres := xreg.RenewResilver(args.UUID).(*xs.Resilver)
	if args.Notif != nil {
		args.Notif.Xact = xres
		xres.AddNotif(args.Notif)
	}
	glog.Infoln(xres.Name())

	// jogger group
	var (
		jg        *mpather.JoggerGroup
		slab, err = res.t.PageMM().GetSlab(memsys.MaxPageSlabSize)
		jctx      = &joggerCtx{xres: xres, t: res.t}

		opts = &mpather.JoggerGroupOpts{
			T:                     res.t,
			CTs:                   []string{fs.ObjectType, fs.ECSliceType},
			VisitObj:              jctx.visitObj,
			VisitCT:               jctx.visitCT,
			Slab:                  slab,
			SkipGloballyMisplaced: args.SkipGlobMisplaced,
		}
	)
	debug.AssertNoErr(err)
	if args.Mpath == "" {
		jg = mpather.NewJoggerGroup(opts)
	} else {
		jg = mpather.NewJoggerGroup(opts, args.Mpath)
	}

	// run and block waiting
	res.end.Store(0)
	jg.Run()
	err = res.wait(jg, xres)

	xres.Finish(err)
}

// Wait for an abort or for resilvering joggers to finish.
func (res *Res) wait(jg *mpather.JoggerGroup, xres *xs.Resilver) (err error) {
	tsi := res.t.Snode()
	for {
		select {
		case <-xres.ChanAbort():
			if err = jg.Stop(); err != nil {
				glog.Errorf("%s: %s aborted, err: %v", tsi, xres, err)
			} else {
				glog.Infof("%s: %s aborted", tsi, xres)
			}
			return cmn.NewErrAborted(xres.Name(), "", err)
		case <-jg.ListenFinished():
			if err = fs.RemoveMarker(cmn.ResilverMarker); err == nil {
				glog.Infof("%s: %s removed marker ok", tsi, xres)
			}
			return
		}
	}
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

// TODO -- FIXME: update `in` and `out` resilver stats
//
// TODO: revisit error handling with respect to: which other ones are critical enough to abort
// TODO: refactor EC bits
// TODO: check for OOS preemptively
// NOTE: not deleting _misplaced_ and successfully copied objects -
//       delegating to `storage cleanup`
func (jg *joggerCtx) visitObj(lom *cluster.LOM, buf []byte) error {
	var (
		hlom  *cluster.LOM
		xname = jg.xres.Name()
	)
	if !lom.TryLock(true) {
		return nil // skipping _busy_
	}
	defer func() {
		lom.Unlock(true)
		if hlom != nil {
			cluster.FreeLOM(hlom)
		}
	}()
	// EC metafile
	var metaOldPath, metaNewPath string
	if !lom.IsHRW() && lom.Bprops().EC.Enabled {
		// first, copy the metafile and then, if successful, the object
		newMpath, _, err := cluster.ResolveFQN(lom.HrwFQN)
		if err != nil {
			glog.Warningf("%s: %s %v", xname, lom, err)
			return nil
		}
		ct := cluster.NewCTFromLOM(lom, fs.ObjectType)
		metaOldPath, metaNewPath, err = _moveECMeta(ct, lom.MpathInfo(), newMpath.MpathInfo, buf)
		if err != nil {
			glog.Warningf("%s: failed to copy metafile %s %q -> %q: %v",
				xname, lom, lom.MpathInfo().Path, newMpath.MpathInfo.Path, err)
			return nil
		}
	}
	//
	// check hrw location and copies; fix all of the above, if need be
	//
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil
	}
	for {
		mi, isHrw := lom.ToMpath()
		if mi == nil {
			break
		}
		if isHrw {
			err := lom.Copy(mi, buf)
			if err != nil {
				glog.Errorf("%s: failed to restore %s, err: %v", xname, lom, err)
				// EC cleanup and return
				if metaNewPath != "" {
					if err = os.Remove(metaNewPath); err != nil {
						glog.Warningf("%s: nested (%s %s: %v)", xname, lom, metaNewPath, err)
					}
				}
				return err
			}
			hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
			hlom = cluster.AllocLOMbyFQN(hrwFQN)

			err = hlom.Init(lom.Bucket())
			if err != nil {
				glog.Errorf("%s: %s %v", xname, hlom, err) // e.g., "bucket removed"
				return err
			}
			debug.Assert(hlom.MpathInfo().Path == mi.Path)
			hlom.Uncache(false /*delDirty*/)
			err = hlom.Load(false /*cache it*/, true /*locked*/)
			if err != nil {
				debug.AssertNoErr(err)
				return err
			}
			// can swap on the fly
			lom = hlom
			continue
		}
		err := lom.Copy(mi, buf)
		if err == nil {
			continue
		}
		if cos.IsErrOOS(err) {
			err = cmn.NewErrAborted(xname, "visit-obj", err)
		}
		glog.Warningf("%s: failed to copy %s to %s, err: %v", xname, lom, mi, err)
		break
	}
	// EC: remove old metafile
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup %s old metafile %q: %v", xname, lom, metaOldPath, err)
		}
	}
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
