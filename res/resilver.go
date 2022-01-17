// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xs"
)

const timedDuration = 4 * time.Second // see also: timedDuration in tgtgfn.go

type (
	Res struct {
		t cluster.Target
		// last or current resilver's time interval
		begin atomic.Int64
		end   atomic.Int64
	}
	Args struct {
		UUID              string
		Notif             *xact.NotifXact
		Rmi               *fs.MountpathInfo
		Action            string
		PostDD            func(rmi *fs.MountpathInfo, action string, xres *xs.Resilver, err error)
		SkipGlobMisplaced bool
		SingleRmiJogger   bool
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
	debug.Assert(args.PostDD == nil || (args.Action == cmn.ActMountpathDetach || args.Action == cmn.ActMountpathDisable))

	if args.SingleRmiJogger {
		jg = mpather.NewJoggerGroup(opts, args.Rmi.Path)
		glog.Infof("%s, action %q, jogger->(%q)", xres.Name(), args.Action, args.Rmi)
	} else {
		jg = mpather.NewJoggerGroup(opts)
		if args.Rmi != nil {
			glog.Infof("%s, action %q, rmi %s, num %d", xres.Name(), args.Action, args.Rmi, jg.Num())
		} else {
			glog.Infof("%s, num %d", xres.Name(), jg.Num())
		}
	}

	// run and block waiting
	res.end.Store(0)
	jg.Run()
	err = res.wait(jg, xres)

	// callback to, finally, detach-disable
	if args.PostDD != nil {
		args.PostDD(args.Rmi, args.Action, xres, err)
	}
	xres.Finish(err)
}

// Wait for an abort or for resilvering joggers to finish.
func (res *Res) wait(jg *mpather.JoggerGroup, xres *xs.Resilver) (err error) {
	tsi := res.t.Snode()
	for {
		select {
		case errCause := <-xres.ChanAbort():
			if err = jg.Stop(); err != nil {
				glog.Errorf("%s: %s aborted (cause %v), traversal err %v", tsi, xres, errCause, err)
			} else {
				glog.Infof("%s: %s aborted (cause %v)", tsi, xres, errCause)
			}
			return cmn.NewErrAborted(xres.Name(), "", errCause)
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

// TODO: revisit EC bits and check for OOS preemptively
// NOTE: not deleting extra copies - delegating to `storage cleanup`
func (jg *joggerCtx) visitObj(lom *cluster.LOM, buf []byte) (errHrw error) {
	const maxRetries = 3
	var (
		orig   = lom
		hlom   *cluster.LOM
		xname  = jg.xres.Name()
		size   int64
		copied bool
	)
	if !lom.TryLock(true) { // NOTE: skipping busy
		time.Sleep(time.Second >> 1)
		if !lom.TryLock(true) {
			return
		}
	}
	// cleanup
	defer func() {
		lom.Unlock(true)
		if hlom != nil {
			cluster.FreeLOM(hlom)
		}
		if copied && errHrw == nil {
			jg.xres.ObjsAdd(1, size)
		}
	}()

	// 1. fix EC metafile
	var metaOldPath, metaNewPath string
	if !lom.IsHRW() && lom.Bprops().EC.Enabled {
		// copy metafile
		newMpath, _, errEc := cluster.ResolveFQN(lom.HrwFQN)
		if errEc != nil {
			glog.Warningf("%s: %s %v", xname, lom, errEc)
			return nil
		}
		ct := cluster.NewCTFromLOM(lom, fs.ObjectType)
		metaOldPath, metaNewPath, errEc = _moveECMeta(ct, lom.MpathInfo(), newMpath.MpathInfo, buf)
		if errEc != nil {
			glog.Warningf("%s: failed to copy EC metafile %s %q -> %q: %v",
				xname, lom, lom.MpathInfo().Path, newMpath.MpathInfo.Path, errEc)
			return nil
		}
	}

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil
	}
	size = lom.SizeBytes()
	// 2. fix hrw location; fail and subsequently abort if unsuccessful
	var retries int
	mi, isHrw := lom.ToMpath()
	if mi == nil {
		goto ret // nothing to do
	}
redo:
	if isHrw {
		hlom, errHrw = jg.fixHrw(lom, mi, buf)
		if errHrw != nil {
			if !os.IsNotExist(errHrw) && !strings.Contains(errHrw.Error(), "does not exist") {
				glog.Errorf("%s: failed to restore %s, errHrw: %v", xname, lom, errHrw)
			}
			// EC cleanup and return
			if metaNewPath != "" {
				if errHrw = os.Remove(metaNewPath); errHrw != nil {
					glog.Warningf("%s: nested (%s %s: %v)", xname, lom, metaNewPath, errHrw)
				}
			}
			return
		}
		// can swap on the fly; NOTE -- TODO: better
		lom.Unlock(true)
		lom = hlom
		copied = true
		lom.Lock(true)
	}

	// 3. fix copies
	for {
		mi, isHrw := lom.ToMpath()
		if mi == nil {
			break
		}
		if isHrw {
			// redo hlom in an unlikely event
			retries++
			if retries > maxRetries {
				errHrw = fmt.Errorf("%s: hrw mountpaths keep changing (%s(%s) => %s => %s ...)",
					xname, orig, orig.MpathInfo(), hlom.MpathInfo(), mi)
				glog.Error(errHrw)
				return
			}
			copied = false
			cluster.FreeLOM(hlom)
			lom, hlom = orig, nil
			time.Sleep(cmn.Timeout.CplaneOperation() / 2)
			goto redo
		}
		err := lom.Copy(mi, buf)
		if err == nil {
			copied = true
			continue
		}
		if cos.IsErrOOS(err) {
			err = cmn.NewErrAborted(xname, "visit-obj", err)
		} else if !os.IsNotExist(err) && !strings.Contains(err.Error(), "does not exist") {
			glog.Warningf("%s: failed to copy %s to %s, err: %v", xname, lom, mi, err)
		}
		break
	}
ret:
	// EC: remove old metafile
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			glog.Warningf("%s: failed to cleanup %s old metafile %q: %v", xname, lom, metaOldPath, err)
		}
	}
	return nil
}

func (*joggerCtx) fixHrw(lom *cluster.LOM, mi *fs.MountpathInfo, buf []byte) (hlom *cluster.LOM, err error) {
	if err = lom.Copy(mi, buf); err != nil {
		return
	}
	hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
	hlom = cluster.AllocLOMbyFQN(hrwFQN)
	if err = hlom.Init(lom.Bucket()); err != nil {
		return
	}
	debug.Assert(hlom.MpathInfo().Path == mi.Path)

	// uncache and reload
	hlom.Uncache(false /*delDirty*/)
	err = hlom.Load(false /*cache it*/, true /*locked*/)
	return
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
