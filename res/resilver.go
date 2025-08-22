// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

const timedDuration = 4 * time.Second // see also: timedDuration in tgtgfn.go

type (
	Res struct {
		// last or current resilver's time interval
		begin atomic.Int64
		end   atomic.Int64
	}
	Args struct {
		UUID            string
		Notif           *xact.NotifXact
		Rmi             *fs.Mountpath
		Action          string
		PostDD          func(rmi *fs.Mountpath, action string, xres *xs.Resilver, err error)
		Custom          xreg.ResArgs
		SingleRmiJogger bool
	}
	joggerCtx struct {
		xres *xs.Resilver
	}
)

func New() *Res { return &Res{} }

func (res *Res) IsActive(multiplier int64) (yes bool) {
	begin := res.begin.Load()
	if begin == 0 {
		return
	}
	now := mono.NanoTime()
	if now-begin < multiplier*int64(timedDuration) {
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

func (res *Res) RunResilver(args *Args, tstats cos.StatsUpdater) {
	res._begin()
	defer res._end()

	if fatalErr, writeErr := fs.PersistMarker(fname.ResilverMarker); fatalErr != nil || writeErr != nil {
		nlog.Errorln("FATAL:", fatalErr, "WRITE:", writeErr)
		return
	}

	avail, _ := fs.Get()
	if len(avail) < 1 {
		nlog.Errorln(cmn.ErrNoMountpaths)
		return
	}

	xctn := xreg.RenewResilver(args.UUID, &args.Custom)
	xres, ok := xctn.(*xs.Resilver)
	debug.Assert(ok)
	if args.Notif != nil {
		args.Notif.Xact = xres
		xres.AddNotif(args.Notif)
	}

	// jogger group
	var (
		jg        *mpather.Jgroup
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
		jctx      = &joggerCtx{xres: xres}
		opts      = &mpather.JgroupOpts{
			CTs:      []string{fs.ObjCT, fs.ECSliceCT},
			VisitObj: jctx.visitObj,
			VisitCT:  jctx.visitCT,
			Slab:     slab,
		}
	)
	debug.AssertNoErr(err)
	debug.Assert(args.PostDD == nil || (args.Action == apc.ActMountpathDetach || args.Action == apc.ActMountpathDisable))

	if args.SingleRmiJogger {
		jg = mpather.NewJoggerGroup(opts, args.Custom.Config, args.Rmi)
		nlog.Infof("%s, action %q, jogger->(%q)", xres.Name(), args.Action, args.Rmi)
	} else {
		jg = mpather.NewJoggerGroup(opts, args.Custom.Config, nil)
		if args.Rmi != nil {
			nlog.Infof("%s, action %q, rmi %s, num %d", xres.Name(), args.Action, args.Rmi, jg.NumJ())
		} else {
			nlog.Infof("%s, num %d", xres.Name(), jg.NumJ())
		}
	}

	// run and block waiting
	res.end.Store(0)
	jg.Run()
	err = wait(jg, xres, tstats)
	if err != nil {
		xres.AddErr(err)
	}
	// callback to, finally, detach-disable
	if args.PostDD != nil {
		args.PostDD(args.Rmi, args.Action, xres, err)
	}
	xres.Finish()
}

// Wait for an abort or for resilvering joggers to finish.
func wait(jg *mpather.Jgroup, xres *xs.Resilver, tstats cos.StatsUpdater) (err error) {
	for {
		select {
		case errCause := <-xres.ChanAbort():
			if err = jg.Stop(); err != nil {
				xres.AddErr(err, 0)
			} else {
				nlog.Infoln(core.T.String()+":", xres.Name(), "aborted, cause:", errCause)
			}
			return cmn.NewErrAborted(xres.Name(), "", errCause)
		case <-jg.ListenFinished():
			if err = fs.RemoveMarker(fname.ResilverMarker, tstats); err == nil {
				nlog.Infoln(core.T.String()+":", xres.Name(), "removed marker ok")
			}
			return
		}
	}
}

// Copies a slice and its metafile (if exists) to the current mpath. At the
// end does proper cleanup: removes ether source files(on success), or
// destination files(on copy failure)
func (jg *joggerCtx) _mvSlice(ct *core.CT, buf []byte) {
	uname := ct.Bck().MakeUname(ct.ObjectName())
	destMpath, _, err := fs.Hrw(uname)
	if err != nil {
		jg.xres.AddErr(err)
		nlog.Infoln("Warning:", err)
		return
	}
	if destMpath.Path == ct.Mountpath().Path {
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bucket(), fs.ECSliceCT, ct.ObjectName())
	srcMetaFQN, destMetaFQN, err := _moveECMeta(ct, ct.Mountpath(), destMpath, buf)
	if err != nil {
		jg.xres.AddErr(err)
		return
	}
	// Slice without metafile - skip it as unusable, let LRU clean it up
	if srcMetaFQN == "" {
		return
	}
	if cmn.Rom.FastV(4, cos.SmoduleReb) {
		nlog.Infof("%s: moving %q -> %q", core.T, ct.FQN(), destFQN)
	}
	if _, _, err = cos.CopyFile(ct.FQN(), destFQN, buf, cos.ChecksumNone); err != nil {
		errV := fmt.Errorf("failed to copy %q -> %q: %v. Rolling back", ct.FQN(), destFQN, err)
		jg.xres.AddErr(errV, 0)
		if err = cos.RemoveFile(destMetaFQN); err != nil {
			errV := fmt.Errorf("failed to cleanup metafile %q: %v", destMetaFQN, err)
			nlog.Infoln("Warning:", errV)
			jg.xres.AddErr(errV)
		}
	}
	if errMeta := cos.RemoveFile(srcMetaFQN); errMeta != nil {
		nlog.Warningln("failed to cleanup meta", srcMetaFQN, "[", errMeta, "]")
	}
	if errSlice := cos.RemoveFile(ct.FQN()); errSlice != nil {
		nlog.Warningln("failed to cleanup slice", ct.FQN(), "[", errSlice, "]")
	}
}

// Copies EC metafile to correct mpath. It returns FQNs of the source and
// destination for a caller to do proper cleanup. Empty values means: either
// the source FQN does not exist(err==nil), or copying failed
func _moveECMeta(ct *core.CT, srcMpath, dstMpath *fs.Mountpath, buf []byte) (string, string, error) {
	src := srcMpath.MakePathFQN(ct.Bucket(), fs.ECMetaCT, ct.ObjectName())
	// If metafile does not exist it may mean that EC has not processed the
	// object yet (e.g, EC was enabled after the bucket was filled), or
	// the metafile has gone
	if err := cos.Stat(src); cos.IsNotExist(err) {
		return "", "", nil
	}
	dst := dstMpath.MakePathFQN(ct.Bucket(), fs.ECMetaCT, ct.ObjectName())
	_, _, err := cos.CopyFile(src, dst, buf, cos.ChecksumNone)
	if err == nil {
		return src, dst, nil
	}
	if cos.IsNotExist(err) {
		err = nil
	}
	return "", "", err
}

// TODO: revisit EC bits and check for OOS preemptively
// NOTE: not deleting extra copies - delegating to `storage cleanup`
func (jg *joggerCtx) visitObj(lom *core.LOM, buf []byte) (errHrw error) {
	const (
		maxRetries = 3
	)
	var (
		orig   = lom
		hlom   *core.LOM
		xname  = jg.xres.Name()
		size   int64
		copied bool
	)

	if jg.xres.Args.SkipGlobMisplaced {
		tsi, err := jg.xres.Args.Smap.HrwHash2T(lom.Digest())
		if err != nil {
			return err
		}
		if tsi.ID() != core.T.SID() {
			return nil
		}
	}

	if !lom.TryLock(true) { // NOTE: skipping busy
		time.Sleep(time.Second >> 1)
		if !lom.TryLock(true) {
			return nil
		}
	}
	// cleanup
	defer func() {
		lom = orig
		lom.Unlock(true)
		if copied && errHrw == nil {
			jg.xres.ObjsAdd(1, size)
		}
	}()

	// 1. fix EC metafile
	var metaOldPath, metaNewPath string
	if !lom.IsHRW() && lom.ECEnabled() {
		var parsed fs.ParsedFQN
		_, err := core.ResolveFQN(*lom.HrwFQN, &parsed)
		if err != nil {
			nlog.Warningf("%s: %s %v", xname, lom, err)
			return nil
		}
		ct := core.NewCTFromLOM(lom, fs.ObjCT)
		// copy metafile
		metaOldPath, metaNewPath, err = _moveECMeta(ct, lom.Mountpath(), parsed.Mountpath, buf)
		if err != nil {
			nlog.Warningf("%s: failed to copy EC metafile %s %q -> %q: %v", xname, lom, lom.Mountpath().Path,
				parsed.Mountpath.Path, err)
			return nil
		}
	}

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil
	}

	size = lom.Lsize()
	// 2. fix hrw location; fail and subsequently abort if unsuccessful
	var (
		retries int
	)
redo:
	mi, isHrw := lom.ToMpath()
	if mi == nil {
		goto ret
	}

	if isHrw {
		hlom, errHrw = jg.fixHrw(lom, mi, buf)
		if errHrw != nil {
			if !cos.IsNotExist(errHrw) && !cos.IsErrNotFound(errHrw) {
				errV := fmt.Errorf("%s: failed to restore %s, errHrw: %v", xname, lom, errHrw)
				jg.xres.AddErr(errV, 0)
			}
			// EC cleanup and return
			if metaNewPath != "" {
				if errHrw = os.Remove(metaNewPath); errHrw != nil {
					errV := fmt.Errorf("%s: nested (%s %s: %v)", xname, lom, metaNewPath, errHrw)
					nlog.Infoln("Warning:", errV)
					jg.xres.AddErr(errV, 0)
				}
			}
			return errHrw
		}
		lom = hlom
		copied = true
	}

	// 3. fix copies
outer:
	for {
		// NOTE: do NOT shadow mi/isHrw; they are re-used at 'redo:'.
		mi, isHrw = lom.ToMpath()
		if mi == nil {
			break
		}
		if isHrw {
			// redo hlom in an unlikely event
			retries++
			if retries > maxRetries {
				hmi := "???"
				if hlom != nil && hlom.Mountpath() != nil {
					hmi = hlom.Mountpath().String()
				}
				errHrw = fmt.Errorf("%s: hrw mountpaths keep changing (%s(%s) => %s => %s ...)",
					xname, orig, orig.Mountpath(), hmi, mi)
				jg.xres.AddErr(errHrw, 0)
				return errHrw
			}
			copied = false
			lom, hlom = orig, nil
			time.Sleep(cmn.Rom.CplaneOperation() / 2)
			goto redo
		}
		err := lom.Copy(mi, buf)
		switch {
		case err == nil:
			copied = true
		case cos.IsErrOOS(err):
			errV := fmt.Errorf("%s: %s OOS, err: %w", core.T, mi, err)
			err = cmn.NewErrAborted(xname, "", errV)
			jg.xres.Abort(err)
			break outer
		case !cos.IsNotExist(err) && !cos.IsErrNotFound(err):
			errV := fmt.Errorf("%s: failed to copy %s to %s, err: %w", xname, lom, mi, err)
			nlog.Infoln("Warning:", errV)
			jg.xres.AddErr(errV)
			break outer
		default:
			errV := fmt.Errorf("%s: failed to copy %s to %s, err: %w", xname, lom, mi, err)
			jg.xres.AddErr(errV)
		}
	}
ret:
	// EC: remove old metafile
	if metaOldPath != "" {
		if err := os.Remove(metaOldPath); err != nil {
			nlog.Warningf("%s: failed to cleanup %s old metafile %q: %v", xname, lom, metaOldPath, err)
		}
	}
	return nil
}

func (*joggerCtx) fixHrw(lom *core.LOM, mi *fs.Mountpath, buf []byte) (hlom *core.LOM, err error) {
	if err = lom.Copy(mi, buf); err != nil {
		return
	}
	hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
	hlom = &core.LOM{}
	if err = hlom.InitFQN(hrwFQN, lom.Bucket()); err != nil {
		return
	}
	debug.Assert(hlom.Mountpath().Path == mi.Path)

	// reload; cache iff write-policy != immediate
	err = hlom.Load(!hlom.WritePolicy().IsImmediate() /*cache it*/, true /*locked*/)
	return
}

func (jg *joggerCtx) visitCT(ct *core.CT, buf []byte) (err error) {
	debug.Assert(ct.ContentType() == fs.ECSliceCT)
	if !ct.Bck().Props.EC.Enabled {
		// Since `%ec` directory is inside a bucket, it is safe to skip
		// the entire `%ec` directory when EC is disabled for the bucket.
		return filepath.SkipDir
	}
	jg._mvSlice(ct, buf)
	return nil
}
