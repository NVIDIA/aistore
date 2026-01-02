// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

const (
	ivalInactive = 4 * time.Second
)

type (
	Res struct {
		xres   *xs.Resilver
		jgroup *mpather.Jgroup
		end    atomic.Int64
		nbusy  atomic.Int64
		mu     sync.Mutex
	}
	Args struct {
		UUID            string
		Notif           *xact.NotifXact
		Rmi             *fs.Mountpath
		WG              *sync.WaitGroup
		Action          string
		PostDD          func(rmi *fs.Mountpath, action string, xres *xs.Resilver, err error)
		Custom          xreg.ResArgs
		SingleRmiJogger bool
	}
	jogger struct {
		p    *Res
		xres *xs.Resilver
	}
)

func New() *Res { return &Res{} }

func (res *Res) IsActive(multiplier int64) bool {
	xres := res.GetXact()
	if xres != nil {
		if xres.IsAborted() {
			return false
		}
		if !xres.IsDone() {
			return true
		}
	}
	end := res.end.Load()
	if end == 0 || multiplier == 0 {
		return false
	}
	return mono.Since(end) < time.Duration(multiplier)*ivalInactive
}

func (res *Res) GetXact() *xs.Resilver {
	res.mu.Lock()
	xres := res.xres
	res.mu.Unlock()
	return xres
}

func (res *Res) Abort(err error) (aborted bool) {
	res.mu.Lock()
	xres := res.xres
	jgroup := res.jgroup
	if xres != nil {
		res.jgroup = nil
	}
	res.mu.Unlock()
	if xres == nil {
		return false
	}
	aborted = xres.Abort(err)
	if !aborted {
		return false
	}

	if jgroup != nil {
		_ = jgroup.Stop()
	}
	return true
}

func (res *Res) Run(args *Args, tstats cos.StatsUpdater) {
	avail, _ := fs.Get()
	if len(avail) < 1 {
		if args.WG != nil {
			args.WG.Done()
		}
		nlog.Errorln(cmn.ErrNoMountpaths)
		return
	}

	xres := res.initRenew(args)
	if args.WG != nil {
		args.WG.Done()
	}
	if xres == nil {
		return
	}

	// jgroup
	var (
		jgroup    *mpather.Jgroup
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize)
		j         = &jogger{p: res, xres: xres}
		opts      = &mpather.JgroupOpts{
			CTs:      []string{fs.ObjCT, fs.ECSliceCT},
			VisitObj: j.visitObj,
			VisitCT:  j.visitECSlice,
			Slab:     slab,
			RW:       true,
		}
	)
	debug.AssertNoErr(err)
	debug.Assert(args.PostDD == nil || (args.Action == apc.ActMountpathDetach || args.Action == apc.ActMountpathDisable))

	if args.SingleRmiJogger {
		jgroup = mpather.NewJgroup(opts, args.Custom.Config, args.Rmi)
		nlog.Infof("%s, action %q, jogger->(%q)", xres.Name(), args.Action, args.Rmi)
	} else {
		jgroup = mpather.NewJgroup(opts, args.Custom.Config, nil)
		if args.Rmi != nil {
			nlog.Infof("%s, action %q, rmi %s, num %d", xres.Name(), args.Action, args.Rmi, jgroup.NumJ())
		} else {
			nlog.Infof("%s, num %d", xres.Name(), jgroup.NumJ())
		}
	}

	res.mu.Lock() // 2nd --------------------------------------
	if xres != res.xres {
		// (unlikely)
		res.mu.Unlock()
		_ = jgroup.Stop()
		return
	}
	res.end.Store(0)
	res.jgroup = jgroup
	res.mu.Unlock() //  --------------------------------------

	// run and block waiting
	jgroup.Run()
	res.wait(jgroup, xres, tstats)

	// callback to, finally, detach-disable
	if args.PostDD != nil {
		args.PostDD(args.Rmi, args.Action, xres, err)
	}

	xres.Finish()

	res.mu.Lock()
	if xres == res.xres {
		res.end.Store(mono.NanoTime())
		res.xres = nil
		res.jgroup = nil
	}
	res.mu.Unlock()

	if n := res.nbusy.Load(); n > 0 {
		nlog.Warningf("%s done (skipped %d busy)", xres.Name(), n)
	}
}

func (res *Res) initRenew(args *Args) *xs.Resilver {
	rns := xreg.RenewResilver(args.UUID, &args.Custom)
	if rns.Err != nil {
		debug.Assertf(!cmn.IsErrXactUsePrev(rns.Err), "not expecting %v(%T)", rns.Err, rns.Err)
		nlog.Errorln("failed to start", args.Action, "renewal err:", rns.Err)
		return nil
	}
	xctn := rns.Entry.Get()
	xres := xctn.(*xs.Resilver)
	if rns.IsRunning() {
		nlog.Warningf("%s: start/preempt race [%q, %s]", args.Action, args.UUID, xres.Name())
		return nil
	}

	debug.Func(func() {
		if r := res.GetXact(); r != nil {
			debug.Assertf(r.IsDone() || r.IsAborted(), "%s: (done=%t, aborted=%t)", r, r.IsDone(), r.IsAborted())
		}
	})
	debug.Assertf(xres.ID() == args.UUID, "res-id mismatch: %q vs %q", xres.Name(), args.UUID)

	res.mu.Lock() // 1st --------------------------------------

	fatalErr, warnErr := fs.PersistMarker(fname.ResilverMarker, true /*quiet*/)
	if fatalErr != nil {
		xres.Abort(fatalErr)
		res.mu.Unlock()
		return nil
	}
	res.xres = xres

	res.mu.Unlock() // ------------------------------------

	if warnErr != nil {
		nlog.Warningln(warnErr)
	}
	if args.Notif != nil {
		args.Notif.Xact = xres
		xres.AddNotif(args.Notif)
	}
	return xres
}

// Wait for an abort or for resilvering joggers to finish.
func (res *Res) wait(jgroup *mpather.Jgroup, xres *xs.Resilver, tstats cos.StatsUpdater) {
	for {
		select {
		case errCause := <-xres.ChanAbort():
			err := jgroup.Stop()
			res.mu.Lock()
			if res.jgroup == jgroup {
				res.jgroup = nil
			}
			res.mu.Unlock()
			if !xres.IsAborted() {
				e := cos.Ternary(errCause == nil, err, errCause)
				xres.Abort(e)
				nlog.Warningln(xres.Name(), "stopped: [", errCause, err, "]")
			}
			return
		case <-jgroup.ListenFinished():
			if fs.RemoveMarker(fname.ResilverMarker, tstats, false /*stopping*/) {
				nlog.Infoln(core.T.String()+":", xres.Name(), "removed marker ok")
			}
			return
		}
	}
}

// Copies EC metafile to correct mpath. It returns FQNs of the source and
// destination for a caller to do proper cleanup. Empty values means: either
// the source FQN does not exist(err==nil), or copying failed
func _cpECMeta(ct *core.CT, srcMpath, dstMpath *fs.Mountpath, buf []byte) (string, string, error) {
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
func (j *jogger) visitObj(lom *core.LOM, buf []byte) (errHrw error) {
	const (
		maxRetries = 3
	)
	var (
		orig   = lom
		hlom   *core.LOM
		xname  = j.xres.Name()
		size   int64
		copied bool
	)

	if j.xres.IsAborted() {
		return nil
	}

	if !lom.TryLock(true) { // NOTE: skipping busy
		j.p.nbusy.Inc()
		if cmn.Rom.V(4, cos.ModReb) {
			nlog.Warningln("skipping busy:", lom.Cname())
		}
		return nil
	}

	// cleanup
	defer func() {
		lom = orig
		lom.Unlock(true)
		if copied && errHrw == nil {
			j.xres.ObjsAdd(1, size)
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
		metaOldPath, metaNewPath, err = _cpECMeta(ct, lom.Mountpath(), parsed.Mountpath, buf)
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
	mi, fixHrw := lom.ToMpath()
	if mi == nil {
		goto ret
	}

	if fixHrw {
		hlom, errHrw = j.fixHrw(lom, mi, buf)
		if errHrw != nil {
			if !cos.IsNotExist(errHrw) && !cos.IsErrNotFound(errHrw) {
				errV := fmt.Errorf("%s: failed to restore %s, errHrw: %v", xname, lom, errHrw)
				j.xres.AddErr(errV, 0)
			}
			// EC cleanup and return
			if metaNewPath != "" {
				if errHrw = os.Remove(metaNewPath); errHrw != nil {
					errV := fmt.Errorf("%s: nested (%s %s: %v)", xname, lom, metaNewPath, errHrw)
					nlog.Infoln("Warning:", errV)
					j.xres.AddErr(errV, 0)
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
		// NOTE: do NOT shadow mi/fixHrw; they are re-used at 'redo:'.
		mi, fixHrw = lom.ToMpath()
		if mi == nil {
			break
		}
		if fixHrw {
			// redo hlom in an unlikely event
			retries++
			if retries > maxRetries {
				hmi := "???"
				if hlom != nil && hlom.Mountpath() != nil {
					hmi = hlom.Mountpath().String()
				}
				errHrw = fmt.Errorf("%s: hrw mountpaths keep changing (%s(%s) => %s => %s ...)",
					xname, orig, orig.Mountpath(), hmi, mi)
				j.xres.AddErr(errHrw, 0)
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
			j.xres.Abort(err)
			break outer
		case !cos.IsNotExist(err) && !cos.IsErrNotFound(err):
			errV := fmt.Errorf("%s: failed to copy %s to %s, err: %w", xname, lom, mi, err)
			nlog.Infoln("Warning:", errV)
			j.xres.AddErr(errV)
			break outer
		default:
			errV := fmt.Errorf("%s: failed to copy %s to %s, err: %w", xname, lom, mi, err)
			j.xres.AddErr(errV)
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

func (*jogger) fixHrw(lom *core.LOM, mi *fs.Mountpath, buf []byte) (hlom *core.LOM, err error) {
	debug.Assertf(lom.IsLocked() == apc.LockWrite, "%s must be w-locked (have %d)", lom.Cname(), lom.IsLocked())

	if lom.IsChunked() {
		u, err := core.NewUfest("", lom, true)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ufest: %w", err)
		}
		if err := u.LoadCompleted(lom); err != nil {
			return nil, err
		}
		return u.Relocate(mi, buf)
	}

	// regular objects use the regular Copy method
	if err = lom.Copy(mi, buf); err != nil {
		return nil, err
	}
	hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
	hlom = &core.LOM{}
	if err = hlom.InitFQN(hrwFQN, lom.Bucket()); err != nil {
		return nil, err
	}
	debug.Assert(hlom.Mountpath().Path == mi.Path)

	// reload; cache iff write-policy != immediate
	err = hlom.Load(!hlom.WritePolicy().IsImmediate() /*cache it*/, true /*locked*/)
	return hlom, err
}

func (j *jogger) visitECSlice(ct *core.CT, buf []byte) (err error) {
	debug.Assert(ct.ContentType() == fs.ECSliceCT)
	if !ct.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}
	if j.xres.IsAborted() {
		return nil
	}
	j._mvSlice(ct, buf)
	return nil
}

// Copies a slice and its metafile (if exists) to the current mpath. At the
// end does proper cleanup: removes either source files(on success), or
// destination files(on copy failure)
func (j *jogger) _mvSlice(ct *core.CT, buf []byte) {
	uname := ct.Bck().MakeUname(ct.ObjectName())
	destMpath, _, err := fs.Hrw(uname)
	if err != nil {
		j.xres.AddErr(err)
		nlog.Infoln("Warning:", err)
		return
	}
	if destMpath.Path == ct.Mountpath().Path {
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bucket(), fs.ECSliceCT, ct.ObjectName())
	srcMetaFQN, destMetaFQN, err := _cpECMeta(ct, ct.Mountpath(), destMpath, buf)
	if err != nil {
		j.xres.AddErr(err)
		return
	}
	// Slice without metafile - skip it as unusable, let LRU clean it up
	if srcMetaFQN == "" {
		return
	}
	if cmn.Rom.V(4, cos.ModReb) {
		nlog.Infof("%s: moving %q -> %q", core.T, ct.FQN(), destFQN)
	}
	if _, _, err = cos.CopyFile(ct.FQN(), destFQN, buf, cos.ChecksumNone); err != nil {
		errV := fmt.Errorf("failed to copy %q -> %q: %v. Rolling back", ct.FQN(), destFQN, err)
		j.xres.AddErr(errV, 0)
		if err = cos.RemoveFile(destMetaFQN); err != nil {
			errV := fmt.Errorf("failed to cleanup metafile %q: %v", destMetaFQN, err)
			nlog.Infoln("Warning:", errV)
			j.xres.AddErr(errV)
		}
	}
	if errMeta := cos.RemoveFile(srcMetaFQN); errMeta != nil {
		nlog.Warningln("failed to cleanup meta", srcMetaFQN, "[", errMeta, "]")
	}
	if errSlice := cos.RemoveFile(ct.FQN()); errSlice != nil {
		nlog.Warningln("failed to cleanup slice", ct.FQN(), "[", errSlice, "]")
	}
}
