// Package res provides local volume resilvering upon mountpath-attach and similar
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package res

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// TODO:
// - revisit EC bits and check for OOS preemptively
// - not deleting extra copies - delegating to `storage cleanup`
// - skipping _busy_ objects w/ TryLock + retries
// - non-immediate write policy (lom.WritePolicy().IsImmediate())

const (
	ivalInactive = 4 * time.Second
	busySleep    = 10 * time.Millisecond
	busyRetries  = 5
)

type (
	Res struct {
		xres *xs.Resilver
		end  atomic.Int64
		mu   sync.Mutex
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
		AdminAPI        bool
	}
	jogger struct {
		p     *Res
		xres  *xs.Resilver
		avail fs.MPI
		// work
		sentinel string
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
	res.mu.Unlock()
	if xres == nil {
		return false
	}
	return xres.Abort(err)
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
		j         = &jogger{p: res, xres: xres, avail: avail}
		opts      = &mpather.JgroupOpts{
			Parent:   xres,
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
	res.mu.Unlock() //  --------------------------------------

	// run and block waiting
	xres.SetJgroup(jgroup)
	jgroup.Run()
	wait(jgroup, xres, tstats)

	// callback to, finally, detach-disable
	if args.PostDD != nil {
		args.PostDD(args.Rmi, args.Action, xres, err)
	}

	xres.Finish()

	res.mu.Lock()
	if xres == res.xres {
		res.end.Store(mono.NanoTime())
		res.xres = nil
	}
	res.mu.Unlock()

	s := "num objects visited: " + strconv.FormatInt(jgroup.NumVisits(), 10)
	if n := xres.Nbusy.Load(); n > 0 {
		nlog.Warningf("%s done [%s, skipped busy: %d]", xres.Name(), s, n)
	} else {
		nlog.Infof("%s done [%s]", xres.Name(), s)
	}

	xres.SetJgroup(nil)
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
func wait(jgroup *mpather.Jgroup, xres *xs.Resilver, tstats cos.StatsUpdater) {
	for {
		select {
		case <-xres.ChanAbort():
			_ = jgroup.Stop()
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
func _cpECMeta(lom *core.LOM, srcMpath, dstMpath *fs.Mountpath, buf []byte) (string, string, error) {
	src := srcMpath.MakePathFQN(lom.Bucket(), fs.ECMetaCT, lom.ObjName)
	// If metafile does not exist it may mean that EC has not processed the
	// object yet (e.g, EC was enabled after the bucket was filled), or
	// the metafile has gone
	if err := cos.Stat(src); cos.IsNotExist(err) {
		return "", "", nil
	}
	dst := dstMpath.MakePathFQN(lom.Bucket(), fs.ECMetaCT, lom.ObjName)
	_, _, err := cos.CopyFile(src, dst, buf, cos.ChecksumNone)
	if err == nil {
		return src, dst, nil
	}
	if cos.IsNotExist(err) {
		err = nil
	}
	return "", "", err
}

func (j *jogger) lock(lom *core.LOM) (locked bool) {
	if lom.TryLock(true) {
		return true
	}

	shouldRetry := lom.IsHRW()
	if !shouldRetry {
		mirror := lom.MirrorConf()
		shouldRetry = mirror.Enabled && mirror.Copies >= 2
	}
	if !shouldRetry {
		goto busy
	}

	for range busyRetries {
		if j.xres.IsAborted() {
			return false
		}
		time.Sleep(busySleep)
		if lom.TryLock(true) {
			return true
		}
	}
busy:
	if lom.IsHRW() {
		j.xres.Nbusy.Inc()
		if cmn.Rom.V(4, cos.ModReb) {
			nlog.Warningln("skipping busy:", lom.Cname())
		}
	}
	return false
}

func (j *jogger) visitObj(lom *core.LOM, buf []byte) (errHrw error) {
	var (
		orig  = lom
		hlom  *core.LOM
		xname = j.xres.Name()
		size  int64
	)

	if j.xres.IsAborted() {
		return nil
	}

	if !j.lock(lom) {
		return nil
	}

	// cleanup
	defer func() {
		lom = orig
		lom.Unlock(true)
	}()

	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return nil
	}

	// fix EC metafile
	var metaOldPath, metaNewPath string
	if lom.ECEnabled() {
		if hrwMi, ok := lom.Hrw(j.avail); !ok {
			// copy metafile
			var err error
			metaOldPath, metaNewPath, err = _cpECMeta(lom, lom.Mountpath(), hrwMi, buf)
			if err != nil {
				j.xres.AddErr(err)
				return nil
			}
		}
	}

	if lom.IsCopy() {
		if j.sentinel == "" {
			const (
				maxRune = "\U0010FFFF" // max Unicode value
			)
			j.sentinel = strings.Repeat(maxRune, 4)
		}
		return j.visitCopy(lom, buf)
	}

	size = lom.Lsize()
	mi, ok := lom.HrwWithChunks(j.avail)
	if mi == nil {
		goto ret
	}
	if !ok {
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
		j.xres.ObjsAdd(1, size)
	} else {
		hlom = lom
	}

	if err := j.fixCopies(hlom, buf); err != nil {
		return err
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

// 'mi' here is HRW mountpath (and the copying destination) under current (avail) volume
func (*jogger) fixHrw(lom *core.LOM, mi *fs.Mountpath, buf []byte) (hlom *core.LOM, _ error) {
	debug.Assertf(lom.IsLocked() == apc.LockWrite, "%s must be w-locked (have %d)", lom.Cname(), lom.IsLocked())

	if lom.IsChunked() {
		u, err := core.NewUfest("", lom, true)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ufest: %w", err)
		}
		if err := u.LoadCompleted(lom); err != nil {
			return nil, err
		}

		/* NOTE: alternatively, copy all chunks instead of relocating
		   hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
		   hlom, err = lom.Copy2FQN(hrwFQN, buf)
		   if err != nil {
		           return nil, err
		   }
		   return hlom, nil
		*/

		return u.Relocate(mi, buf)
	}

	// regular objects use the regular Copy method
	if err := lom.Copy(mi, buf); err != nil {
		return nil, err
	}

	return _loadHlom(lom, mi)
}

func _loadHlom(lom *core.LOM, mi *fs.Mountpath) (hlom *core.LOM, err error) {
	hrwFQN := mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
	hlom = &core.LOM{}
	if err = hlom.InitFQN(hrwFQN, lom.Bucket()); err != nil {
		return nil, err
	}
	debug.Assert(hlom.Mountpath().Path == mi.Path)

	// reload; cache iff write-policy != immediate
	err = hlom.Load(false, true /*locked*/)
	return hlom, err
}

func (j *jogger) visitCopy(lom *core.LOM, buf []byte) error {
	mi, ok := lom.HrwWithChunks(j.avail)
	if mi == nil || ok {
		return nil // nothing to do
	}
	isPrimary, mainExists := lom.IsPrimaryCopy(j.avail, mi, j.sentinel)
	if !isPrimary || mainExists {
		return nil
	}

	// this jogger owns canonical copy - use it to restore main replica
	hlom, err := j.fixHrw(lom, mi, buf)
	if err != nil {
		j.xres.AddErr(err)
		return err
	}
	debug.Assert(hlom.Mountpath().Path == mi.Path)
	j.xres.ObjsAdd(1, lom.Lsize())

	// this same designated jogger goes ahead to restore copies
	return j.fixCopies(hlom, buf)
}

func (j *jogger) fixCopies(hlom *core.LOM, buf []byte) (abortErr error) {
	const (
		fmterr = "failed to copy %s to %s, err: %v"
	)
	exp, got := hlom.CleanupCopies(j.avail)
	for got < exp {
		mi := hlom.LeastUtilNoCopy()
		if mi == nil {
			return nil // not enough mountpaths
		}
		err := hlom.Copy(mi, buf)
		switch {
		case err == nil:
			got++
		case cos.IsErrOOS(err):
			j.xres.Abort(err)
			return err
		case !cos.IsNotExist(err) && !cos.IsErrNotFound(err):
			errV := fmt.Errorf(fmterr, hlom.Cname(), mi, err)
			nlog.Infoln("Warning:", errV)
			j.xres.AddErr(errV)
		default:
			errV := fmt.Errorf(fmterr, hlom.Cname(), mi, err)
			j.xres.AddErr(errV)
		}
	}
	return nil
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
		j.xres.AddErr(err, 0)
		return
	}
	if destMpath.Path == ct.Mountpath().Path {
		return
	}
	dstMeta := destMpath.MakePathFQN(ct.Bucket(), fs.ECMetaCT, ct.ObjectName())
	// Slice without metafile - skip it as unusable, let LRU clean it up
	if err := cos.Stat(dstMeta); err != nil {
		if !cos.IsNotExist(err) {
			j.xres.AddErr(err)
		}
		return
	}

	destFQN := destMpath.MakePathFQN(ct.Bucket(), fs.ECSliceCT, ct.ObjectName())
	if cmn.Rom.V(4, cos.ModReb) {
		nlog.Infof("%s: moving %q -> %q", core.T, ct.FQN(), destFQN)
	}
	if _, _, err = cos.CopyFile(ct.FQN(), destFQN, buf, cos.ChecksumNone); err != nil {
		errV := fmt.Errorf("failed to copy %q -> %q: %v. Rolling back", ct.FQN(), destFQN, err)
		j.xres.AddErr(errV, 0)
	}
	if errSlice := cos.RemoveFile(ct.FQN()); errSlice != nil {
		nlog.Warningln("failed to cleanup slice", ct.FQN(), "[", errSlice, "]")
	}
}
