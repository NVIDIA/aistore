// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	IniCln struct {
		StatsT  stats.Tracker
		Config  *cmn.Config
		Xaction *XactCln
		WG      *sync.WaitGroup
		Args    *xact.ArgsMsg
	}
	XactCln struct {
		xact.Base
	}
)

// private
type (
	// parent (contains mpath joggers)
	clnP struct {
		wg      sync.WaitGroup
		joggers map[string]*clnJ
		ini     IniCln
		cs      struct {
			a fs.CapStatus // initial
			b fs.CapStatus // capacity after removing 'deleted'
			c fs.CapStatus // upon finishing
		}
		jcnt atomic.Int32
	}
	// clnJ represents a single cleanup context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	clnJ struct {
		// runtime
		oldWork   []string
		misplaced struct {
			loms []*core.LOM
			ec   []*core.CT // EC slices and replicas without corresponding metafiles (CT FQN -> Meta FQN)
		}
		bck cmn.Bck
		now int64
		// init-time
		p       *clnP
		ini     *IniCln
		stopCh  chan struct{}
		joggers map[string]*clnJ
		mi      *fs.Mountpath
		config  *cmn.Config
	}
	clnFactory struct {
		xreg.RenewBase
		xctn *XactCln
	}
)

// interface guard
var (
	_ xreg.Renewable = (*clnFactory)(nil)
	_ core.Xact      = (*XactCln)(nil)
)

func (*XactCln) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactCln) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

////////////////
// clnFactory //
////////////////

func (*clnFactory) New(args xreg.Args, _ *meta.Bck) xreg.Renewable {
	return &clnFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *clnFactory) Start() error {
	p.xctn = &XactCln{}
	p.xctn.InitBase(p.UUID(), apc.ActStoreCleanup, nil)
	return nil
}

func (*clnFactory) Kind() string     { return apc.ActStoreCleanup }
func (p *clnFactory) Get() core.Xact { return p.xctn }

func (*clnFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	return xreg.WprUse, cmn.NewErrXactUsePrev(prevEntry.Get().String())
}

func RunCleanup(ini *IniCln) fs.CapStatus {
	var (
		xcln    = ini.Xaction
		config  = cmn.GCO.Get()
		avail   = fs.GetAvail()
		num     = len(avail)
		joggers = make(map[string]*clnJ, num)
		parent  = &clnP{joggers: joggers, ini: *ini}
	)
	defer func() {
		if ini.WG != nil {
			ini.WG.Done()
		}
	}()
	if num == 0 {
		xcln.AddErr(cmn.ErrNoMountpaths, 0)
		xcln.Finish()
		return fs.CapStatus{}
	}
	now := time.Now().UnixNano()
	for mpath, mi := range avail {
		joggers[mpath] = &clnJ{
			oldWork: make([]string, 0, 64),
			stopCh:  make(chan struct{}, 1),
			mi:      mi,
			config:  config,
			ini:     &parent.ini,
			p:       parent,
			now:     now,
		}
		joggers[mpath].misplaced.loms = make([]*core.LOM, 0, 64)
		joggers[mpath].misplaced.ec = make([]*core.CT, 0, 64)
	}
	parent.jcnt.Store(int32(len(joggers)))
	providers := apc.Providers.ToSlice()
	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go j.run(providers)
	}

	parent.cs.a = fs.Cap()
	nlog.Infoln(xcln.Name(), "started: ", xcln, parent.cs.a.String())
	if ini.WG != nil {
		ini.WG.Done()
		ini.WG = nil
	}
	parent.wg.Wait()

	for _, j := range joggers {
		j.stop()
	}

	var err, errCap error
	parent.cs.c, err, errCap = fs.CapRefresh(config, nil /*tcdf*/)
	if err != nil {
		xcln.AddErr(err)
	}
	if errCap != nil {
		xcln.AddErr(errCap)
	}
	xcln.Finish()
	nlog.Infoln(xcln.Name(), "finished:", errCap)

	return parent.cs.c
}

// check other conditions (other than too-early) prior to going ahead to remove misplaced
func (p *clnP) rmMisplaced() bool {
	var (
		g = xreg.GetRebMarked()
		l = xreg.GetResilverMarked()
	)
	if g.Xact == nil && l.Xact == nil && !g.Interrupted && !g.Restarted && !l.Interrupted {
		return true
	}

	// force(?) and log
	var (
		why   string
		flog  = nlog.Warningln
		cserr = p.cs.a.Err()
		ok    bool
	)
	switch {
	case g.Xact != nil:
		why = g.Xact.String() + " is running"
	case g.Interrupted:
		why = "rebalance interrupted"
		ok = p.ini.Args.Force
	case g.Restarted:
		why = "node restarted"
		ok = p.ini.Args.Force
	case l.Xact != nil:
		why = l.Xact.String() + " is running"
	case l.Interrupted:
		why = "resilver interrupted"
	}
	if cserr != nil {
		flog = nlog.Errorln
	}
	if ok {
		flog(core.T.String(), p.ini.Xaction.String(), "proceeding to remove misplaced obj-s with force, ignoring: [", cserr, why, "]")
	} else {
		flog(core.T.String(), p.ini.Xaction.String(), "not removing misplaced obj-s: [", cserr, why, "]")
	}
	return ok
}

//////////
// clnJ //
//////////

// mountpath cleanup j

func (j *clnJ) String() string {
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString(j.ini.Xaction.String())
	sb.WriteString(": jog-")
	sb.WriteString(j.mi.String())
	if j.ini.Args.Force {
		sb.WriteString("-with-force")
	}
	return sb.String()
}

func (j *clnJ) stop() { j.stopCh <- struct{}{} }

func (j *clnJ) run(providers []string) {
	const f = "%s: freed space %s (not including removed 'deleted')"
	var (
		size     int64
		err, erm error
	)
	// globally
	erm = j.removeDeleted()
	if erm != nil {
		nlog.Errorln(erm)
	}

	// traverse
	if len(j.ini.Args.Buckets) != 0 {
		size, err = j.jogBcks(j.ini.Args.Buckets)
	} else {
		size, err = j.jog(providers)
	}
	if err == nil {
		err = erm
	}
	if err == nil {
		if size != 0 {
			nlog.Infof(f, j, cos.ToSizeIEC(size, 1))
		}
	} else {
		nlog.Errorf(f+", err: %v", j, cos.ToSizeIEC(size, 1), err)
	}
	j.p.wg.Done()
}

func (j *clnJ) jog(providers []string) (size int64, rerr error) {
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
		var (
			sz   int64
			bcks []cmn.Bck
			err  error
			opts = fs.WalkOpts{Mi: j.mi, Bck: cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}}
		)
		if bcks, err = fs.AllMpathBcks(&opts); err != nil {
			nlog.Errorln(err)
			if rerr == nil {
				rerr = err
			}
			continue
		}
		if len(bcks) == 0 {
			continue
		}
		sz, err = j.jogBcks(bcks)
		size += sz
		if err != nil && rerr == nil {
			rerr = err
		}
	}
	return
}

func (j *clnJ) jogBcks(bcks []cmn.Bck) (size int64, rerr error) {
	bowner := core.T.Bowner()
	for i := range bcks { // for each bucket under a given provider
		var (
			err error
			sz  int64
			bck = bcks[i]
			b   = meta.CloneBck(&bck)
		)
		j.bck = bck
		err = b.Init(bowner)
		if err != nil {
			if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
				const act = "delete non-existing"
				if err = fs.DestroyBucket(act, &bck, 0 /*unknown BID*/); err == nil {
					nlog.Infof("%s: %s %s", j, act, bck)
				} else {
					j.ini.Xaction.AddErr(err)
					nlog.Errorf("%s %s: %v - skipping", j, act, err)
				}
			} else {
				// TODO: config option to scrub `fs.AllMpathBcks` buckets
				j.ini.Xaction.AddErr(err)
				nlog.Errorf("%s: %v - skipping %s", j, err, bck)
			}
			continue
		}
		sz, err = j.jogBck()
		size += sz
		if err != nil && rerr == nil {
			rerr = err
		}
	}
	return size, rerr
}

func (j *clnJ) removeDeleted() (err error) {
	err = j.mi.RemoveDeleted(j.String())
	if err != nil {
		j.ini.Xaction.AddErr(err)
	}
	if cnt := j.p.jcnt.Dec(); cnt > 0 {
		return
	}

	// last rm-deleted done: refresh cap now
	var errCap error
	j.p.cs.b, err, errCap = fs.CapRefresh(j.config, nil /*tcdf*/)
	if err != nil {
		j.ini.Xaction.Abort(err)
	} else {
		nlog.Infoln(j.ini.Xaction.Name(), "post-rm('deleted'):", errCap)
	}
	return
}

func (j *clnJ) jogBck() (size int64, err error) {
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		Bck:      j.bck,
		CTs:      []string{fs.WorkfileType, fs.ObjectType, fs.ECSliceType, fs.ECMetaType},
		Callback: j.walk,
		Sorted:   false,
	}
	if err = fs.Walk(opts); err != nil {
		return
	}
	size, err = j.rmLeftovers()
	return
}

func (j *clnJ) visitCT(parsedFQN *fs.ParsedFQN, fqn string) {
	switch parsedFQN.ContentType {
	case fs.WorkfileType:
		_, base := filepath.Split(fqn)
		contentResolver := fs.CSM.Resolver(fs.WorkfileType)
		_, old, ok := contentResolver.ParseUniqueFQN(base)
		// workfiles: remove old or do nothing
		if ok && old {
			j.oldWork = append(j.oldWork, fqn)
		}
	case fs.ECSliceType:
		// EC slices:
		// - EC enabled: remove only slices with missing metafiles
		// - EC disabled: remove all slices
		ct, err := core.NewCTFromFQN(fqn, core.T.Bowner())
		if err != nil || !ct.Bck().Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			return
		}
		if err := ct.LoadSliceFromFS(); err != nil {
			return
		}
		// Saving a CT is not atomic: first it saves CT, then its metafile
		// follows. Ignore just updated CTs to avoid processing incomplete data.
		if ct.MtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
			return
		}
		metaFQN := fs.CSM.Gen(ct, fs.ECMetaType, "")
		if cos.Stat(metaFQN) != nil {
			j.misplaced.ec = append(j.misplaced.ec, ct)
		}
	case fs.ECMetaType:
		// EC metafiles:
		// - EC enabled: remove only without corresponding slice or replica
		// - EC disabled: remove all metafiles
		ct, err := core.NewCTFromFQN(fqn, core.T.Bowner())
		if err != nil || !ct.Bck().Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			return
		}
		// Metafile is saved the last. If there is no corresponding replica or
		// slice, it is safe to remove the stray metafile.
		sliceCT := ct.Clone(fs.ECSliceType)
		if cos.Stat(sliceCT.FQN()) == nil {
			return
		}
		objCT := ct.Clone(fs.ObjectType)
		if cos.Stat(objCT.FQN()) == nil {
			return
		}
		j.oldWork = append(j.oldWork, fqn)
	default:
		debug.Assert(false, "Unsupported content type: ", parsedFQN.ContentType)
	}
}

// [TODO]
// - add stats error counters (stats.ErrLmetaCorruptedCount, ...)
// - revisit rm-ed byte counting
// - dry-run (feature) with all to-be removed listed
func (j *clnJ) visitObj(fqn string, lom *core.LOM) {
	if err := lom.InitFQN(fqn, &j.bck); err != nil {
		nlog.Errorln(j.String(), "unexpected object fqn", fqn, err)
		// TODO -- FIXME: consider applying force
		return
	}
	// handle load err
	if errLoad := lom.Load(false /*cache it*/, false /*locked*/); errLoad != nil {
		_, atimefs, _, err := lom.Fstat(true /*get-atime*/)
		if err != nil {
			if !os.IsNotExist(err) {
				err = os.NewSyscallError("stat", err)
				j.ini.Xaction.AddErr(err)
				core.T.FSHC(err, lom.Mountpath(), lom.FQN)
			}
			return
		}
		// too early to remove anything
		if atimefs+int64(j.config.LRU.DontEvictTime) > j.now {
			return
		}
		if cmn.IsErrLmetaCorrupted(errLoad) {
			if err := lom.RemoveMain(); err != nil {
				nlog.Errorf("%s: failed to rm MD-corrupted %s: %v (nested: %v)", j, lom, errLoad, err)
				j.ini.Xaction.AddErr(err)
			} else {
				nlog.Errorf("%s: removed MD-corrupted %s: %v", j, lom, errLoad)
			}
		} else if cmn.IsErrLmetaNotFound(errLoad) {
			if err := lom.RemoveMain(); err != nil {
				nlog.Errorf("%s: failed to rm no-MD %s: %v (nested: %v)", j, lom, errLoad, err)
				j.ini.Xaction.AddErr(err)
			} else {
				nlog.Errorf("%s: removed no-MD %s: %v", j, lom, errLoad)
			}
		}
		return
	}

	// TODO: switch
	// too early; NOTE: default dont-evict = 2h
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		if cmn.Rom.FastV(5, cos.SmoduleSpace) {
			nlog.Infoln("too early for", lom.String(), "atime", lom.Atime().String(), "dont-evict", j.config.LRU.DontEvictTime.D())
		}
		return
	}
	if lom.IsHRW() {
		if lom.HasCopies() {
			j.rmExtraCopies(lom)
		}
		if lom.Lsize() == 0 {
			if j.ini.Args.Flags&xact.XrmZeroSize == xact.XrmZeroSize {
				if ecode, err := core.T.DeleteObject(lom, false /*evict*/); err != nil {
					nlog.Errorln("failed to remove zero-size", lom.Cname(), "err:", err, "code:", ecode)
				} else {
					nlog.Warningln("removed zero-size", lom.Cname())
				}
			}
		}
		return
	}
	if lom.IsCopy() {
		// will be _visited_ separately (if not already)
		return
	}
	if lom.ECEnabled() {
		// misplaced EC
		metaFQN := fs.CSM.Gen(lom, fs.ECMetaType, "")
		if cos.Stat(metaFQN) != nil {
			j.misplaced.ec = append(j.misplaced.ec, core.NewCTFromLOM(lom, fs.ObjectType))
		}
	} else {
		// misplaced object
		lom = lom.CloneMD(lom.FQN)
		j.misplaced.loms = append(j.misplaced.loms, lom)
	}
}

func (j *clnJ) rmExtraCopies(lom *core.LOM) {
	if !lom.TryLock(true) {
		return // must be busy
	}
	defer lom.Unlock(true)

	// TODO: switch

	// reload under lock and check atime - again
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cos.IsNotExist(err, 0) {
			j.ini.Xaction.AddErr(err)
		}
		return
	}
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		return
	}
	if lom.IsCopy() {
		return // extremely unlikely but ok
	}
	if _, err := lom.DelExtraCopies(); err != nil {
		err = fmt.Errorf("%s: failed delete redundant copies of %s: %v", j, lom, err)
		j.ini.Xaction.AddErr(err, 5, cos.SmoduleSpace)
	}
}

func (j *clnJ) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		j._rmEmptyDir(fqn)
		return nil
	}
	if err := j.yieldTerm(); err != nil {
		return err
	}

	var parsed fs.ParsedFQN
	if _, err := core.ResolveFQN(fqn, &parsed); err != nil {
		return nil
	}
	if parsed.ContentType != fs.ObjectType {
		j.visitCT(&parsed, fqn)
	} else {
		lom := core.AllocLOM("")
		j.visitObj(fqn, lom)
		core.FreeLOM(lom)
	}
	return nil
}

func (j *clnJ) _rmEmptyDir(fqn string) {
	base := filepath.Base(fqn)

	if fs.LikelyCT(base) {
		return
	}
	if len(fqn) < len(base)+len(j.bck.Name)+8 {
		return
	}
	if !fs.ContainsCT(fqn[0:len(fqn)-len(base)], j.bck.Name) {
		return
	}

	fh, err := os.Open(fqn)
	if err != nil {
		j.ini.Xaction.AddErr(fmt.Errorf("check-empty-dir: failed to open %q: %v", fqn, err))
		core.T.FSHC(err, j.mi, "")
		return
	}
	names, ern := fh.Readdirnames(1)
	cos.Close(fh)

	switch ern {
	case nil:
		// do nothing
	case io.EOF:
		// note: removing a child may render its parent empty as well, but we do not recurs
		debug.Assert(len(names) == 0, names)
		err := syscall.Rmdir(fqn)
		debug.AssertNoErr(err)
		if cmn.Rom.FastV(4, cos.SmoduleSpace) {
			nlog.Infoln(j.String(), "rm empty dir:", fqn)
		}
	default:
		nlog.Warningf("%s: failed to read dir %q: %v", j, fqn, ern)
	}
}

func (j *clnJ) rmLeftovers() (size int64, err error) {
	var (
		fevicted, bevicted int64
		xcln               = j.ini.Xaction
	)
	if cmn.Rom.FastV(4, cos.SmoduleSpace) {
		nlog.Infof("%s: num-old %d, misplaced (%d, ec=%d)", j, len(j.oldWork), len(j.misplaced.loms), len(j.misplaced.ec))
	}

	// 1. rm older work
	for _, workfqn := range j.oldWork {
		finfo, erw := os.Stat(workfqn)
		if erw == nil {
			if err := cos.RemoveFile(workfqn); err != nil {
				nlog.Errorf("%s: failed to rm old work %q: %v", j, workfqn, err)
			} else {
				size += finfo.Size()
				fevicted++
				bevicted += finfo.Size()
				if cmn.Rom.FastV(4, cos.SmoduleSpace) {
					nlog.Infof("%s: rm old work %q, size=%d", j, workfqn, size)
				}
			}
		}
	}
	j.oldWork = j.oldWork[:0]

	// 2. rm misplaced
	if len(j.misplaced.loms) > 0 && j.p.rmMisplaced() {
		for _, mlom := range j.misplaced.loms {
			var (
				fqn     = mlom.FQN
				removed bool
			)
			lom := core.AllocLOM(mlom.ObjName)
			switch {
			case lom.InitBck(&j.bck) != nil:
				removed = os.Remove(fqn) == nil
			case lom.FromFS() != nil:
				removed = os.Remove(fqn) == nil
			default:
				removed, _ = lom.DelExtraCopies(fqn)
			}
			core.FreeLOM(lom)

			if removed {
				fevicted++
				bevicted += mlom.Lsize(true /*not loaded*/)
				if cmn.Rom.FastV(4, cos.SmoduleSpace) {
					nlog.Infof("%s: rm misplaced %q, size=%d", j, mlom, mlom.Lsize(true /*not loaded*/))
				}
				if err = j.yieldTerm(); err != nil {
					return
				}
			}
		}
	}
	j.misplaced.loms = j.misplaced.loms[:0]

	// 3. rm EC slices and replicas that are still without correcponding metafile
	for _, ct := range j.misplaced.ec {
		metaFQN := fs.CSM.Gen(ct, fs.ECMetaType, "")
		if cos.Stat(metaFQN) == nil {
			continue
		}
		if os.Remove(ct.FQN()) == nil {
			fevicted++
			bevicted += ct.Lsize()
			if err = j.yieldTerm(); err != nil {
				return
			}
		}
	}
	j.misplaced.ec = j.misplaced.ec[:0]

	j.ini.StatsT.Add(stats.CleanupStoreSize, bevicted)
	j.ini.StatsT.Add(stats.CleanupStoreCount, fevicted)
	xcln.ObjsAdd(int(fevicted), bevicted)
	return
}

func (j *clnJ) yieldTerm() error {
	xcln := j.ini.Xaction
	select {
	case errCause := <-xcln.ChanAbort():
		return cmn.NewErrAborted(xcln.Name(), "", errCause)
	case <-j.stopCh:
		return cmn.NewErrAborted(xcln.Name(), "", nil)
	default:
		break
	}
	if xcln.Finished() {
		return cmn.NewErrAborted(xcln.Name(), "", nil)
	}
	return nil
}
