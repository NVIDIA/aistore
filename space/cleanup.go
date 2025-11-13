// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// NOTE:
// - for cleanup policies and implementation details, see README.md in this package
// - report stats counters "cleanup.store.n" & "cleanup.store.size" (not to confuse with generic ""loc-objs", "in-objs", etc.)

// (batch sizing)
const (
	flagRmOldWork = 1 << iota
	flagRmMisplacedLOMs
	flagRmMisplacedEC
	flagRmInvalid
	flagRmAll = flagRmOldWork | flagRmMisplacedLOMs | flagRmMisplacedEC | flagRmInvalid
)

const (
	sparseLogCnt = 100
)

type (
	XactCln struct {
		p   *clnFactory
		ini *IniCln
		xact.Base
	}
	IniCln struct {
		StatsT  stats.Tracker
		Xaction *XactCln
		WG      *sync.WaitGroup
		Args    *xact.ArgsMsg
	}
)

// private
type (
	// parent (contains mpath joggers)
	clnP struct {
		ini     IniCln
		joggers map[string]*clnJ
		cs      struct {
			a fs.CapStatus // initial
			b fs.CapStatus // after removing 'deleted'
			c fs.CapStatus // upon finishing
		}
		wg   sync.WaitGroup
		jcnt atomic.Int32 // initial
		// upon finishing
	}
	// clnJ represents a single cleanup context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	clnJ struct {
		now       time.Time
		p         *clnP
		config    *cmn.Config
		mi        *fs.Mountpath
		joggers   map[string]*clnJ
		stopCh    chan struct{}
		ini       *IniCln
		bck       cmn.Bck
		name      string
		misplaced struct {
			loms []*core.LOM
			ec   []*core.CT // EC slices and replicas without corresponding metafiles (CT FQN -> Meta FQN)
		}
		oldWork []string // EC slices and replicas without corresponding metafiles (CT FQN -> Meta FQN)

		invalid []string
		nmisplc int64
		norphan int64
		nvisits int64

		// throttle
		adv load.Advice
	}
	clnFactory struct {
		xctn *XactCln
		xreg.RenewBase
	}
)

// interface guard
var (
	_ xreg.Renewable = (*clnFactory)(nil)
	_ core.Xact      = (*XactCln)(nil)
)

func (*XactCln) Run(*sync.WaitGroup) { debug.Assert(false) } // via RunCleanup() below

func (r *XactCln) ctlmsg() string {
	s := r.p.Args.Custom.(string)
	if r.ini == nil {
		return s
	}
	return s + ", " + r.ini.Args.String()
}

func (r *XactCln) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.AddBaseSnap(snap)

	snap.SetCtlMsg(r.Name(), r.ctlmsg())

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
	p.xctn = &XactCln{p: p}
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

	xcln.ini = ini

	now := time.Now()
	for mpath, mi := range avail {
		j := &clnJ{
			oldWork: make([]string, 0, 64),
			invalid: make([]string, 0, 64),
			stopCh:  make(chan struct{}, 1),
			mi:      mi,
			config:  config,
			ini:     &parent.ini,
			p:       parent,
			now:     now,
		}
		j.name = j._str()

		// init throttling context
		j.adv.Init(load.FlMem|load.FlCla|load.FlDsk, &load.Extra{Mi: j.mi, Cfg: &j.config.Disk, RW: false})

		// add
		joggers[mpath] = j
		joggers[mpath].misplaced.loms = make([]*core.LOM, 0, 64)
		joggers[mpath].misplaced.ec = make([]*core.CT, 0, 64)
	}
	parent.jcnt.Store(int32(len(joggers)))
	providers := apc.Providers.ToSlice()
	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go j.jog(providers)
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
		xcln.Finish()
		nlog.Warningln(xcln.Name(), "finished with cap error:", errCap)
	} else {
		xcln.Finish()
	}
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
	xcln := p.ini.Xaction
	if ok {
		flog(core.T.String(), xcln.String(), "proceeding to remove misplaced obj-s with force, ignoring: [", cserr, why, "]")
	} else {
		flog(core.T.String(), xcln.String(), "not removing misplaced obj-s: [", cserr, why, "]")
	}
	return ok
}

//////////
// clnJ //
//////////

func (j *clnJ) String() string { return j.name }

func (j *clnJ) _str() string {
	var sb strings.Builder
	sb.Grow(128)
	sb.WriteString(j.ini.Xaction.Name())
	sb.WriteString("-j[")
	sb.WriteString(j.mi.Path)
	if j.ini.Args.Force {
		sb.WriteString("--f") // force
	}
	if j.rmZeroSize() {
		sb.WriteString("--z") // rm zero-size
	}
	if _, ok := j.keepMisplaced(); ok {
		sb.WriteString("--k") // keep misplaced
	}
	sb.WriteByte(']')
	return sb.String()
}

func (j *clnJ) stop() { j.stopCh <- struct{}{} }

func (j *clnJ) dont() time.Duration { return j.config.Space.DontCleanupTime.D() }

func (j *clnJ) rmZeroSize() bool { return j.ini.Args.Flags&xact.FlagZeroSize != 0 }

func (j *clnJ) keepMisplaced() (string, bool) {
	if j.ini.Args.Flags&xact.FlagKeepMisplaced != 0 {
		return "keeping", true
	}
	return "removing", false
}

func (j *clnJ) jog(providers []string) {
	// globally
	j.rmDeleted()

	// traverse
	if len(j.ini.Args.Buckets) != 0 {
		j.jogBcks(j.ini.Args.Buckets)
	} else {
		j.jogProviders(providers)
	}

	j.oldWork = slices.Clip(j.oldWork)
	j.invalid = slices.Clip(j.invalid)
	j.misplaced.loms = slices.Clip(j.misplaced.loms)
	j.misplaced.ec = slices.Clip(j.misplaced.ec)

	j.p.wg.Done()
}

func (j *clnJ) jogProviders(providers []string) {
	xcln := j.ini.Xaction
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
		var (
			bcks []cmn.Bck
			err  error
			opts = fs.WalkOpts{Mi: j.mi, Bck: cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}}
		)
		if bcks, err = fs.AllMpathBcks(&opts); err != nil {
			xcln.AddErr(err, 0)
			continue
		}
		if len(bcks) == 0 {
			continue
		}
		j.jogBcks(bcks)
		if xcln.IsAborted() || j.done() {
			return
		}
	}
}

func (j *clnJ) jogBcks(bcks []cmn.Bck) {
	var (
		xcln   = j.ini.Xaction
		bowner = core.T.Bowner()
	)
	for i := range bcks { // for each bucket under a given provider
		var (
			err error
			bck = bcks[i]
			b   = meta.CloneBck(&bck)
		)
		j.bck = bck
		err = b.Init(bowner)
		if err != nil {
			if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
				const act = "delete non-existing"
				if err = fs.DestroyBucket(act, &bck, 0 /*unknown BID*/); err == nil {
					nlog.Infof("%s: %s %s", j, act, bck.String())
				} else {
					xcln.AddErr(err)
					nlog.Errorf("%s %s: %v - skipping", j, act, err)
				}
			} else {
				// TODO: config option to scrub `fs.AllMpathBcks` buckets
				xcln.AddErr(err)
				nlog.Errorf("%s: %v - skipping %s", j, err, bck.String())
			}
			continue
		}
		j._jogBck()
		if xcln.IsAborted() || j.done() {
			return
		}
	}
}

// walk a given bucket and visit assorted content types (below)
func (j *clnJ) _jogBck() {
	xcln := j.ini.Xaction
	opts := &fs.WalkOpts{
		Mi:       j.mi,
		Bck:      j.bck,
		CTs:      []string{fs.WorkCT, fs.ObjCT, fs.ECSliceCT, fs.ECMetaCT, fs.ChunkCT, fs.ChunkMetaCT},
		Callback: j.visit,
		Sorted:   false,
	}
	err := fs.Walk(opts)
	if j.norphan > 0 {
		nlog.Warningln(j.String(), "removed", j.norphan, "orphan chunks")
	}
	if err != nil {
		xcln.AddErr(err)
		return
	}
	j.rmLeftovers(flagRmAll)
}

func (j *clnJ) visit(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		j.rmEmptyDir(fqn)
		return nil
	}
	if j.done() {
		return nil
	}

	j.nvisits++
	if finfo, err := os.Lstat(fqn); err == nil {
		mtime := finfo.ModTime()
		if mtime.Add(j.dont()).After(j.now) {
			return nil // skipping - too early
		}
	}

	var parsed fs.ParsedFQN
	if err := parsed.Init(fqn); err != nil {
		j.rmInvalidFQN(fqn, "", err)
		return nil
	}

	if !parsed.Bck.Equal(&j.bck) {
		err := fmt.Errorf("%s: unexpected bucket mismatch: [%q, %s, %s]", j, fqn, parsed.Bck.String(), j.bck.String())
		debug.AssertNoErr(err)
		j.ini.Xaction.AddErr(err, 0)
		return nil
	}

	if parsed.ContentType != fs.ObjCT {
		j.visitCT(&parsed, fqn)
	} else {
		lom := core.AllocLOM("")
		j.visitObj(fqn, lom)
		core.FreeLOM(lom)
	}

	if j.adv.ShouldCheck(j.nvisits) {
		j.adv.Refresh()
		if j.adv.Sleep > 0 {
			time.Sleep(j.adv.Sleep)
		}
	}

	return nil
}

func (j *clnJ) rmInvalidFQN(fqn, ctType string, err error) {
	if cmn.Rom.Features().IsSet(feat.KeepUnknownFQN) {
		return
	}

	var e error
	if err != nil {
		e = fmt.Errorf("invalid fqn %q: %v", fqn, err)
	} else {
		e = fmt.Errorf("invalid %q fqn: %q", ctType, fqn)
	}
	xcln := j.ini.Xaction
	xcln.AddErr(e)

	nlog.Warningln(j.String(), "rm", e)
	j.invalid = append(j.invalid, fqn)
	j.rmAnyBatch(flagRmInvalid)
}

func (j *clnJ) visitCT(parsed *fs.ParsedFQN, fqn string) {
	switch parsed.ContentType {
	case fs.WorkCT:
		_, ubase := filepath.Split(fqn)
		contentInfo := fs.CSM.ParseUbase(ubase, fs.WorkCT)
		if !contentInfo.Ok {
			j.rmInvalidFQN(fqn, "work", nil)
		} else if contentInfo.Old {
			j.oldWork = append(j.oldWork, fqn)
			j.rmAnyBatch(flagRmOldWork)
		}

	// EC enabled:
	// - remove slices with missing metafiles
	// - remove metafiles with missing slice _and_ replica
	// EC disabled:
	// - remove all slices and metafiles
	case fs.ECSliceCT:
		if !j.bck.Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			j.rmAnyBatch(flagRmOldWork)
			return
		}
		ct := core.NewCTFromParsed(parsed, fqn)
		metaFQN := ct.GenFQN(fs.ECMetaCT)
		if cos.Stat(metaFQN) == nil {
			// metafile present, nothing to do
			return
		}
		j.misplaced.ec = append(j.misplaced.ec, ct)
		j.rmAnyBatch(flagRmMisplacedEC)
	case fs.ECMetaCT:
		if !j.bck.Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			j.rmAnyBatch(flagRmOldWork)
			return
		}
		ct := core.NewCTFromParsed(parsed, fqn)

		sliceCT := ct.Clone(fs.ECSliceCT)
		if cos.Stat(sliceCT.FQN()) == nil {
			// keep meta if any EC slice exists
			return
		}
		replicaCT := ct.Clone(fs.ObjCT)
		if cos.Stat(replicaCT.FQN()) == nil {
			// keep meta if a local full replica (fs.ObjCT) exists
			return
		}

		// Metafile is saved the last. Since there is no corresponding replica and slice,
		// it is safe to remove the meta.
		j.oldWork = append(j.oldWork, fqn)
		j.rmAnyBatch(flagRmOldWork)

	case fs.ChunkCT:
		contentInfo := fs.CSM.ParseUbase(parsed.ObjName, fs.ChunkCT)
		if !contentInfo.Ok {
			j.rmInvalidFQN(fqn, "chunk", nil)
			return
		}
		uploadID := contentInfo.Extras[0]
		lom := core.AllocLOM(contentInfo.Base)
		if j.initCTLOM(lom, fqn) == nil {
			j.visitChunk(fqn, lom, uploadID)
		}
		core.FreeLOM(lom)
	case fs.ChunkMetaCT:
		contentInfo := fs.CSM.ParseUbase(parsed.ObjName, fs.ChunkMetaCT)
		if !contentInfo.Ok {
			j.rmInvalidFQN(fqn, "chunk-manifest", nil)
			return
		}
		if len(contentInfo.Extras) > 0 {
			// old partial manifest
			nlog.Warningln(j.String(), "rm old partial:", fqn, "[", contentInfo.Extras[0], j.bck.Cname(contentInfo.Base), "]")
			j.oldWork = append(j.oldWork, fqn)
			j.rmAnyBatch(flagRmOldWork)
		}

	default:
		debug.Assert(false, "Unsupported content type: ", parsed.ContentType)
	}
}

func (j *clnJ) initCTLOM(lom *core.LOM, fqn string) error {
	err := lom.InitBck(&j.bck)
	if err == nil {
		return nil
	}
	xcln := j.ini.Xaction
	if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
		nlog.Warningln(j.String(), "bucket gone - aborting:", err)
	} else {
		err = fmt.Errorf("%s: unexpected lom-init fail [ %q => %q, %w ]", j, fqn, lom.ObjName, err)
		nlog.Errorln(err)
	}
	xcln.Abort(err)
	return err
}

func (j *clnJ) visitChunk(chunkFQN string, lom *core.LOM, uploadID string) {
	lom.Lock(false)
	completedID := j._getCompletedID(lom)
	lom.Unlock(false)

	// 1. have completed
	if completedID != "" {
		if completedID != uploadID {
			j.norphan++
			if j.norphan%sparseLogCnt == 1 || cmn.Rom.V(5, cos.ModSpace) {
				nlog.Warningln(j.String(), "orphan chunk", chunkFQN, "vs completed: [", completedID, lom.Cname(), j.norphan, "]")
			}
			j.oldWork = append(j.oldWork, chunkFQN)
			j.rmAnyBatch(flagRmOldWork)
		}
		return
	}

	j.norphan++

	// 2. resolve partial; if exists check its age
	fqn := lom.GenFQN(fs.ChunkMetaCT, uploadID) // (compare with Ufest._fqns())
	if finfo, err := os.Lstat(fqn); err == nil {
		if finfo.ModTime().Add(j.dont()).After(j.now) {
			return
		}
		if j.norphan%sparseLogCnt == 1 || cmn.Rom.V(5, cos.ModSpace) {
			nlog.Warningln(j.String(), "orphan chunk", chunkFQN, "from partial: [", fqn, lom.Cname(), j.norphan, "]")
		}
		j.oldWork = append(j.oldWork, chunkFQN)
		j.rmAnyBatch(flagRmOldWork)
	}

	// 3. no partial and no completed: the chunk appears to be orphan and old
	if j.norphan%sparseLogCnt == 1 || cmn.Rom.V(4, cos.ModSpace) {
		nlog.Warningln(j.String(), "orphan chunk w/ no manifests", chunkFQN, j.norphan)
	}
	j.oldWork = append(j.oldWork, chunkFQN)
	j.rmAnyBatch(flagRmOldWork)
}

func (j *clnJ) _getCompletedID(lom *core.LOM) (id string) {
	xcln := j.ini.Xaction
	if err := lom.Load(false, true); err != nil {
		return
	}
	if !lom.IsChunked() {
		return
	}

	manifest, err := core.NewUfest("", lom, true /*must-exist*/)
	if err != nil {
		debug.AssertNoErr(err)
		xcln.AddErr(err, 0)
		return
	}
	if err := manifest.LoadCompleted(lom); err != nil {
		e := fmt.Errorf("%s: failed to load completed manifest that must exist: %v", j, err)
		xcln.AddErr(e, 0)
		return
	}
	return manifest.ID()
}

func (j *clnJ) visitObj(fqn string, lom *core.LOM) {
	xcln := j.ini.Xaction
	if err := lom.InitFQN(fqn, &j.bck); err != nil {
		if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
			nlog.Warningln(j.String(), "bucket gone - aborting:", err)
			xcln.Abort(err)
		} else {
			err := fmt.Errorf("%s: unexpected object fqn %q: %v", j, fqn, err)
			xcln.AddErr(err, 0)
		}
		return
	}
	// force md loading from disk
	lom.Uncache()
	// and load
	if errLoad := lom.Load(false /*cache it*/, false /*locked*/); errLoad != nil {
		if cmn.IsErrLmetaCorrupted(errLoad) {
			if err := lom.RemoveMain(); err != nil {
				e := fmt.Errorf("%s rm MD-corrupted %s: %v (nested: %v)", j, lom, errLoad, err)
				xcln.AddErr(e, 0)
			} else {
				nlog.Errorf("%s: removed MD-corrupted %s: %v", j, lom, errLoad)
			}
		} else if cmn.IsErrLmetaNotFound(errLoad) {
			if err := lom.RemoveMain(); err != nil {
				e := fmt.Errorf("%s rm no-MD %s: %v (nested: %v)", j, lom, errLoad, err)
				xcln.AddErr(e, 0)
			} else {
				nlog.Errorf("%s: removed no-MD %s: %v", j, lom, errLoad)
			}
		}
		return
	}

	atime := lom.Atime()
	switch {
	// too early atime-wise
	case atime.Add(j.dont()).After(j.now):
		if cmn.Rom.V(5, cos.ModSpace) {
			nlog.Infoln("too early for", lom.String(), "atime", lom.Atime().String(), "dont-cleanup", j.dont())
		}
	case lom.IsHRW():
		// cleanup extra copies; rm zero size if requested
		if lom.HasCopies() {
			j.rmExtraCopies(lom)
		}
		if lom.Lsize() == 0 && j.rmZeroSize() {
			// remove in place
			if err := lom.RemoveMain(); err != nil {
				e := fmt.Errorf("%s rm zero-size %s: %v", j, lom, err)
				xcln.AddErr(e, 0)
			} else {
				nlog.Warningln(j.String(), "removed zero-size", lom.Cname())
				j.ini.StatsT.Inc(stats.CleanupStoreCount)
			}
		}
	case lom.IsCopy():
		// will be _visited_ separately (if not already)
	case lom.ECEnabled():
		// misplaced EC
		metaFQN := lom.GenFQN(fs.ECMetaCT)
		if cos.Stat(metaFQN) != nil {
			j.misplaced.ec = append(j.misplaced.ec, core.NewCTFromLOM(lom, fs.ObjCT))
			j.rmAnyBatch(flagRmMisplacedEC)
		}
	default:
		// misplaced object
		j.nmisplc++
		tag, keep := j.keepMisplaced()

		// an unlikely corner case: taking precedence
		if lom.Lsize() == 0 && j.rmZeroSize() {
			tag, keep = "removing", false
		}

		if j.nmisplc%sparseLogCnt == 1 || cmn.Rom.V(4, cos.ModSpace) {
			nlog.Warningln(j.String(), tag, "misplaced object:", lom.Cname(), j.nmisplc)
		}
		if !keep {
			lom = lom.Clone()
			j.misplaced.loms = append(j.misplaced.loms, lom)
			j.rmAnyBatch(flagRmMisplacedLOMs)
		}
	}
}

//
// removals --------------------------------------------
//

func (j *clnJ) rmAnyBatch(specifier int) {
	batch := j.config.Space.BatchSize
	debug.Assert(batch >= cmn.GCBatchSizeMin)

	switch specifier {
	case flagRmOldWork:
		if int64(len(j.oldWork)) < batch {
			return
		}
	case flagRmMisplacedLOMs:
		if int64(len(j.misplaced.loms)) < batch {
			return
		}
	case flagRmMisplacedEC:
		if int64(len(j.misplaced.ec)) < batch {
			return
		}
	case flagRmInvalid:
		if int64(len(j.invalid)) < batch {
			return
		}
	default:
		debug.Assert(false, "invalid rm-batch specifier: ", specifier)
		return
	}
	j.rmLeftovers(specifier)
}

func (j *clnJ) rmDeleted() {
	xcln := j.ini.Xaction
	err := j.mi.RemoveDeleted(j.String())
	if err != nil {
		xcln.AddErr(err)
	}
	if cnt := j.p.jcnt.Dec(); cnt > 0 {
		return
	}

	// last rm-deleted done: refresh cap now
	var errCap error
	j.p.cs.b, err, errCap = fs.CapRefresh(j.config, nil /*tcdf*/)
	if err != nil {
		xcln.Abort(err)
		return
	}
	if errCap != nil {
		nlog.Warningln(xcln.Name(), "post-rm('deleted'):", errCap)
	}
}

func (j *clnJ) rmExtraCopies(lom *core.LOM) {
	xcln := j.ini.Xaction
	if !lom.TryLock(true) {
		return // must be busy
	}
	defer lom.Unlock(true)

	// reload under lock and check atime - again
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !cos.IsNotExist(err) {
			xcln.AddErr(err)
		}
		return
	}
	atime := lom.Atime()
	if atime.Add(j.dont()).After(j.now) {
		return
	}
	if lom.IsCopy() {
		return // extremely unlikely but ok
	}
	if _, err := lom.DelExtraCopies(); err != nil {
		e := fmt.Errorf("%s: failed delete redundant copies of %s: %v", j, lom, err)
		xcln.AddErr(e, 5, cos.ModSpace)
	}
}

func (j *clnJ) rmEmptyDir(fqn string) {
	var (
		xcln = j.ini.Xaction
		base = filepath.Base(fqn)
	)
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
		xcln.AddErr(fmt.Errorf("check-empty-dir: open %q: %v", fqn, err))
		core.T.FSHC(err, j.mi, "")
		return
	}
	names, ern := fh.Readdirnames(1)
	cos.Close(fh)

	switch ern {
	case nil:
		// do nothing
	case io.EOF:
		debug.Assert(len(names) == 0, names)

		// note: removing a child may render its parent empty as well, but we do not recurs
		if err := syscall.Rmdir(fqn); err == nil {
			if cmn.Rom.V(4, cos.ModSpace) {
				nlog.Infoln(j.String(), "rm empty dir:", fqn)
			}
		} else if errno, ok := err.(syscall.Errno); ok {
			switch errno {
			case syscall.ENOENT, syscall.ENOTEMPTY, syscall.EBUSY, syscall.EEXIST, syscall.ENOTDIR:
				// benign
			default:
				xcln.AddErr(fmt.Errorf("%s rmdir %q: %v", j, fqn, err), 0) // consider FSHC
			}
		} else {
			xcln.AddErr(fmt.Errorf("%s rmdir %q: %v", j, fqn, err), 0) // ditto FSHC
		}

	default:
		nlog.Warningf("%s read dir %q: %v", j, fqn, ern)
	}
}

func (j *clnJ) rmLeftovers(specifier int) {
	var (
		nfiles, nbytes int64
		xcln           = j.ini.Xaction
	)
	old, ml, me, inv := len(j.oldWork), len(j.misplaced.loms), len(j.misplaced.ec), len(j.invalid)
	nlog.Infoln(j.String(), "[ old:", old, "misplaced obj:", ml, "misplaced ec:", me, "invalid:", inv, "]")

	// 1. rm older work
	if specifier&flagRmOldWork != 0 {
		for _, workfqn := range j.oldWork {
			finfo, erw := os.Lstat(workfqn)
			if erw == nil {
				if err := cos.RemoveFile(workfqn); err != nil {
					e := fmt.Errorf("%s: rm old %q: %v", j, workfqn, err)
					xcln.AddErr(e)
				} else {
					nfiles++
					nbytes += finfo.Size()
					j._throttle(nfiles)
					if cmn.Rom.V(5, cos.ModSpace) {
						nlog.Infoln(j.String(), "rm old", workfqn, "size", finfo.Size())
					}
				}
			}
		}
		j.oldWork = j.oldWork[:0]
		j.now = time.Now()
	}

	// 2. rm misplaced
	if specifier&flagRmMisplacedLOMs != 0 {
		if len(j.misplaced.loms) > 0 && j.p.rmMisplaced() /*note: caution*/ {
			for _, mlom := range j.misplaced.loms {
				var (
					err     error
					fqn     = mlom.FQN
					removed bool
				)
				lom := core.AllocLOM(mlom.ObjName)
				switch {
				case lom.InitBck(&j.bck) != nil:
					err = os.Remove(fqn)
					removed = err == nil
				case lom.FromFS() != nil:
					err = os.Remove(fqn)
					removed = err == nil
				default:
					removed, err = lom.DelExtraCopies(fqn)
				}
				if err != nil {
					e := fmt.Errorf("%s rm misplaced %q: %v", j, lom.String(), err)
					xcln.AddErr(e)
				}
				core.FreeLOM(lom)

				if removed {
					nfiles++
					size := mlom.Lsize(true /*not loaded*/)
					nbytes += size
					if cmn.Rom.V(4, cos.ModSpace) {
						nlog.Infoln(j.String(), "rm misplaced", mlom.String(), "size", size)
					}

					j._throttle(nfiles)
					if j.done() {
						return
					}
				}
			}
		}
		j.misplaced.loms = j.misplaced.loms[:0]
		j.now = time.Now()
	}

	// 3. rm EC slices and replicas that are still without corresponding metafile
	if specifier&flagRmMisplacedEC != 0 {
		for _, ct := range j.misplaced.ec {
			metaFQN := ct.GenFQN(fs.ECMetaCT)
			if cos.Stat(metaFQN) == nil {
				continue
			}
			if os.Remove(ct.FQN()) == nil {
				nfiles++
				nbytes += ct.Lsize()

				j._throttle(nfiles)
				if j.done() {
					return
				}
			}
		}
		j.misplaced.ec = j.misplaced.ec[:0]
		j.now = time.Now()
	}

	// 4. rm invalid FQNs - unrecognized or malformed content types
	if specifier&flagRmInvalid != 0 {
		for _, fqn := range j.invalid {
			finfo, erw := os.Lstat(fqn)
			if erw == nil {
				if err := cos.RemoveFile(fqn); err != nil {
					e := fmt.Errorf("%s: rm invalid %q: %v", j, fqn, err)
					xcln.AddErr(e)
				} else {
					nfiles++
					nbytes += finfo.Size()
					if cmn.Rom.V(5, cos.ModSpace) {
						nlog.Infoln(j.String(), "rm invalid", fqn, "size", finfo.Size())
					}
					j._throttle(nfiles)
				}
			}
		}
		j.invalid = j.invalid[:0]
		j.now = time.Now()
	}

	j.ini.StatsT.Add(stats.CleanupStoreSize, nbytes)
	j.ini.StatsT.Add(stats.CleanupStoreCount, nfiles)
	xcln.ObjsAdd(int(nfiles), nbytes)
}

func (j *clnJ) _throttle(n int64) {
	if j.adv.ShouldCheck(n) {
		j.adv.Refresh()
		if j.adv.Sleep > 0 {
			time.Sleep(j.adv.Sleep)
		}
	}
}

func (j *clnJ) done() bool {
	xcln := j.ini.Xaction
	select {
	case <-xcln.ChanAbort():
		return true
	case <-j.stopCh:
		return true
	default:
		break
	}
	return xcln.IsDone()
}
