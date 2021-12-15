// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

// TODO: unify and refactor (lru, cleanup-store)

type (
	IniCln struct {
		T       cluster.Target
		Xaction *XactCln
		StatsT  stats.Tracker
		Buckets []cmn.Bck // optional list of specific buckets to cleanup
		WG      *sync.WaitGroup
	}
	XactCln struct {
		xaction.XactBase
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
			loms []*cluster.LOM
			ec   []*cluster.CT // EC slices and replicas without corresponding metafiles (CT FQN -> Meta FQN)
		}
		bck cmn.Bck
		now int64
		// init-time
		p       *clnP
		ini     *IniCln
		stopCh  chan struct{}
		joggers map[string]*clnJ
		mi      *fs.MountpathInfo
		config  *cmn.Config
	}
	clnFactory struct {
		xreg.RenewBase
		xact *XactCln
	}
)

// interface guard
var _ xreg.Renewable = (*clnFactory)(nil)

func (*XactCln) Run(*sync.WaitGroup) { debug.Assert(false) }

////////////////
// clnFactory //
////////////////

func (*clnFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &clnFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *clnFactory) Start() error {
	p.xact = &XactCln{}
	p.xact.InitBase(p.UUID(), cmn.ActStoreCleanup, nil)
	return nil
}

func (*clnFactory) Kind() string        { return cmn.ActStoreCleanup }
func (p *clnFactory) Get() cluster.Xact { return p.xact }

func (p *clnFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	err = fmt.Errorf("%s is already running - not starting %q", prevEntry.Get(), p.Str(p.Kind()))
	return
}

func RunCleanup(ini *IniCln) fs.CapStatus {
	var (
		xcln           = ini.Xaction
		config         = cmn.GCO.Get()
		availablePaths = fs.GetAvail()
		num            = len(availablePaths)
		joggers        = make(map[string]*clnJ, num)
		parent         = &clnP{joggers: joggers, ini: *ini}
	)
	defer func() {
		if ini.WG != nil {
			ini.WG.Done()
		}
	}()
	if num == 0 {
		xcln.Finish(cmn.ErrNoMountpaths)
		glog.Error(cmn.ErrNoMountpaths)
		return fs.CapStatus{}
	}
	for mpath, mi := range availablePaths {
		joggers[mpath] = &clnJ{
			oldWork: make([]string, 0, 64),
			stopCh:  make(chan struct{}, 1),
			mi:      mi,
			config:  config,
			ini:     &parent.ini,
			p:       parent,
		}
		joggers[mpath].misplaced.loms = make([]*cluster.LOM, 0, 64)
		joggers[mpath].misplaced.ec = make([]*cluster.CT, 0, 64)
	}
	parent.jcnt.Store(int32(len(joggers)))
	providers := cmn.Providers.ToSlice()
	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go j.run(providers)
	}

	parent.cs.a = fs.GetCapStatus()
	if parent.cs.a.Err != nil {
		glog.Warningf("%s started, %s", xcln, parent.cs.a)
	} else {
		glog.Infof("%s started, %s", xcln, parent.cs.a)
	}
	if ini.WG != nil {
		ini.WG.Done()
		ini.WG = nil
	}
	parent.wg.Wait()

	for _, j := range joggers {
		j.stop()
	}
	xcln.Finish(nil)
	parent.cs.c, _ = fs.RefreshCapStatus(nil, nil)
	if parent.cs.c.Err != nil {
		glog.Warningf("%s finished, %s", xcln, parent.cs.c)
	} else {
		glog.Infof("%s finished, %s", xcln, parent.cs.c)
	}
	return parent.cs.c
}

func (p *clnP) rmMisplaced() (yes bool) {
	g, l := xreg.GetRebMarked(), xreg.GetResilverMarked()
	if g.Xact != nil || l.Xact != nil {
		return
	}
	yes = !g.Interrupted && !l.Interrupted
	if yes && p.cs.a.Err != nil {
		glog.Errorf("%s: %s but not removing misplaced/obsolete copies in presence of interrupted rebalance",
			p.ini.Xaction, p.cs.a.String())
	}
	return
}

//////////////////////
// mountpath jogger //
//////////////////////

func (j *clnJ) String() string {
	return fmt.Sprintf("%s: jog-%s", j.ini.Xaction, j.mi)
}

func (j *clnJ) stop() { j.stopCh <- struct{}{} }

func (j *clnJ) run(providers []string) {
	const f = "%s: freed space %s (not including removed 'deleted')"
	var (
		size     int64
		err, erm error
	)
	defer j.p.wg.Done()
	erm = j.removeDeleted()
	if erm != nil {
		glog.Error(erm)
	}
	if len(j.ini.Buckets) != 0 {
		size, err = j.jogBcks(j.ini.Buckets)
	} else {
		size, err = j.jog(providers)
	}
	if err == nil {
		err = erm
	}
	if err == nil {
		if size == 0 {
			return
		}
		glog.Infof(f, j, cos.B2S(size, 1))
	} else {
		glog.Errorf(f+", err %v", j, cos.B2S(size, 1), err)
	}
}

func (j *clnJ) jog(providers []string) (size int64, rerr error) {
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
		var (
			sz   int64
			bcks []cmn.Bck
			err  error
			opts = fs.Options{Mi: j.mi, Bck: cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}}
		)
		if bcks, err = fs.AllMpathBcks(&opts); err != nil {
			glog.Error(err)
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
	bowner := j.ini.T.Bowner()
	for _, bck := range bcks { // for each bucket under a given provider
		var (
			sz  int64
			err error
			b   = cluster.NewBckEmbed(bck)
		)
		j.bck = bck
		err = b.Init(bowner)
		if err != nil {
			if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
				const act = "delete non-existing"
				if err = fs.DestroyBucket(act, bck, 0 /*unknown bid*/); err == nil {
					glog.Infof("%s: %s %s", j, act, bck)
				} else {
					glog.Errorf("%s: failed to %s %s, err %v", j, act, bck, err)
				}
			} else {
				// TODO: config option to scrub `fs.AllMpathBcks` buckets
				glog.Errorf("%s: %v - skipping %s", j, err, bck)
			}
			continue
		}
		sz, err = j.jogBck()
		size += sz
		if err != nil && rerr == nil {
			rerr = err
		}
	}
	return
}

func (j *clnJ) removeDeleted() (err error) {
	var errCap error
	err = j.mi.RemoveDeleted(j.String())
	if cnt := j.p.jcnt.Dec(); cnt > 0 {
		return
	}
	j.p.cs.b, errCap = fs.RefreshCapStatus(nil, nil)
	if err != nil {
		glog.Errorf("%s: %v", j, errCap)
	} else {
		if j.p.cs.b.Err != nil {
			glog.Warningf("%s post-rm('deleted'), %s", j.ini.Xaction, j.p.cs.b)
		} else {
			glog.Infof("%s post-rm('deleted'), %s", j.ini.Xaction, j.p.cs.b)
		}
	}
	return
}

func (j *clnJ) jogBck() (size int64, err error) {
	opts := &fs.Options{
		Mi:       j.mi,
		Bck:      j.bck,
		CTs:      []string{fs.WorkfileType, fs.ObjectType, fs.ECSliceType, fs.ECMetaType},
		Callback: j.walk,
		Sorted:   false,
	}
	j.now = time.Now().UnixNano()
	if err = fs.Walk(opts); err != nil {
		return
	}
	size, err = j.rmLeftovers()
	return
}

func (j *clnJ) visitCT(parsedFQN fs.ParsedFQN, fqn string) {
	switch parsedFQN.ContentType {
	case fs.WorkfileType:
		_, base := filepath.Split(fqn)
		contentResolver := fs.CSM.RegisteredContentTypes[fs.WorkfileType]
		_, old, ok := contentResolver.ParseUniqueFQN(base)
		// workfiles: remove old or do nothing
		if ok && old {
			j.oldWork = append(j.oldWork, fqn)
		}
	case fs.ECSliceType:
		// EC slices:
		// - EC enabled: remove only slices with missing metafiles
		// - EC disabled: remove all slices
		ct, err := cluster.NewCTFromFQN(fqn, j.p.ini.T.Bowner())
		if err != nil || !ct.Bck().Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			return
		}
		if err := ct.LoadFromFS(); err != nil {
			return
		}
		// Saving a CT is not atomic: first it saves CT, then its metafile
		// follows. Ignore just updated CTs to avoid processing incomplete data.
		if ct.MtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
			return
		}
		metaFQN := fs.CSM.GenContentFQN(ct, fs.ECMetaType, "")
		if fs.Access(metaFQN) != nil {
			j.misplaced.ec = append(j.misplaced.ec, ct)
		}
	case fs.ECMetaType:
		// EC metafiles:
		// - EC enabled: remove only without corresponding slice or replica
		// - EC disabled: remove all metafiles
		ct, err := cluster.NewCTFromFQN(fqn, j.p.ini.T.Bowner())
		if err != nil || !ct.Bck().Props.EC.Enabled {
			j.oldWork = append(j.oldWork, fqn)
			return
		}
		// Metafile is saved the last. If there is no corresponding replica or
		// slice, it is safe to remove the stray metafile.
		sliceCT := ct.Clone(fs.ECSliceType)
		if fs.Access(sliceCT.FQN()) == nil {
			return
		}
		objCT := ct.Clone(fs.ObjectType)
		if fs.Access(objCT.FQN()) == nil {
			return
		}
		j.oldWork = append(j.oldWork, fqn)
	default:
		debug.Assertf(false, "Unsupported content type: %s", parsedFQN.ContentType)
	}
}

func (j *clnJ) visitLOM(fqn string) {
	lom := &cluster.LOM{FQN: fqn}
	err := lom.Init(j.bck)
	if err != nil {
		return
	}
	// TODO: lom.Load now returns just fmt.Error string for xattr troubles.
	// It must return designated errors for damaged/missing xattrs etc to
	// provide a fine-grained error statistics for a user.
	err = lom.Load(false /*cache it*/, false /*locked*/)
	if err != nil {
		// TODO:
		// - remove the file on some errors
		// - update correct counters (add counters for noXattr, damagedXattr etc)
		glog.Errorf("Fail to load LOM %q: %v", fqn, err)
		return
	}
	if lom.IsHRW() {
		return
	}
	if lom.AtimeUnix()+int64(j.config.LRU.DontEvictTime) > j.now {
		return
	}
	if lom.HasCopies() && lom.IsCopy() {
		return
	}
	if lom.Bprops().EC.Enabled {
		metaFQN := fs.CSM.GenContentFQN(lom, fs.ECMetaType, "")
		if fs.Access(metaFQN) != nil {
			j.misplaced.ec = append(j.misplaced.ec, cluster.NewCTFromLOM(lom, fs.ObjectType))
		}
	} else {
		j.misplaced.loms = append(j.misplaced.loms, lom)
	}
}

func (j *clnJ) walk(fqn string, de fs.DirEntry) error {
	if de.IsDir() {
		return nil
	}
	if err := j.yieldTerm(); err != nil {
		return err
	}
	parsedFQN, _, err := cluster.ResolveFQN(fqn)
	if err != nil {
		return nil
	}
	if parsedFQN.ContentType != fs.ObjectType {
		j.visitCT(parsedFQN, fqn)
	} else {
		j.visitLOM(fqn)
	}

	return nil
}

// TODO: remove disfunctional files as soon as possible without adding them to slices.
func (j *clnJ) rmLeftovers() (size int64, err error) {
	var (
		fevicted, bevicted int64
		xcln               = j.ini.Xaction
	)
	// 1. rm older work
	for _, workfqn := range j.oldWork {
		finfo, erw := os.Stat(workfqn)
		if erw == nil {
			if err := cos.RemoveFile(workfqn); err != nil {
				glog.Errorf("%s: failed to rm old work %q: %v", j, workfqn, err)
			} else {
				size += finfo.Size()
				fevicted++
				bevicted += finfo.Size()
				if verbose {
					glog.Infof("%s: rm old work %q, size=%d", j, workfqn, size)
				}
			}
		}
	}
	j.oldWork = j.oldWork[:0]

	// 2. rm misplaced
	if j.p.rmMisplaced() {
		for _, mlom := range j.misplaced.loms {
			var (
				fqn     = mlom.FQN
				removed bool
			)
			lom := &cluster.LOM{ObjName: mlom.ObjName} // yes placed
			if lom.Init(j.bck) != nil {
				removed = os.Remove(fqn) == nil
			} else if lom.FromFS() != nil {
				removed = os.Remove(fqn) == nil
			} else {
				removed, _ = lom.DelExtraCopies(fqn)
			}
			if removed {
				fevicted++
				bevicted += mlom.SizeBytes(true /*not loaded*/)
				if verbose {
					glog.Infof("%s: rm misplaced %q, size=%d", j, mlom, mlom.SizeBytes(true /*not loaded*/))
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
		metaFQN := fs.CSM.GenContentFQN(ct, fs.ECMetaType, "")
		if fs.Access(metaFQN) == nil {
			continue
		}
		if os.Remove(ct.FQN()) == nil {
			fevicted++
			bevicted += ct.SizeBytes()
			if err = j.yieldTerm(); err != nil {
				return
			}
		}
	}
	j.misplaced.ec = j.misplaced.ec[:0]

	j.ini.StatsT.Add(stats.CleanupStoreSize, bevicted) // TODO -- FIXME
	j.ini.StatsT.Add(stats.CleanupStoreCount, fevicted)
	xcln.ObjsAdd(int(fevicted), bevicted)
	return
}

func (j *clnJ) yieldTerm() error {
	xcln := j.ini.Xaction
	select {
	case <-xcln.ChanAbort():
		return cmn.NewErrAborted(xcln.Name(), "", nil)
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
