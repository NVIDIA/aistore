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
			b fs.CapStatus // post removal of 'deleted' ($trash)
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

func (p *clnP) rmMisplaced() bool {
	g, l := xreg.GetRebMarked(), xreg.GetResilverMarked()
	if g.Xact != nil || l.Xact != nil {
		return false
	}
	// NOTE: high-watermark warranted
	if p.cs.a.Err != nil {
		return true
	}
	return !g.Interrupted && !l.Interrupted
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
	erm = j.removeTrash()
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
				j.ini.T.TrashNonExistingBucket(bck)
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

func (j *clnJ) removeTrash() (rerr error) {
	trashDir := j.mi.MakePathTrash()
	dentries, err := os.ReadDir(trashDir)
	if err != nil {
		if os.IsNotExist(err) {
			cos.CreateDir(trashDir)
			err = nil
		}
		rerr = err
		goto ret
	}
	for _, dent := range dentries {
		fqn := filepath.Join(trashDir, dent.Name())
		if !dent.IsDir() {
			glog.Errorf("%s: unexpected non-directory item %q in 'deleted'", j, fqn)
			continue
		}
		if err = os.RemoveAll(fqn); err == nil {
			continue
		}
		if !os.IsNotExist(err) {
			glog.Errorf("%s: failed to remove %q from 'deleted', err %v", j, fqn, err)
			if rerr == nil {
				rerr = err
			}
		}
	}
ret:
	if cnt := j.p.jcnt.Dec(); cnt > 0 {
		return
	}
	j.p.cs.b, err = fs.RefreshCapStatus(nil, nil)
	if err != nil {
		glog.Errorf("%s: %v", j, err)
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
	size, err = j.evict()
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

func (j *clnJ) visitLOM(parsedFQN fs.ParsedFQN) {
	lom := &cluster.LOM{ObjName: parsedFQN.ObjName}
	err := lom.Init(j.bck)
	if err != nil {
		return
	}
	err = lom.Load(false /*cache it*/, false /*locked*/)
	if err != nil {
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
		j.visitLOM(parsedFQN)
	}

	return nil
}

func (j *clnJ) evict() (size int64, err error) {
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
				if verbose {
					glog.Infof("%s: rm old work %q, size=%d", j, workfqn, size)
				}
			}
		}
	}
	j.oldWork = j.oldWork[:0]

	// 2. rm misplaced
	if j.p.rmMisplaced() {
		for _, lom := range j.misplaced.loms {
			var (
				fqn     = lom.FQN
				removed bool
			)
			lom = &cluster.LOM{ObjName: lom.ObjName} // yes placed
			if lom.Init(j.bck) != nil {
				removed = os.Remove(fqn) == nil
			} else if lom.FromFS() != nil {
				removed = os.Remove(fqn) == nil
			} else {
				removed, _ = lom.DelExtraCopies(fqn)
			}
			if !removed && lom.FQN != fqn {
				removed = os.Remove(fqn) == nil
			}
			if removed {
				if verbose {
					glog.Infof("%s: rm misplaced %q, size=%d", j, lom, lom.SizeBytes(true /*not loaded*/))
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
			if err = j.yieldTerm(); err != nil {
				return
			}
		}
	}
	j.misplaced.ec = j.misplaced.ec[:0]

	j.ini.StatsT.Add(stats.CleanupStoreSize, bevicted) // TODO -- FIXME
	j.ini.StatsT.Add(stats.CleanupStoreCount, fevicted)
	xcln.ObjectsAdd(fevicted)
	xcln.BytesAdd(bevicted)
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
