// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

type (
	InitStoreCln struct {
		T       cluster.Target
		Xaction *StoreClnXaction
		StatsT  stats.Tracker
		Buckets []cmn.Bck // list of buckets to run LRU
	}

	// parent - contains mpath joggers
	cleanP struct {
		wg      sync.WaitGroup
		joggers map[string]*cleanJ
		ini     InitStoreCln
	}

	// cleanJ represents a single cleanup context and a single /jogger/
	// that traverses and evicts a single given mountpath.
	cleanJ struct {
		// runtime
		totalSize int64
		oldWork   []string
		misplaced struct {
			loms []*cluster.LOM
			ec   []*cluster.CT // EC slices and replicas without corresponding metafiles (CT FQN -> Meta FQN)
		}
		bck cmn.Bck
		now int64
		// init-time
		p         *cleanP
		ini       *InitStoreCln
		stopCh    chan struct{}
		joggers   map[string]*cleanJ
		mpathInfo *fs.MountpathInfo
		config    *cmn.Config
		// runtime
		allowDelObj bool
	}

	StoreClnFactory struct {
		xreg.RenewBase
		xact *StoreClnXaction
	}
	StoreClnXaction struct {
		xaction.XactBase
	}
)

// interface guard
var (
	_ xreg.Renewable = (*StoreClnFactory)(nil)
)

func rmMisplaced() bool {
	g, l := xreg.GetRebMarked(), xreg.GetResilverMarked()
	return !g.Interrupted && !l.Interrupted && g.Xact == nil && l.Xact == nil
}

func (*StoreClnXaction) Run(*sync.WaitGroup) { debug.Assert(false) }

/////////////
// Factory //
/////////////

func (*StoreClnFactory) New(args xreg.Args, _ *cluster.Bck) xreg.Renewable {
	return &StoreClnFactory{RenewBase: xreg.RenewBase{Args: args}}
}

func (p *StoreClnFactory) Start() error {
	p.xact = &StoreClnXaction{}
	p.xact.InitBase(p.UUID(), cmn.ActStoreCleanup, nil)
	return nil
}

func (*StoreClnFactory) Kind() string        { return cmn.ActStoreCleanup }
func (p *StoreClnFactory) Get() cluster.Xact { return p.xact }

func (p *StoreClnFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	err = fmt.Errorf("%s is already running - not starting %q", prevEntry.Get(), p.Str(p.Kind()))
	return
}

func RunStoreClean(ini *InitStoreCln) {
	var (
		xlru           = ini.Xaction
		config         = cmn.GCO.Get()
		availablePaths = fs.GetAvail()
		num            = len(availablePaths)
		joggers        = make(map[string]*cleanJ, num)
		parent         = &cleanP{joggers: joggers, ini: *ini}
	)
	glog.Infof("[cleanup] %s started: dont-evict-time %v", xlru, config.LRU.DontEvictTime)
	if num == 0 {
		glog.Warning(cmn.ErrNoMountpaths)
		xlru.Finish(cmn.ErrNoMountpaths)
		return
	}
	for mpath, mpathInfo := range availablePaths {
		joggers[mpath] = &cleanJ{
			oldWork:   make([]string, 0, 64),
			stopCh:    make(chan struct{}, 1),
			mpathInfo: mpathInfo,
			config:    config,
			ini:       &parent.ini,
			p:         parent,
		}
		joggers[mpath].misplaced.loms = make([]*cluster.LOM, 0, 64)
		joggers[mpath].misplaced.ec = make([]*cluster.CT, 0, 64)
	}
	providers := cmn.Providers.ToSlice()

	for _, j := range joggers {
		parent.wg.Add(1)
		j.joggers = joggers
		go j.run(providers)
	}
	parent.wg.Wait()

	for _, j := range joggers {
		j.stop()
	}
	xlru.Finish(nil)
}

//////////////////////
// mountpath jogger //
//////////////////////

func (j *cleanJ) String() string {
	return fmt.Sprintf("%s: (%s, %s)", j.ini.T.Snode(), j.ini.Xaction, j.mpathInfo)
}

func (j *cleanJ) stop() { j.stopCh <- struct{}{} }

func (j *cleanJ) run(providers []string) {
	var err error
	defer j.p.wg.Done()
	if err = j.removeTrash(); err != nil {
		goto ex
	}
	if len(j.ini.Buckets) != 0 {
		glog.Infof("[lru] %s: freeing-up %s", j, cos.B2S(j.totalSize, 2))
		err = j.jogBcks(j.ini.Buckets)
	} else {
		err = j.jog(providers)
	}
ex:
	if err == nil || cmn.IsErrBucketNought(err) || cmn.IsErrObjNought(err) {
		return
	}
	glog.Errorf("[lru] %s: exited with err %v", j, err)
}

func (j *cleanJ) jog(providers []string) (err error) {
	glog.Infof("%s: freeing-up %s", j, cos.B2S(j.totalSize, 2))
	for _, provider := range providers { // for each provider (NOTE: ordering is random)
		var (
			bcks []cmn.Bck
			opts = fs.Options{
				Mi:  j.mpathInfo,
				Bck: cmn.Bck{Provider: provider, Ns: cmn.NsGlobal},
			}
		)
		if bcks, err = fs.AllMpathBcks(&opts); err != nil {
			return
		}
		if err = j.jogBcks(bcks); err != nil {
			return
		}
	}
	return
}

func (j *cleanJ) jogBcks(bcks []cmn.Bck) (err error) {
	if len(bcks) == 0 {
		return
	}
	for _, bck := range bcks { // for each bucket under a given provider
		j.bck = bck
		if j.allowDelObj, err = j.allow(); err != nil {
			if cmn.IsErrBckNotFound(err) || cmn.IsErrRemoteBckNotFound(err) {
				j.ini.T.TrashNonExistingBucket(bck)
			} else {
				// TODO: config option to scrub `fs.AllMpathBcks` buckets
				glog.Errorf("%s: %v - skipping %s", j, err, bck)
			}
			err = nil
			continue
		}
		if _, err = j.jogBck(); err != nil {
			return
		}
	}
	return
}

func (j *cleanJ) removeTrash() error {
	trashDir := j.mpathInfo.MakePathTrash()
	return fs.Scanner(trashDir, func(fqn string, de fs.DirEntry) error {
		if de.IsDir() {
			if err := os.RemoveAll(fqn); err != nil && !os.IsNotExist(err) {
				glog.Errorf("%s: %v", j, err)
				return err
			}
		} else if err := os.Remove(fqn); err != nil && !os.IsNotExist(err) {
			glog.Errorf("%s: %v", j, err)
			return err
		}
		return nil
	})
}

func (j *cleanJ) jogBck() (size int64, err error) {
	opts := &fs.Options{
		Mi:       j.mpathInfo,
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

func (j *cleanJ) visitCT(parsedFQN fs.ParsedFQN, fqn string) {
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

func (j *cleanJ) visitLOM(parsedFQN fs.ParsedFQN) {
	if !j.allowDelObj {
		return
	}
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

func (j *cleanJ) walk(fqn string, de fs.DirEntry) error {
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

func (j *cleanJ) evict() (size int64, err error) {
	var (
		fevicted, bevicted int64
		capCheck           int64
		xlru               = j.ini.Xaction
	)
	// 1. rm older work
	for _, workfqn := range j.oldWork {
		finfo, erw := os.Stat(workfqn)
		if erw == nil {
			if err := cos.RemoveFile(workfqn); err != nil {
				glog.Warningf("Failed to remove old work %q: %v", workfqn, err)
			} else {
				size += finfo.Size()
			}
		}
	}
	j.oldWork = j.oldWork[:0]

	// 2. rm misplaced
	if rmMisplaced() {
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
				if capCheck, err = j.postRemove(capCheck, lom.SizeBytes(true /*not loaded*/)); err != nil {
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
			if capCheck, err = j.postRemove(capCheck, ct.SizeBytes()); err != nil {
				return
			}
		}
	}
	j.misplaced.ec = j.misplaced.ec[:0]

	j.ini.StatsT.Add(stats.StoreRmSize, bevicted)
	j.ini.StatsT.Add(stats.StoreRmCount, fevicted)
	xlru.ObjectsAdd(fevicted)
	xlru.BytesAdd(bevicted)
	return
}

func (j *cleanJ) postRemove(prev, size int64) (capCheck int64, err error) {
	j.totalSize -= size
	capCheck = prev + size
	if err = j.yieldTerm(); err != nil {
		return
	}
	// init, recompute, and throttle - once per capCheckThresh
	capCheck = 0
	j.allowDelObj, _ = j.allow()
	j.config = cmn.GCO.Get()
	j.now = time.Now().UnixNano()
	return
}

func (j *cleanJ) yieldTerm() error {
	xlru := j.ini.Xaction
	select {
	case <-xlru.ChanAbort():
		return cmn.NewErrAborted(xlru.Name(), "", nil)
	case <-j.stopCh:
		return cmn.NewErrAborted(xlru.Name(), "", nil)
	default:
		break
	}
	if xlru.Finished() {
		return cmn.NewErrAborted(xlru.Name(), "", nil)
	}
	return nil
}

func (j *cleanJ) allow() (ok bool, err error) {
	var (
		bowner = j.ini.T.Bowner()
		b      = cluster.NewBckEmbed(j.bck)
	)
	if err = b.Init(bowner); err != nil {
		return
	}
	ok = b.Props.LRU.Enabled && b.Allow(cmn.AccessObjDELETE) == nil
	return
}
