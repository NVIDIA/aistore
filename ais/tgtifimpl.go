// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/housekeep/lru"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
)

//
// implements cluster.Target interfaces
//

var _ cluster.Target = &targetrunner{}

func (t *targetrunner) GetBowner() cluster.Bowner    { return t.bmdowner }
func (t *targetrunner) FSHC(err error, path string)  { t.fshc(err, path) }
func (t *targetrunner) GetMem2() *memsys.Mem2        { return nodeCtx.mm }
func (t *targetrunner) GetFSPRG() fs.PathRunGroup    { return &t.fsprg }
func (t *targetrunner) GetSmap() *cluster.Smap       { return t.smapowner.Get() }
func (t *targetrunner) Snode() *cluster.Snode        { return t.si }
func (t *targetrunner) Cloud() cluster.CloudProvider { return t.cloud }
func (t *targetrunner) PrefetchQueueLen() int        { return len(t.prefetchQueue) }

func (t *targetrunner) IsRebalancing() bool {
	_, running := t.xactions.isRebalancing(cmn.ActGlobalReb)
	_, runningLocal := t.xactions.isRebalancing(cmn.ActLocalReb)
	return running || runningLocal
}

func (t *targetrunner) AvgCapUsed(config *cmn.Config, used ...int32) (avgCapUsed int32, oos bool) {
	if len(used) > 0 {
		t.capUsed.Lock()
		t.capUsed.used = used[0]
		if t.capUsed.oos && t.capUsed.used < int32(config.LRU.HighWM) {
			t.capUsed.oos = false
		} else if !t.capUsed.oos && t.capUsed.used > int32(config.LRU.OOS) {
			t.capUsed.oos = true
		}
		avgCapUsed, oos = t.capUsed.used, t.capUsed.oos
		t.capUsed.Unlock()
	} else {
		t.capUsed.RLock()
		avgCapUsed, oos = t.capUsed.used, t.capUsed.oos
		t.capUsed.RUnlock()
	}
	return
}

// gets triggered by the stats evaluation of a remaining capacity
// and then runs in a goroutine - see stats package, target_stats.go
func (t *targetrunner) RunLRU() {
	if t.IsRebalancing() {
		glog.Infoln("Warning: rebalancing (local or global) is in progress, skipping LRU run")
		return
	}
	xlru := t.xactions.renewLRU()
	if xlru == nil {
		return
	}
	ini := lru.InitLRU{
		Xlru:                xlru,
		Statsif:             t.statsif,
		T:                   t,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
	}
	lru.InitAndRun(&ini) // blocking

	xlru.EndTime(time.Now())
}

func (t *targetrunner) Prefetch() {
	xpre := t.xactions.renewPrefetch(getstorstatsrunner())

	if xpre == nil {
		return
	}
loop:
	for {
		select {
		case fwd := <-t.prefetchQueue:
			if !fwd.deadline.IsZero() && time.Now().After(fwd.deadline) {
				continue
			}
			bckIsLocal, _ := t.bmdowner.get().ValidateBucket(fwd.bucket, fwd.bckProvider)
			if bckIsLocal {
				glog.Errorf("prefetch: bucket %s is local, nothing to do", fwd.bucket)
			} else {
				for _, objname := range fwd.objnames {
					t.prefetchMissing(fwd.ctx, objname, fwd.bucket, fwd.bckProvider)
				}
			}
			// Signal completion of prefetch
			if fwd.done != nil {
				fwd.done <- struct{}{}
			}
		default:
			// When there is nothing left to fetch, the prefetch routine ends
			break loop

		}
	}
	xpre.EndTime(time.Now())
}

// slight variation vs t.httpobjget()
func (t *targetrunner) GetObject(w io.Writer, lom *cluster.LOM, started time.Time) error {
	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     context.Background(),
	}
	err, _ := goi.getObject()
	return err
}

// slight variation vs t.doPut() above
func (t *targetrunner) PutObject(workFQN string, reader io.ReadCloser, lom *cluster.LOM,
	recvType cluster.RecvType, cksum cmn.Cksummer, started time.Time) error {

	poi := &putObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		r:       reader,
		workFQN: workFQN,
		ctx:     context.Background(),
	}
	if recvType == cluster.ColdGet {
		poi.cold = true
		poi.cksumToCheck = cksum
	}
	err, _ := poi.putObject()
	return err
}

func (t *targetrunner) CopyObject(lom *cluster.LOM, bucketTo string, buf []byte, uncache bool) (err error) {
	ri := &replicInfo{smap: t.smapowner.get(), bucketTo: bucketTo, t: t, buf: buf, uncache: uncache}
	_, err = ri.copyObject(lom, lom.Objname)
	return
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) GetCold(ct context.Context, lom *cluster.LOM, prefetch bool) (err error, errCode int) {
	if prefetch {
		if !cluster.ObjectLocker.TryLock(lom.Uname(), true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return cmn.NewSkipError(), 0
		}
	} else {
		cluster.ObjectLocker.Lock(lom.Uname(), true) // one cold-GET at a time
	}
	var (
		vchanged, crace bool
		workFQN         = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	)
	if err, errCode = t.cloud.getObj(ct, workFQN, lom); err != nil {
		err = fmt.Errorf("%s: GET failed, err: %v", lom, err)
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
		return
	}
	defer func() {
		if err != nil {
			cluster.ObjectLocker.Unlock(lom.Uname(), true)
			if errRemove := os.Remove(workFQN); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", err, workFQN, errRemove)
				t.fshc(errRemove, workFQN)
			}
		}
	}()
	if err = cmn.Rename(workFQN, lom.FQN); err != nil {
		err = fmt.Errorf("unexpected failure to rename %s => %s, err: %v", workFQN, lom.FQN, err)
		t.fshc(err, lom.FQN)
		return
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()

	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	if prefetch {
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
	} else {
		if vchanged {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1},
				stats.NamedVal64{stats.GetColdSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeCount, 1})
		} else if !crace {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1}, stats.NamedVal64{stats.GetColdSize, lom.Size()})
		}
		cluster.ObjectLocker.DowngradeLock(lom.Uname())
	}
	return
}

//
// implements health.fspathDispatcher interface
//

func (t *targetrunner) DisableMountpath(mountpath string, reason string) (disabled bool, err error) {
	glog.Warningf("Disabling mountpath %s: %s", mountpath, reason)
	t.xactions.stopMountpathXactions()
	return t.fsprg.disableMountpath(mountpath)
}
