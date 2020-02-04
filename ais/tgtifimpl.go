// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	"github.com/NVIDIA/aistore/xaction"
)

//
// implements cluster.Target interfaces
//

var _ cluster.Target = &targetrunner{}

func (t *targetrunner) GetBowner() cluster.Bowner    { return t.bmdowner }
func (t *targetrunner) GetSowner() cluster.Sowner    { return t.smapowner }
func (t *targetrunner) FSHC(err error, path string)  { t.fshc(err, path) }
func (t *targetrunner) GetMMSA() *memsys.MMSA        { return daemon.gmm }
func (t *targetrunner) GetSmallMMSA() *memsys.MMSA   { return daemon.smm }
func (t *targetrunner) GetFSPRG() fs.PathRunGroup    { return &t.fsprg }
func (t *targetrunner) Snode() *cluster.Snode        { return t.si }
func (t *targetrunner) Cloud() cluster.CloudProvider { return t.cloud }
func (t *targetrunner) PrefetchQueueLen() int        { return len(t.prefetchQueue) }

func (t *targetrunner) GetGFN(gfnType cluster.GFNType) cluster.GFN {
	switch gfnType {
	case cluster.GFNLocal:
		return &t.gfn.local
	case cluster.GFNGlobal:
		return &t.gfn.global
	}
	cmn.AssertMsg(false, "Invalid GFN type")
	return nil
}

func (t *targetrunner) RebalanceInfo() cluster.RebalanceInfo {
	_, running := xaction.Registry.IsRebalancing(cmn.ActGlobalReb)
	_, runningLocal := xaction.Registry.IsRebalancing(cmn.ActLocalReb)
	return cluster.RebalanceInfo{
		IsRebalancing: running || runningLocal,
		GlobalRebID:   t.rebManager.GlobRebID(),
	}
}

func (t *targetrunner) AvgCapUsed(config *cmn.Config, used ...int32) (capInfo cmn.CapacityInfo) {
	if config == nil {
		config = cmn.GCO.Get()
	}
	if len(used) > 0 {
		t.capUsed.Lock()
		t.capUsed.used = used[0]
		if t.capUsed.oos && int64(t.capUsed.used) < config.LRU.HighWM {
			t.capUsed.oos = false
		} else if !t.capUsed.oos && int64(t.capUsed.used) > config.LRU.OOS {
			t.capUsed.oos = true
		}
		capInfo.UsedPct, capInfo.OOS = t.capUsed.used, t.capUsed.oos
		t.capUsed.Unlock()
	} else {
		t.capUsed.RLock()
		capInfo.UsedPct, capInfo.OOS = t.capUsed.used, t.capUsed.oos
		t.capUsed.RUnlock()
	}
	capInfo.High = int64(capInfo.UsedPct) > config.LRU.HighWM
	if capInfo.OOS || capInfo.High {
		capInfo.Err = cmn.NewErrorCapacityExceeded(t.si.Name(), config.LRU.HighWM, capInfo.UsedPct, capInfo.OOS)
	}
	return
}

// gets triggered by the stats evaluation of a remaining capacity
// and then runs in a goroutine - see stats package, target_stats.go
func (t *targetrunner) RunLRU() {
	if t.RebalanceInfo().IsRebalancing {
		glog.Infoln("Warning: rebalancing (local or global) is in progress, skipping LRU run")
		return
	}
	xlru := xaction.Registry.RenewLRU()
	if xlru == nil {
		return
	}
	ini := lru.InitLRU{
		T:                   t,
		Xaction:             xlru,
		StatsT:              t.statsT,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
	}
	lru.InitAndRun(&ini) // blocking

	xlru.EndTime(time.Now())
}

func (t *targetrunner) Prefetch() {
	xpre := xaction.Registry.RenewPrefetch(getstorstatsrunner())

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
			if err := fwd.bck.Init(t.bmdowner); err != nil {
				glog.Errorf("prefetch: %s, err: %v", fwd.bck, err)
			} else if fwd.bck.IsAIS() {
				glog.Errorf("prefetch: %s is ais bucket, nothing to do", fwd.bck)
			} else {
				for _, objname := range fwd.objnames {
					t.prefetchMissing(fwd.ctx, objname, fwd.bck)
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
func (t *targetrunner) PutObject(workFQN string, reader io.ReadCloser, lom *cluster.LOM, recvType cluster.RecvType, cksum *cmn.Cksum, started time.Time) error {
	poi := &putObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		r:       reader,
		workFQN: workFQN,
		ctx:     context.Background(),
	}
	if recvType == cluster.Migrated {
		poi.cksumToCheck = cksum
		poi.migrated = true
	} else if recvType == cluster.ColdGet {
		poi.cold = true
		poi.cksumToCheck = cksum
	}
	err, _ := poi.putObject()
	return err
}

func (t *targetrunner) CopyObject(lom *cluster.LOM, bckTo *cluster.Bck, buf []byte, localOnly bool) (copied bool, err error) {
	ri := &replicInfo{smap: t.smapowner.get(),
		bckTo:     bckTo,
		t:         t,
		buf:       buf,
		localOnly: localOnly,
		uncache:   false,
		finalize:  false,
	}
	copied, err = ri.copyObject(lom, lom.Objname)
	return
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) GetCold(ct context.Context, lom *cluster.LOM, prefetch bool) (err error, errCode int) {
	if prefetch {
		if !lom.TryLock(true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return cmn.ErrSkip, 0
		}
	} else {
		lom.Lock(true) // one cold-GET at a time
	}
	var (
		vchanged, crace bool
		workFQN         = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	)
	if err, errCode = t.cloud.getObj(ct, workFQN, lom); err != nil {
		err = fmt.Errorf("%s: GET failed, err: %v", lom, err)
		lom.Unlock(true)
		return
	}
	defer func() {
		if err != nil {
			lom.Unlock(true)
			if errRemove := cmn.RemoveFile(workFQN); errRemove != nil {
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
		lom.Unlock(true)
	} else {
		if vchanged {
			t.statsT.AddMany(
				stats.NamedVal64{Name: stats.GetColdCount, Value: 1},
				stats.NamedVal64{Name: stats.GetColdSize, Value: lom.Size()},
				stats.NamedVal64{Name: stats.VerChangeSize, Value: lom.Size()},
				stats.NamedVal64{Name: stats.VerChangeCount, Value: 1},
			)
		} else if !crace {
			t.statsT.AddMany(
				stats.NamedVal64{Name: stats.GetColdCount, Value: 1},
				stats.NamedVal64{Name: stats.GetColdSize, Value: lom.Size()},
			)
		}
		lom.DowngradeLock()
	}
	return
}

func (t *targetrunner) PromoteFile(srcFQN string, bck *cluster.Bck, objName string, overwrite, safe, verbose bool) (err error) {
	if err = bck.AllowPUT(); err != nil {
		return
	}
	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bck.Bck); err != nil {
		return
	}
	// local or remote?
	var (
		si   *cluster.Snode
		smap = t.smapowner.get()
	)
	if si, err = cluster.HrwTarget(lom.Uname(), &smap.Smap); err != nil {
		return
	}
	// remote
	if si.ID() != t.si.ID() {
		if verbose {
			glog.Infof("promote/PUT %s => %s @ %s", srcFQN, lom, si.ID())
		}
		buf, slab := daemon.gmm.Alloc()
		lom.FQN = srcFQN
		ri := &replicInfo{smap: smap, t: t, bckTo: lom.Bck(), buf: buf, localOnly: false}

		// TODO -- FIXME: handle overwrite (lookup first)
		_, err = ri.putRemote(lom, lom.Objname, si)
		slab.Free(buf)
		return
	}

	// local
	err = lom.Load(false)
	if err == nil && !overwrite {
		err = fmt.Errorf("%s already exists", lom)
		return
	}
	if verbose {
		s := ""
		if overwrite {
			s = "+"
		}
		glog.Infof("promote%s %s => %s", s, srcFQN, lom)
	}
	var (
		cksum   *cmn.Cksum
		written int64
		workFQN string
		poi     = &putObjInfo{t: t, lom: lom}
	)

	if safe {
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

		buf, slab := daemon.gmm.Alloc()
		written, cksum, err = cmn.CopyFile(srcFQN, workFQN, buf, true)
		slab.Free(buf)
		if err != nil {
			return
		}
		lom.SetCksum(cksum)
	} else {
		workFQN = srcFQN // use the file as it would be intermediate (work) file
		fi, err := os.Stat(srcFQN)
		if err != nil {
			return err
		}
		written = fi.Size()
	}
	cmn.Assert(workFQN != "")
	poi.workFQN = workFQN

	lom.SetSize(written)
	err, _ = poi.finalize()
	return
}

//
// implements health.fspathDispatcher interface
//

func (t *targetrunner) DisableMountpath(mountpath string, reason string) (disabled bool, err error) {
	glog.Warningf("Disabling mountpath %s: %s", mountpath, reason)
	return t.fsprg.disableMountpath(mountpath)
}

func (t *targetrunner) Health(si *cluster.Snode, includeReb bool, timeout time.Duration) ([]byte, error) {
	query := url.Values{}
	if includeReb {
		query.Add(cmn.URLParamRebStatus, "true")
	}
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   si.URL(cmn.NetworkIntraControl),
			Path:   cmn.URLPath(cmn.Version, cmn.Health),
			Query:  query,
		},
		timeout: timeout,
	}
	res := t.call(args)
	return res.outjson, res.err
}

func (t *targetrunner) RebalanceNamespace(si *cluster.Snode) ([]byte, int, error) {
	// pull the data
	query := url.Values{}
	header := make(http.Header)
	header.Set(cmn.HeaderCallerID, t.si.ID())
	query.Add(cmn.URLParamRebData, "true")
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Header: header,
			Base:   si.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPath(cmn.Version, cmn.Rebalance),
			Query:  query,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := t.call(args)
	return res.outjson, res.status, res.err
}
