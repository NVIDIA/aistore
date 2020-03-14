// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orhaned workfiles.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// ============================================= Summary ===========================================
//
// The LRU module implements a well-known least-recently-used cache replacement policy.
//
// LRU-driven eviction is based on the two configurable watermarks: config.LRU.LowWM and
// config.LRU.HighWM (section "lru" in the /deploy/dev/local/aisnode_config.sh).
// When and if exceeded, AIStore target will start gradually evicting objects from its
// stable storage: oldest first access-time wise.
//
// LRU is implemented as a so-called extended action (aka x-action, see xaction.go) that gets
// triggered when/if a used local capacity exceeds high watermark (config.LRU.HighWM). LRU then
// runs automatically. In order to reduce its impact on the live workload, LRU throttles itself
// in accordance with the current storage-target's utilization (see xaction_throttle.go).
//
// There's only one API that this module provides to the rest of the code:
//   - runLRU - to initiate a new LRU extended action on the local target
// All other methods are private to this module and are used only internally.
//
// ============================================= Summary ===========================================

// LRU defaults/tunables
const (
	minEvictThresh = 10 * cmn.MiB
	capCheckThresh = 256 * cmn.MiB // capacity checking threshold, when exceeded may result in lru throttling
)

type (
	InitLRU struct {
		T                   cluster.Target
		Xaction             *Xaction
		StatsT              stats.Tracker
		GetFSUsedPercentage func(path string) (usedPercentage int64, ok bool)
		GetFSStats          func(path string) (blocks, bavail uint64, bsize int64, err error)
	}
	fileInfoMinHeap []*cluster.LOM

	// lruCtx represents a single LRU context that runs in a single goroutine (worker)
	// that traverses and evicts a single given filesystem, or more exactly,
	// subtree in this filesystem identified by the bucketdir
	lruCtx struct {
		// runtime
		curSize   int64
		totalSize int64
		newest    time.Time
		heap      *fileInfoMinHeap
		oldWork   []string
		misplaced []*cluster.LOM
		// init-time
		ini        InitLRU
		stopCh     chan struct{}
		joggers    map[string]*lruCtx
		mpathInfo  *fs.MountpathInfo
		bck        cmn.Bck
		bckTypeDir string
		config     *cmn.Config
		throttle   bool
		aborted    bool
	}

	Xaction struct {
		cmn.MountpathXact
		cmn.XactBase
	}
)

//====================== LRU: initiation  ======================================
//
// construct per local subdir LRU joggers and run them all;
// serialize "cloud/" and "local/" traversals
//
//==============================================================================

func InitAndRun(ini *InitLRU) {
	wg := &sync.WaitGroup{}
	config := cmn.GCO.Get()
	glog.Infof("LRU: %s started: dont-evict-time %v", ini.Xaction, config.LRU.DontEvictTime)

	availablePaths, _ := fs.Mountpaths.Get()
	startLRUJoggers := func(provider string) (aborted bool) {
		joggers := make(map[string]*lruCtx, len(availablePaths))
		errCh := make(chan struct{}, len(availablePaths))

		for mpath, mpathInfo := range availablePaths {
			bck := cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
			joggers[mpath] = newLRU(ini, mpathInfo, bck, config)
		}
		for _, j := range joggers {
			wg.Add(1)
			go j.jog(wg, joggers, errCh)
		}
		wg.Wait()
		close(errCh)
		select {
		case _, ok := <-errCh:
			aborted = ok
		default:
			break
		}
		return
	}

	// NOTE: the sequence: cloud buckets first, ais buckets second
	if config.Cloud.Supported {
		if aborted := startLRUJoggers(config.Cloud.Provider); aborted {
			return
		}
	}

	if aborted := startLRUJoggers(cmn.ProviderAIS); aborted {
		return
	}
}

func newLRU(ini *InitLRU, mpathInfo *fs.MountpathInfo, bck cmn.Bck, config *cmn.Config) *lruCtx {
	return &lruCtx{
		oldWork:   make([]string, 0, 64),
		misplaced: make([]*cluster.LOM, 0, 64),
		ini:       *ini,
		stopCh:    make(chan struct{}, 1),
		mpathInfo: mpathInfo,
		bck:       bck,
		config:    config,
	}
}

func stopAll(joggers map[string]*lruCtx, exceptMpath string) {
	for _, j := range joggers {
		if j.mpathInfo.Path == exceptMpath {
			continue
		}
		j.stopCh <- struct{}{}
	}
}

func (xact *Xaction) Description() string {
	return "LRU-based cache eviction and free capacity management"
}
