// Package lru provides atime-based least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orhaned workfiles.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/stats"
)

// ============================================= Summary ===========================================
//
// The LRU module implements a well-known least-recently-used cache replacement policy.
//
// In DFC, LRU-driven eviction is based on the two configurable watermarks: config.LRU.LowWM and
// config.LRU.HighWM (section "lru_config" in the setup/config.sh).
// When and if exceeded, DFC storage target will start gradually evicting objects from its
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
	minEvictThresh   = cmn.MiB
	capCheckInterval = cmn.MiB * 256 // capacity checking "interval"
)

type (
	InitLRU struct {
		Ratime     *atime.Runner
		Xlru       cmn.Xact
		Namelocker cluster.NameLocker
		Statsif    stats.Tracker
		T          cluster.Target
	}

	fileInfo struct {
		fqn string
		lom *cluster.LOM
		old bool
	}
	fileInfoMinHeap []*fileInfo

	// lructx represents a single LRU context that runs in a single goroutine (worker)
	// that traverses and evicts a single given filesystem, or more exactly,
	// subtree in this filesystem identified by the bucketdir
	lructx struct {
		// runtime
		cursize int64
		totsize int64
		newest  time.Time
		heap    *fileInfoMinHeap
		oldwork []*fileInfo
		// init-time
		ini             InitLRU
		stopCh          chan struct{}
		joggers         map[string]*lructx
		mpathInfo       *fs.MountpathInfo
		contentType     string
		bckTypeDir      string
		contentResolver fs.ContentResolver
		config          *cmn.Config
		atimeRespCh     chan *atime.Response
		dontevictime    time.Time
		bislocal        bool
		throttle        bool
		aborted         bool
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
	glog.Infof("LRU: %s started: dont-evict-time %v", ini.Xlru, config.LRU.DontEvictTime)

	ini.Ratime = ini.T.GetAtimeRunner()
	availablePaths, _ := fs.Mountpaths.Get()
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		if !contentResolver.PermToEvict() {
			continue
		}
		// TODO: extend LRU for other content types
		if contentType != fs.WorkfileType && contentType != fs.ObjectType {
			glog.Warningf("Skipping content type %q", contentType)
			continue
		}
		//
		// NOTE the sequence: Cloud buckets first, local buckets second
		//
		startLRUJoggers := func(bislocal bool) (aborted bool) {
			joggers := make(map[string]*lructx, len(availablePaths))
			errCh := make(chan struct{}, len(availablePaths))
			for mpath, mpathInfo := range availablePaths {
				joggers[mpath] = newlru(ini, mpathInfo, contentType, contentResolver, config, bislocal)
			}
			for _, j := range joggers {
				wg.Add(1)
				go j.jog(wg, joggers, errCh)
			}
			wg.Wait()
			close(errCh)
			select {
			case <-errCh:
				aborted = true
			default:
				break
			}
			return
		}
		if aborted := startLRUJoggers(false /*cloud*/); aborted {
			break
		}

		if config.LRU.LRULocalBuckets {
			if aborted := startLRUJoggers(true /*local*/); aborted {
				break
			}
		}
	}
}

func newlru(ini *InitLRU, mpathInfo *fs.MountpathInfo, contentType string, contentResolver fs.ContentResolver, config *cmn.Config, bislocal bool) *lructx {
	lctx := &lructx{
		oldwork:         make([]*fileInfo, 0, 64),
		ini:             *ini,
		stopCh:          make(chan struct{}, 1),
		mpathInfo:       mpathInfo,
		contentType:     contentType,
		contentResolver: contentResolver,
		config:          config,
		atimeRespCh:     make(chan *atime.Response, 1),
		bislocal:        bislocal,
	}
	return lctx
}

func stopAll(joggers map[string]*lructx, exceptMpath string) {
	for _, j := range joggers {
		if j.mpathInfo.Path == exceptMpath {
			continue
		}
		j.stopCh <- struct{}{}
	}
}
