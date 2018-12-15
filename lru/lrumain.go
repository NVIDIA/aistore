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
	"github.com/NVIDIA/dfcpub/ios"
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
	minevict = cmn.MiB
)

type (
	InitLRU struct {
		Riostat     *ios.IostatRunner
		Ratime      *atime.Runner
		Xlru        cmn.XactInterface
		Namelocker  cluster.NameLocker
		Bmdowner    cluster.Bowner
		Statsif     stats.Tracker
		Targetif    cluster.Target
		CtxResolver *fs.ContentSpecMgr
	}

	fileInfo struct {
		fqn     string
		usetime time.Time
		size    int64
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
		ini         InitLRU
		fs          string
		bucketdir   string
		throttler   cluster.Throttler
		atimeRespCh chan *atime.Response
		ctxResolver *fs.ContentSpecMgr
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
	glog.Infof("LRU: %s started: dont-evict-time %v", ini.Xlru, cmn.GCO.Get().LRU.DontEvictTime)

	ini.Ratime = ini.Targetif.GetAtimeRunner()
	ini.Bmdowner = ini.Targetif.GetBowner()

	availablePaths, _ := fs.Mountpaths.Get()
	for contentType, contentResolver := range ini.CtxResolver.RegisteredContentTypes {
		if !contentResolver.PermToEvict() {
			continue
		}
		//
		// NOTE the sequence: LRU local buckets first, Cloud buckets - second
		//
		for path, mpathInfo := range availablePaths {
			lctx := newlru(ini, mpathInfo, fs.Mountpaths.MakePathLocal(path, contentType))
			wg.Add(1)
			go lctx.jog(wg)
		}
		wg.Wait()
		for path, mpathInfo := range availablePaths {
			lctx := newlru(ini, mpathInfo, fs.Mountpaths.MakePathCloud(path, contentType))
			wg.Add(1)
			go lctx.jog(wg)
		}
		wg.Wait()
	}
}

func newlru(ini *InitLRU, mpathInfo *fs.MountpathInfo, bucketdir string) *lructx {
	throttler := &cluster.Throttle{
		Riostat: ini.Riostat,
		Path:    mpathInfo.Path,
		FS:      mpathInfo.FileSystem,
		Flag:    cluster.OnDiskUtil | cluster.OnFSUsed}
	lctx := &lructx{
		oldwork:     make([]*fileInfo, 0, 64),
		ini:         *ini,
		fs:          mpathInfo.FileSystem,
		bucketdir:   bucketdir,
		throttler:   throttler,
		atimeRespCh: make(chan *atime.Response, 1),
		ctxResolver: ini.CtxResolver,
	}
	return lctx
}
