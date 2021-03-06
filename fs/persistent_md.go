// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	NodeRestartedMarker = "node_restarted"
	RebalanceMarker     = "rebalance"
	ResilverMarker      = "resilver"
	markersDirName      = ".ais.markers"

	BmdPersistedFileName = ".ais.bmd"
	BmdPersistedPrevious = BmdPersistedFileName + ".prev" // previous version

	VmdPersistedFileName = ".ais.vmd"
)

// List of AIS metadata files and directories (basenames only)
var mdFilesDirs = []string{
	markersDirName,

	BmdPersistedFileName,
	BmdPersistedPrevious,

	VmdPersistedFileName,
}

func MarkerExists(marker string) bool {
	markerPath := filepath.Join(markersDirName, marker)
	return len(FindPersisted(markerPath)) > 0
}

func PersistMarker(marker string) error {
	markerPath := filepath.Join(markersDirName, marker)
	cnt, available := PersistOnMpaths(markerPath, "", nil, 1)
	if cnt == 0 {
		return fmt.Errorf("failed to persist marker %q (available mountPaths: %d)", marker, available)
	}
	return nil
}

func RemoveMarker(marker string) {
	var (
		mpaths, _  = Get()
		markerPath = filepath.Join(markersDirName, marker)
	)
	for _, mpath := range mpaths {
		if err := cos.RemoveFile(filepath.Join(mpath.Path, markerPath)); err != nil {
			glog.Errorf("Failed to cleanup %q from %q, err: %v", markerPath, mpath.Path, err)
		}
	}
}

// PersistOnMpaths persists `what` on mountpaths under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// If `backupPath != ""`, it removes files from `backupPath` and moves files from `path` to `backupPath`.
// Returns how many times it has successfully stored a file.
func PersistOnMpaths(path, backupPath string, what interface{}, atMost int, opts ...jsp.Options) (cnt,
	availCnt int) {
	var (
		options            = jsp.CksumSign(0 /*metaver*/)
		availableMpaths, _ = Get()
	)
	availCnt = len(availableMpaths)
	if atMost == 0 {
		atMost = availCnt
	}
	if len(opts) > 0 {
		options = opts[0]
	}

	for _, mpath := range availableMpaths {
		moved := false

		// 1. (Optional) Move currently stored in `path` to `backupPath`.
		if backupPath != "" {
			moved = mpath.MoveMD(path, backupPath)
		} else if err := mpath.Remove(path); err != nil {
			glog.Error(err)
		}

		// 2. Persist `what` on `path`.
		if cnt < atMost {
			if err := mpath.StoreMD(path, what, options); err != nil {
				glog.Errorf("Failed to persist %q on %q, err: %v", path, mpath, err)
			} else {
				cnt++
			}
		}

		// 3. (Optional) Clean up very old versions of persisted data - only if they were not updated.
		if backupPath != "" && !moved {
			if err := mpath.Remove(backupPath); err != nil {
				glog.Error(err)
			}
		}
	}
	debug.Func(func() {
		expected := cos.Min(atMost, availCnt)
		debug.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead",
			path, expected, cnt)
	})
	return
}

func ClearMDOnAllMpaths() {
	available, _ := Get()
	for _, mpath := range available {
		mpath.ClearMDs()
	}
}

func RemoveDaemonIDs() {
	available, disabled := Get()
	for _, mpath := range available {
		if err := RemoveXattr(mpath.Path, daemonIDXattr); err != nil {
			glog.Warning(err)
		}
	}
	for _, mpath := range disabled {
		if err := RemoveXattr(mpath.Path, daemonIDXattr); err != nil {
			glog.Warning(err)
		}
	}
}

func FindPersisted(path string) MPI {
	var (
		avail, _ = Get()
		mpaths   = make(MPI, len(avail))
	)
	for mpath, mpathInfo := range avail {
		fpath := filepath.Join(mpath, path)
		if err := Access(fpath); err == nil {
			mpaths[mpath] = mpathInfo
		}
	}
	return mpaths
}
