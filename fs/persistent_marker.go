// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

const (
	NodeRestartedMarker = ".ais.noderestarted"
	RebalanceMarker     = ".ais.rebalance_marker"
	ResilverMarker      = ".ais.resilver_marker"

	BmdPersistedFileName = ".ais.bmd"
	BmdPersistedPrevious = BmdPersistedFileName + ".prev" // previous version

	VmdPersistedFileName = ".ais.vmd"
)

// List of AIS metadata files stored
var mdFiles = []string{
	NodeRestartedMarker,

	RebalanceMarker,
	ResilverMarker,

	BmdPersistedFileName,
	BmdPersistedPrevious,

	VmdPersistedFileName,
}

func MarkerExists(marker string) bool {
	return len(FindPersisted(marker)) > 0
}

func PersistMarker(marker string) error {
	cnt, available := PersistOnMpaths(marker, "", nil, 1)
	if cnt == 0 {
		return fmt.Errorf("failed to persist marker %q (available mountPaths: %d)", marker, available)
	}
	return nil
}

// PersistOnMpaths persists `what` on mountpaths under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// If `backupPath != ""`, it removes files from `backupPath` and moves files from `path` to `backupPath`.
// Returns how many times it has successfully stored a file.
func PersistOnMpaths(path, backupPath string, what interface{}, atMost int, opts ...jsp.Options) (cnt,
	availCnt int) {
	var (
		options            = jsp.CksumSign()
		availableMpaths, _ = Get()
	)

	if atMost == 0 {
		atMost = len(availableMpaths)
	}
	if len(opts) > 0 {
		options = opts[0]
	}

	for _, mpath := range availableMpaths {
		moved := false

		// 1. (Optional) Move currently stored in `path` to `backupPath`.
		if backupPath != "" {
			moved = movePersistedFile(filepath.Join(mpath.Path, path), filepath.Join(mpath.Path, backupPath))
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
			if err := os.Remove(filepath.Join(mpath.Path, backupPath)); err != nil && !os.IsNotExist(err) {
				glog.Error(err)
			}
		}
	}

	if debug.Enabled {
		expected := cmn.Min(atMost, len(availableMpaths))
		cmn.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead", path, expected, cnt)
	}

	return
}

func movePersistedFile(from, to string) bool {
	err := os.Rename(from, to)
	if err != nil && !os.IsNotExist(err) {
		glog.Error(err)
	}
	return err == nil
}

func ClearMDOnMpath(mpath *MountpathInfo) {
	for _, path := range mdFiles {
		fpath := filepath.Join(mpath.Path, path)
		os.Remove(fpath)
	}
}

func RemovePersisted(path string) {
	mpaths, _ := Get()
	for _, mpath := range mpaths {
		if err := os.Remove(filepath.Join(mpath.Path, path)); err != nil && !os.IsNotExist(err) {
			glog.Errorf("Failed to cleanup %q from %q, err: %v", path, mpath.Path, err)
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
