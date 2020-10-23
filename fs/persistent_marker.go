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
	VmdPersistedFileName = ".ais.vmd"
)

func MarkerExists(marker string) bool {
	return len(FindPersisted(marker)) > 0
}

func PersistMarker(marker string) error {
	cnt, available := PersistOnMpaths(marker, nil, 1)
	if cnt == 0 {
		return fmt.Errorf("failed to persist marker %q (available mountPaths: %d)", marker, available)
	}
	return nil
}

func RemoveMarker(marker string) {
	markerMpaths := FindPersisted(marker)
	if len(markerMpaths) == 0 {
		return
	}

	for _, mp := range markerMpaths {
		if err := os.Remove(filepath.Join(mp.Path, marker)); err != nil {
			glog.Errorf("failed to cleanup %q marker from %q: %v", marker, mp.Path, err)
		}
	}
}

// PersistOnMpaths persists `what` on every mountpath under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// Returns how many times it successfully stored a file.
func PersistOnMpaths(path string, what interface{}, atMost int, opts ...jsp.Options) (cnt, availCnt int) {
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

	for _, mountPath := range availableMpaths {
		if err := persistOnSingleMpath(mountPath, path, what, options); err != nil {
			glog.Errorf("Failed to persist %q on %q, err: %v", path, mountPath, err)
		} else {
			cnt++
		}

		if cnt == atMost {
			break
		}
	}

	if debug.Enabled {
		expected := cmn.Min(atMost, len(availableMpaths))
		cmn.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead", path, expected, cnt)
	}

	return
}

func MovePersisted(oldPath, newPath string) (cnt int) {
	oldMpaths := FindPersisted(oldPath)
	for mpath := range oldMpaths {
		oldFpath := filepath.Join(mpath, oldPath)
		newFpath := filepath.Join(mpath, newPath)

		if err := os.Rename(oldFpath, newFpath); err != nil {
			glog.Errorf("failed to rename %q => %q, err: %v", oldFpath, newFpath, err)
			continue
		}
		cnt++
	}

	return
}

func RemovePersisted(path string) {
	mpaths := FindPersisted(path)
	for mpath := range mpaths {
		if err := os.Remove(filepath.Join(mpath, path)); err != nil {
			glog.Error(err)
		}
	}
}

func persistOnSingleMpath(mpath *MountpathInfo, path string, what interface{}, options jsp.Options) (err error) {
	fpath := filepath.Join(mpath.Path, path)
	if what == nil {
		_, err = cmn.CreateFile(fpath)
	} else {
		err = jsp.Save(fpath, what, options)
	}
	return err
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
