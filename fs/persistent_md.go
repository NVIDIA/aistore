// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
)

// List of AIS metadata files and directories (basenames only)
var mdFilesDirs = []string{
	cmn.MarkersDirName,

	cmn.BmdFname,
	cmn.BmdPreviousFname,

	cmn.VmdFname,
}

func MarkerExists(marker string) bool {
	markerPath := filepath.Join(cmn.MarkersDirName, marker)
	return CountPersisted(markerPath) > 0
}

func PersistMarker(marker string) error {
	const numMarkers = 1
	var (
		relname            = filepath.Join(cmn.MarkersDirName, marker)
		availableMpaths, _ = Get()
		cnt                int
	)
	for _, mi := range availableMpaths {
		fpath := filepath.Join(mi.Path, relname)
		if err := Access(fpath); err == nil {
			cnt++
			if cnt > numMarkers {
				if err := cos.RemoveFile(fpath); err != nil {
					glog.Errorf("Failed to cleanup %q marker: %v", fpath, err)
				} else {
					cnt--
				}
			}
		} else if cnt < numMarkers {
			if file, err := cos.CreateFile(fpath); err == nil {
				file.Close()
				cnt++
			} else {
				glog.Errorf("Failed to create %q marker: %v", fpath, err)
			}
		}
	}
	if cnt == 0 {
		return fmt.Errorf("failed to persist %q marker (%d)", marker, len(availableMpaths))
	}
	return nil
}

func RemoveMarker(marker string) {
	var (
		availableMpaths, _ = Get()
		relname            = filepath.Join(cmn.MarkersDirName, marker)
	)
	for _, mi := range availableMpaths {
		if err := cos.RemoveFile(filepath.Join(mi.Path, relname)); err != nil {
			glog.Errorf("Failed to cleanup %q from %q: %v", relname, mi.Path, err)
		}
	}
}

// PersistOnMpaths persists `what` on mountpaths under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// If `backupPath != ""`, it removes files from `backupPath` and moves files from `path` to `backupPath`.
// Returns how many times it has successfully stored a file.
func PersistOnMpaths(fname, backupName string, meta jsp.Opts, atMost int, sgl *memsys.SGL) (cnt, availCnt int) {
	availableMpaths, _ := Get()
	availCnt = len(availableMpaths)
	if atMost == 0 {
		atMost = availCnt
	}
	for _, mi := range availableMpaths {
		var moved bool

		// 1. (Optional) Move/rename fname to backupName.
		if backupName != "" {
			moved = mi.move(fname, backupName)
		} else if err := mi.Remove(fname); err != nil {
			glog.Error(err)
		}
		// 2. Persist meta as fname.
		if cnt < atMost {
			fpath := filepath.Join(mi.Path, fname)
			if err := jsp.SaveMeta(fpath, meta, sgl); err != nil {
				glog.Errorf("Failed to persist %q on %q, err: %v", fname, mi, err)
			} else {
				cnt++
			}
		}
		// 3. (Optional) Cleanup old.
		if backupName != "" && !moved {
			if err := mi.Remove(backupName); err != nil {
				glog.Error(err)
			}
		}
	}
	debug.Func(func() {
		expected := cos.Min(atMost, availCnt)
		debug.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead",
			fname, expected, cnt)
	})
	return
}

func RemoveDaemonIDs() {
	available, disabled := Get()
	for _, mi := range available {
		err := removeXattr(mi.Path, daemonIDXattr)
		debug.AssertNoErr(err)
	}
	for _, mi := range disabled {
		err := removeXattr(mi.Path, daemonIDXattr)
		debug.AssertNoErr(err)
	}
}

func CountPersisted(fname string) (cnt int) {
	available, _ := Get()
	for mpath := range available {
		fpath := filepath.Join(mpath, fname)
		if err := Access(fpath); err == nil {
			cnt++
		}
	}
	return
}
