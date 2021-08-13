// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
)

const numMarkers = 1

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

func PersistMarker(marker string) (fatalErr, writeErr error) {
	var (
		relname            = filepath.Join(cmn.MarkersDirName, marker)
		availableMpaths, _ = Get()
		cnt                int
	)
	if len(availableMpaths) == 0 {
		fatalErr = ErrNoMountpaths
		return
	}
	for _, mi := range availableMpaths {
		fpath := filepath.Join(mi.Path, relname)
		if err := Access(fpath); err == nil {
			cnt++
			if cnt > numMarkers {
				if err := cos.RemoveFile(fpath); err != nil {
					if writeErr == nil {
						writeErr = err
					}
					glog.Errorf("Failed to cleanup %q marker: %v", fpath, err)
				} else {
					cnt--
				}
			}
		} else if cnt < numMarkers {
			if file, err := cos.CreateFile(fpath); err == nil {
				writeErr = err
				file.Close()
				cnt++
			} else {
				glog.Errorf("Failed to create %q marker: %v", fpath, err)
			}
		}
	}
	if cnt == 0 {
		fatalErr = fmt.Errorf("failed to persist %q marker (%d)", marker, len(availableMpaths))
	}
	return
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
func PersistOnMpaths(fname, backupName string, meta jsp.Opts, atMost int, b []byte, sgl *memsys.SGL) (cnt, availCnt int) {
	var (
		wto                io.WriterTo
		availableMpaths, _ = Get()
		bcnt               int
	)
	availCnt = len(availableMpaths)
	debug.Assert(atMost > 0)
	if atMost > availCnt {
		atMost = availCnt
	}
	for _, mi := range availableMpaths {
		if backupName != "" {
			bcnt = mi.backupAtmost(fname, backupName, bcnt, atMost)
		}
		fpath := filepath.Join(mi.Path, fname)
		os.Remove(fpath)
		if cnt >= atMost {
			continue
		}
		if b != nil {
			wto = bytes.NewBuffer(b)
		} else if sgl != nil {
			wto = sgl // not reopening - see sgl.WriteTo()
		}
		if err := jsp.SaveMeta(fpath, meta, wto); err != nil {
			glog.Errorf("Failed to persist %q on %q, err: %v", fname, mi, err)
		} else {
			cnt++
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
