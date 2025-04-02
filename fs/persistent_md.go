// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"
)

// NOTE: tunable
const numMarkers = 1

// List of AIS metadata files and directories (basenames only)
var mdFilesDirs = [...]string{
	fname.MarkersDir,
	fname.Bmd,
	fname.BmdPrevious,
	fname.Vmd,
}

func MarkerExists(marker string) bool {
	markerPath := filepath.Join(fname.MarkersDir, marker)
	return CountPersisted(markerPath) > 0
}

func PersistMarker(marker string) (fatalErr, writeErr error) {
	var (
		cnt     int
		relname = filepath.Join(fname.MarkersDir, marker)
		avail   = GetAvail()
	)
	if len(avail) == 0 {
		fatalErr = cmn.ErrNoMountpaths
		return fatalErr, writeErr
	}
	for _, mi := range avail {
		fpath := filepath.Join(mi.Path, relname)
		if err := cos.Stat(fpath); err == nil {
			cnt++
			if cnt > numMarkers {
				if err := cos.RemoveFile(fpath); err != nil {
					if writeErr == nil {
						writeErr = err
					}
					nlog.Errorf("Failed to cleanup %q marker: %v", fpath, err)
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
				nlog.Errorf("Failed to create %q marker: %v", fpath, err)
			}
		}
	}
	if cnt == 0 {
		fatalErr = fmt.Errorf("failed to persist %q marker (%d)", marker, len(avail))
	}
	return fatalErr, writeErr
}

func RemoveMarker(marker string, stup cos.StatsUpdater) (err error) {
	var (
		avail   = GetAvail()
		relname = filepath.Join(fname.MarkersDir, marker)
	)
	for _, mi := range avail {
		if er1 := cos.RemoveFile(filepath.Join(mi.Path, relname)); er1 != nil {
			err = er1
		}
	}
	switch marker {
	case fname.RebalanceMarker:
		stup.ClrFlag(cos.NodeAlerts, cos.RebalanceInterrupted|cos.Rebalancing)
	case fname.ResilverMarker:
		stup.ClrFlag(cos.NodeAlerts, cos.ResilverInterrupted|cos.Resilvering)
	case fname.NodeRestartedPrev:
		stup.ClrFlag(cos.NodeAlerts, cos.NodeRestarted)
	}
	return err
}

// PersistOnMpaths persists `what` on mountpaths under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// If `backupPath != ""`, it removes files from `backupPath` and moves files from `path` to `backupPath`.
// Returns how many times it has successfully stored a file.
func PersistOnMpaths(fname, backupName string, meta jsp.Opts, atMost int, b []byte, sgl *memsys.SGL) (cnt, availCnt int) {
	var (
		wto   cos.WriterTo2
		bcnt  int
		avail = GetAvail()
	)
	availCnt = len(avail)
	debug.Assert(atMost > 0)
	if atMost > availCnt {
		atMost = availCnt
	}
	for _, mi := range avail {
		if backupName != "" {
			bcnt = mi.backupAtmost(fname, backupName, bcnt, atMost)
		}
		fpath := filepath.Join(mi.Path, fname)
		os.Remove(fpath)
		if cnt >= atMost {
			continue
		}
		if b != nil {
			wto = cos.NewBuffer(b)
		} else if sgl != nil {
			wto = sgl // not reopening - see sgl.WriteTo()
		}
		if err := jsp.SaveMeta(fpath, meta, wto); err != nil {
			nlog.Errorf("Failed to persist %q on %q, err: %v", fname, mi, err)
		} else {
			cnt++
		}
	}
	debug.Func(func() {
		expected := min(atMost, availCnt)
		debug.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead",
			fname, expected, cnt)
	})
	return cnt, availCnt
}

func CountPersisted(fname string) (cnt int) {
	avail := GetAvail()
	for mpath := range avail {
		fpath := filepath.Join(mpath, fname)
		if err := cos.Stat(fpath); err == nil {
			cnt++
		}
	}
	return cnt
}
