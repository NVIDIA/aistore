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

// create (up to) numMarkers marker files across available mountpaths:
// - try to keep exactly `numMarkers` copies: if there are more, remove extra;
// - if there are fewer, it creates new ones
// return fatalErr when ended up with no markers
// on any other (unlikely) failure:
// - warn and trigger FSHC (with its subsequent cos.IsIOError filter)
func PersistMarker(marker string) (fatalErr, warnErr error) {
	var (
		cnt     int
		relname = filepath.Join(fname.MarkersDir, marker)
		avail   = GetAvail()
	)
	if len(avail) == 0 {
		return cmn.ErrNoMountpaths, nil
	}
	for _, mi := range avail {
		fpath := filepath.Join(mi.Path, relname)

		// existing marker
		err := cos.Stat(fpath)
		if err == nil {
			cnt++
			if cnt > numMarkers {
				if err := cos.RemoveFile(fpath); err != nil {
					warnErr = cos.Ternary(warnErr == nil, err, warnErr)
					nlog.Errorf("Failed to cleanup %q marker: %v", fpath, err)
					mfs.hc.FSHC(err, mi, fpath)
				} else {
					cnt--
				}
			}
			continue
		}

		if !cos.IsNotExist(err) {
			warnErr = cos.Ternary(warnErr == nil, err, warnErr)
			mfs.hc.FSHC(err, mi, fpath)
		}

		// no marker on this path: create if needed
		if cnt < numMarkers {
			file, err := cos.CreateFile(fpath)
			if err != nil {
				warnErr = cos.Ternary(warnErr == nil, err, warnErr)
				nlog.Errorf("Failed to create %q marker: %v", fpath, err)
				mfs.hc.FSHC(err, mi, fpath)
				continue
			}

			if err := file.Close(); err != nil {
				warnErr = cos.Ternary(warnErr == nil, err, warnErr)
				nlog.Errorf("Failed to close %q marker: %v", fpath, err)
			}
			cnt++
		}
	}

	if cnt == 0 {
		fatalErr = fmt.Errorf("failed to persist %q marker (num avail: %d)", marker, len(avail))
	}
	return fatalErr, warnErr
}

func RemoveMarker(marker string, stup cos.StatsUpdater, stopping bool) (ok bool) {
	var (
		avail   = GetAvail()
		relname = filepath.Join(fname.MarkersDir, marker)
	)
	ok = true
	for _, mi := range avail {
		fpath := filepath.Join(mi.Path, relname)
		if err := cos.RemoveFile(fpath); err != nil {
			ok = false
			if !stopping {
				mfs.hc.FSHC(err, mi, fpath)
			}
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
	return ok
}

// PersistOnMpaths persists `what` on mountpaths under "mountpath.Path/path" filename.
// It does it on maximum `atMost` mountPaths. If `atMost == 0`, it does it on every mountpath.
// If `backupPath != ""`, it removes files from `backupPath` and moves files from `path` to `backupPath`.
// Returns how many times it has successfully stored a file.
func PersistOnMpaths(fn, backupName string, meta jsp.Opts, atMost int, b []byte, sgl *memsys.SGL) (cnt, availCnt int) {
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
			bcnt = mi.backupAtmost(fn, backupName, bcnt, atMost)
		}
		fpath := filepath.Join(mi.Path, fn)
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
			nlog.Errorf("Failed to persist %q on %q, err: %v", fn, mi, err)
		} else {
			cnt++
		}
	}
	debug.Func(func() {
		expected := min(atMost, availCnt)
		debug.Assertf(cnt == expected, "expected %q to be persisted on %d mountpaths got %d instead", fn, expected, cnt)
	})
	return cnt, availCnt
}

func CountPersisted(fn string) (cnt int) {
	avail := GetAvail()
	for mpath := range avail {
		fpath := filepath.Join(mpath, fn)
		if err := cos.Stat(fpath); err == nil {
			cnt++
		}
	}
	return cnt
}
