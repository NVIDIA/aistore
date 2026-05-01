// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type markerEntry struct {
	marker string
	exists bool
}

func checkMarkersExist(t *testing.T, xs ...markerEntry) {
	for _, xctn := range xs {
		exists := fs.MarkerExists(xctn.marker)
		tassert.Fatalf(t, exists == xctn.exists, "%q marker (%t vs %t)", xctn.marker, exists, xctn.exists)
	}
}

func TestMarkers(t *testing.T) {
	const mpathsCnt = 5
	mpaths := tools.PrepareMountPaths(t, mpathsCnt)
	mockst := mock.NewStatsTracker()
	defer tools.RemoveMpaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)

	fatalErr, writeErr := fs.PersistMarker(fname.RebalanceMarker, false /*quiet*/)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: true},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)

	fatalErr, writeErr = fs.PersistMarker(fname.ResilverMarker, false /*quiet*/)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: true},
		markerEntry{marker: fname.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fname.RebalanceMarker, mockst, false)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fname.ResilverMarker, mockst, false)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)
}

// flagTrackingStats implements cos.StatsUpdater and records flag operations
// so tests can verify that RemoveMarker sets/clears the correct alerts.
type flagTrackingStats struct {
	mu           sync.Mutex
	setClrCalls  []flagSetClrCall
	setFlagCalls []flagSetCall
	clrFlagCalls []flagClrCall
}

type (
	flagSetClrCall struct {
		name string
		set  cos.NodeStateFlags
		clr  cos.NodeStateFlags
	}
	flagSetCall struct {
		name  string
		flags cos.NodeStateFlags
	}
	flagClrCall struct {
		name  string
		flags cos.NodeStateFlags
	}
)

var _ cos.StatsUpdater = (*flagTrackingStats)(nil)

func (*flagTrackingStats) Get(string) int64                  { return 0 }
func (*flagTrackingStats) Inc(string)                        {}
func (*flagTrackingStats) IncWith(string, map[string]string) {}
func (*flagTrackingStats) Add(string, int64)                 {}
func (*flagTrackingStats) Observe(string, float64)           {}
func (*flagTrackingStats) AddWith(...cos.NamedVal64)         {}

func (s *flagTrackingStats) SetFlag(name string, flags cos.NodeStateFlags) {
	s.mu.Lock()
	s.setFlagCalls = append(s.setFlagCalls, flagSetCall{name, flags})
	s.mu.Unlock()
}

func (s *flagTrackingStats) ClrFlag(name string, flags cos.NodeStateFlags) {
	s.mu.Lock()
	s.clrFlagCalls = append(s.clrFlagCalls, flagClrCall{name, flags})
	s.mu.Unlock()
}

func (s *flagTrackingStats) SetClrFlag(name string, set, clr cos.NodeStateFlags) {
	s.mu.Lock()
	s.setClrCalls = append(s.setClrCalls, flagSetClrCall{name, set, clr})
	s.mu.Unlock()
}

// makeMarkerUnremovable creates a non-empty directory at each marker path so
// that os.Remove (and thus cos.RemoveFile) fails with ENOTEMPTY — an error
// that cos.RemoveFile does NOT suppress (it only suppresses ENOENT).
func makeMarkerUnremovable(t *testing.T, mpaths fs.MPI, marker string) {
	for _, mi := range mpaths {
		markerDir := filepath.Join(mi.Path, fname.MarkersDir, marker)
		tassert.CheckFatal(t, os.MkdirAll(markerDir, 0o755))
		f, err := os.Create(filepath.Join(markerDir, "block"))
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, f.Close())
	}
}

// cleanupMarkerDirs removes any marker directories (created by makeMarkerUnremovable)
// before RemoveMpaths, so that _moveMarkers doesn't try to copy a directory and
// hit the nil FSHC during test teardown.
func cleanupMarkerDirs(t *testing.T, mpaths fs.MPI) {
	for _, mi := range mpaths {
		for _, marker := range []string{fname.RebalanceMarker, fname.ResilverMarker} {
			tassert.CheckError(t, os.RemoveAll(filepath.Join(mi.Path, fname.MarkersDir, marker)))
		}
	}
}

func TestRemoveMarkerFailure(t *testing.T) {
	const mpathsCnt = 3
	mpaths := tools.PrepareMountPaths(t, mpathsCnt)
	defer func() {
		cleanupMarkerDirs(t, mpaths)
		tools.RemoveMpaths(t, mpaths)
	}()

	t.Run("rebalance", func(t *testing.T) {
		tracker := &flagTrackingStats{}
		makeMarkerUnremovable(t, mpaths, fname.RebalanceMarker)

		ok := fs.RemoveMarker(fname.RebalanceMarker, tracker, true /*stopping*/)
		tassert.Fatalf(t, !ok, "expected RemoveMarker to return false on failure")

		tassert.Fatalf(t, len(tracker.setClrCalls) == 1,
			"expected 1 SetClrFlag call, got %d", len(tracker.setClrCalls))
		call := tracker.setClrCalls[0]
		tassert.Fatalf(t, call.name == cos.NodeAlerts,
			"expected name %q, got %q", cos.NodeAlerts, call.name)
		tassert.Fatalf(t, call.set == cos.RebalanceInterrupted,
			"expected set flag RebalanceInterrupted, got %v", call.set)
		tassert.Fatalf(t, call.clr == cos.Rebalancing,
			"expected clr flag Rebalancing, got %v", call.clr)
		tassert.Fatalf(t, len(tracker.setFlagCalls) == 0,
			"expected no SetFlag calls, got %d", len(tracker.setFlagCalls))
		tassert.Fatalf(t, len(tracker.clrFlagCalls) == 0,
			"expected no ClrFlag calls, got %d", len(tracker.clrFlagCalls))
	})

	t.Run("resilver", func(t *testing.T) {
		tracker := &flagTrackingStats{}
		makeMarkerUnremovable(t, mpaths, fname.ResilverMarker)

		ok := fs.RemoveMarker(fname.ResilverMarker, tracker, true /*stopping*/)
		tassert.Fatalf(t, !ok, "expected RemoveMarker to return false on failure")

		tassert.Fatalf(t, len(tracker.setClrCalls) == 1,
			"expected 1 SetClrFlag call, got %d", len(tracker.setClrCalls))
		call := tracker.setClrCalls[0]
		tassert.Fatalf(t, call.name == cos.NodeAlerts,
			"expected name %q, got %q", cos.NodeAlerts, call.name)
		tassert.Fatalf(t, call.set == cos.ResilverInterrupted,
			"expected set flag ResilverInterrupted, got %v", call.set)
		tassert.Fatalf(t, call.clr == cos.Resilvering,
			"expected clr flag Resilvering, got %v", call.clr)
		tassert.Fatalf(t, len(tracker.setFlagCalls) == 0,
			"expected no SetFlag calls, got %d", len(tracker.setFlagCalls))
		tassert.Fatalf(t, len(tracker.clrFlagCalls) == 0,
			"expected no ClrFlag calls, got %d", len(tracker.clrFlagCalls))
	})
}
