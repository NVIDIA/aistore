// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"testing"

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

	fatalErr, writeErr := fs.PersistMarker(fname.RebalanceMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: true},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)

	fatalErr, writeErr = fs.PersistMarker(fname.ResilverMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: true},
		markerEntry{marker: fname.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fname.RebalanceMarker, mockst)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fname.ResilverMarker, mockst)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)
}

func TestMarkersClear(t *testing.T) {
	const mpathsCnt = 5
	mpaths := tools.PrepareMountPaths(t, mpathsCnt)
	defer tools.RemoveMpaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)

	fatalErr, writeErr := fs.PersistMarker(fname.RebalanceMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	fatalErr, writeErr = fs.PersistMarker(fname.ResilverMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: true},
		markerEntry{marker: fname.ResilverMarker, exists: true},
	)

	for _, mpath := range mpaths {
		mpath.ClearMDs(true)
	}

	checkMarkersExist(t,
		markerEntry{marker: fname.RebalanceMarker, exists: false},
		markerEntry{marker: fname.ResilverMarker, exists: false},
	)
}
