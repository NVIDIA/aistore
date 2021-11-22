// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
)

type markerEntry struct {
	marker string
	exists bool
}

func checkMarkersExist(t *testing.T, xs ...markerEntry) {
	for _, x := range xs {
		exists := fs.MarkerExists(x.marker)
		tassert.Fatalf(t, exists == x.exists, "%q marker (%t vs %t)", x.marker, exists, x.exists)
	}
}

func TestMarkers(t *testing.T) {
	const mpathsCnt = 5
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMpaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: false},
		markerEntry{marker: cmn.ResilverMarker, exists: false},
	)

	fatalErr, writeErr := fs.PersistMarker(cmn.RebalanceMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: true},
		markerEntry{marker: cmn.ResilverMarker, exists: false},
	)

	fatalErr, writeErr = fs.PersistMarker(cmn.ResilverMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: true},
		markerEntry{marker: cmn.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(cmn.RebalanceMarker)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: false},
		markerEntry{marker: cmn.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(cmn.ResilverMarker)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: false},
		markerEntry{marker: cmn.ResilverMarker, exists: false},
	)
}

func TestMarkersClear(t *testing.T) {
	const mpathsCnt = 5
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMpaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: false},
		markerEntry{marker: cmn.ResilverMarker, exists: false},
	)

	fatalErr, writeErr := fs.PersistMarker(cmn.RebalanceMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	fatalErr, writeErr = fs.PersistMarker(cmn.ResilverMarker)
	tassert.CheckFatal(t, fatalErr)
	tassert.CheckFatal(t, writeErr)

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: true},
		markerEntry{marker: cmn.ResilverMarker, exists: true},
	)

	for _, mpath := range mpaths {
		mpath.ClearMDs()
	}

	checkMarkersExist(t,
		markerEntry{marker: cmn.RebalanceMarker, exists: false},
		markerEntry{marker: cmn.ResilverMarker, exists: false},
	)
}
