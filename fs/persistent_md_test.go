// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
)

func TestPersist(t *testing.T) {
	const mpathsCnt = 10
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMountPaths(t, mpaths)

	t.Run("PersistOnMpaths", func(t *testing.T) { testPersistOnMpaths(t, mpaths) })
	t.Run("PersistOnMpathsWithBackup", func(t *testing.T) { testPersistOnMpathsWithBackup(t, mpaths) })
	t.Run("FindPersisted", func(t *testing.T) { testFindPersisted(t, mpaths) })
	t.Run("RemovePersisted", func(t *testing.T) { testRemovePersisted(t, mpaths) })
}

func testPersistOnMpaths(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cos.RandString(3)
		value     = "test-value-string" + cos.RandString(3)
		mpathsCnt = len(mpaths)
	)

	for i := 1; i <= mpathsCnt; i++ {
		createdCnt, _ := fs.PersistOnMpaths(name, "", value, i)
		tassert.Errorf(t, createdCnt == i, "expected %d of %q to be created, got %d", i, name, createdCnt)

		foundCnt := 0
		for _, mpath := range mpaths {
			var (
				path       = filepath.Join(mpath.Path, name)
				foundValue = ""
			)
			_, err := jsp.Load(path, &foundValue, jsp.CksumSign(0))
			if err != nil && !os.IsNotExist(err) {
				t.Fatalf("Failed loading %q, err %v", path, err)
			}
			if err == nil {
				foundCnt++
				tassert.Errorf(t, foundValue == value, "expected %q to be found, got %q", value, foundValue)
				os.Remove(path)
			}
		}

		tassert.Errorf(t, foundCnt == i, "expected %d of %q to be found on mountpaths, got %d", i, name, foundCnt)
	}
}

func testPersistOnMpathsWithBackup(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cos.RandString(3)
		oldName   = name + ".old"
		value     = "test-value-string" + cos.RandString(3)
		newValue  = value + "-new"
		mpathsCnt = len(mpaths)
	)

	for i := 1; i <= mpathsCnt; i++ {
		createdCnt, _ := fs.PersistOnMpaths(name, "", value, i)
		tassert.Errorf(t, createdCnt == i, "expected %d of %q to be created, got %d", i, name, createdCnt)

		createdCnt, _ = fs.PersistOnMpaths(name, oldName, newValue, i)
		tassert.Errorf(t, createdCnt == i, "expected %d of new %q to be created, got %d", i, name, createdCnt)

		foundCnt := 0
		oldFoundCnt := 0
		for _, mpath := range mpaths {
			foundValue := ""

			// Load and check new value.
			path := filepath.Join(mpath.Path, name)
			_, err := jsp.Load(path, &foundValue, jsp.CksumSign(0))
			tassert.Fatalf(t, err == nil || os.IsNotExist(err), "Failed loading %q, err %v", path, err)
			if err == nil {
				foundCnt++
				tassert.Errorf(t, foundValue == newValue, "expected %q to be found, got %q",
					newValue, foundValue)
				os.Remove(path)
			}

			// Load and check old value
			path = filepath.Join(mpath.Path, oldName)
			if _, err = jsp.Load(path, &foundValue, jsp.CksumSign(0)); err == nil {
				oldFoundCnt++
				tassert.Errorf(t, foundValue == value, "expected %q to be found, got %q", value, foundValue)
				os.Remove(path)
			}
			tassert.Fatalf(t, err == nil || os.IsNotExist(err), "Failed loading %q, err %v", path, err)
		}

		tassert.Errorf(t, foundCnt == i, "expected %d of %q to be found on mountpaths, got %d", i, name, foundCnt)
		tassert.Errorf(t, oldFoundCnt == i, "expected %d of %q to be found on mountpaths, got %d", i, oldName, oldFoundCnt)
	}
}

func testFindPersisted(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cos.RandString(3)
		value     = "test-value-string" + cos.RandString(3)
		mpathsCnt = len(mpaths)
	)

	createdCnt, _ := fs.PersistOnMpaths(name, "", value, mpathsCnt/2)
	foundMpaths := fs.FindPersisted(name)

	tassert.Errorf(t, mpathsCnt/2 == createdCnt, "expected %d of %q to be created, got %d", mpathsCnt/2, name, createdCnt)
	tassert.Errorf(t, mpathsCnt/2 == createdCnt, "expected %d of %q to be found, got %d", mpathsCnt/2, name, len(foundMpaths))
}

func testRemovePersisted(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cos.RandString(3)
		value     = "test-value-string" + cos.RandString(3)
		mpathsCnt = len(mpaths)
	)

	_, _ = fs.PersistOnMpaths(name, "", value, mpathsCnt/2)
	for mpath := range mpaths {
		os.RemoveAll(filepath.Join(mpath, name))
	}
	found := fs.FindPersisted(name)

	tassert.Errorf(t, len(found) == 0, "expected all %q to be removed, found %d", name, len(found))
}

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
	defer tutils.RemoveMountPaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: false},
		markerEntry{marker: fs.ResilverMarker, exists: false},
	)

	err := fs.PersistMarker(fs.RebalanceMarker)
	tassert.CheckFatal(t, err)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: true},
		markerEntry{marker: fs.ResilverMarker, exists: false},
	)

	err = fs.PersistMarker(fs.ResilverMarker)
	tassert.CheckFatal(t, err)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: true},
		markerEntry{marker: fs.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fs.RebalanceMarker)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: false},
		markerEntry{marker: fs.ResilverMarker, exists: true},
	)

	fs.RemoveMarker(fs.ResilverMarker)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: false},
		markerEntry{marker: fs.ResilverMarker, exists: false},
	)
}

func TestMarkersClear(t *testing.T) {
	const mpathsCnt = 5
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMountPaths(t, mpaths)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: false},
		markerEntry{marker: fs.ResilverMarker, exists: false},
	)

	err := fs.PersistMarker(fs.RebalanceMarker)
	tassert.CheckFatal(t, err)
	err = fs.PersistMarker(fs.ResilverMarker)
	tassert.CheckFatal(t, err)

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: true},
		markerEntry{marker: fs.ResilverMarker, exists: true},
	)

	for _, mpath := range mpaths {
		mpath.ClearMDs()
	}

	checkMarkersExist(t,
		markerEntry{marker: fs.RebalanceMarker, exists: false},
		markerEntry{marker: fs.ResilverMarker, exists: false},
	)
}
