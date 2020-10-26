// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestPersist(t *testing.T) {
	const mpathsCnt = 10
	mpaths := tutils.PrepareMountPaths(t, mpathsCnt)
	defer tutils.RemoveMountPaths(t, mpaths)

	t.Run("PersistOnMpaths", func(t *testing.T) { testPersistOnMpaths(t, mpaths) })
	t.Run("FindPersisted", func(t *testing.T) { testFindPersisted(t, mpaths) })
	t.Run("RemovePersisted", func(t *testing.T) { testRemovePersisted(t, mpaths) })
	t.Run("MovePersisted", func(t *testing.T) { testMovePersisted(t, mpaths) })
}

func testPersistOnMpaths(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cmn.RandString(3)
		value     = "test-value-string" + cmn.RandString(3)
		mpathsCnt = len(mpaths)
	)

	for i := 1; i <= mpathsCnt; i++ {
		createdCnt, _ := fs.PersistOnMpaths(name, value, i)
		tassert.Errorf(t, createdCnt == i, "expected %d of %q to be created, got %d", i, name, createdCnt)

		foundCnt := 0
		for _, mpath := range mpaths {
			var (
				path       = filepath.Join(mpath.Path, name)
				foundValue = ""
			)
			err := jsp.Load(path, &foundValue, jsp.CksumSign())
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

func testFindPersisted(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cmn.RandString(3)
		value     = "test-value-string" + cmn.RandString(3)
		mpathsCnt = len(mpaths)
	)

	createdCnt, _ := fs.PersistOnMpaths(name, value, mpathsCnt/2)
	foundMpaths := fs.FindPersisted(name)

	tassert.Errorf(t, mpathsCnt/2 == createdCnt, "expected %d of %q to be created, got %d", mpathsCnt/2, name, createdCnt)
	tassert.Errorf(t, mpathsCnt/2 == createdCnt, "expected %d of %q to be found, got %d", mpathsCnt/2, name, len(foundMpaths))
}

func testRemovePersisted(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cmn.RandString(3)
		value     = "test-value-string" + cmn.RandString(3)
		mpathsCnt = len(mpaths)
	)

	_, _ = fs.PersistOnMpaths(name, value, mpathsCnt/2)
	fs.RemovePersisted(name)
	found := fs.FindPersisted(name)

	tassert.Errorf(t, len(found) == 0, "expected all %q to be removed, found %d", name, len(found))
}

func testMovePersisted(t *testing.T, mpaths fs.MPI) {
	var (
		name      = ".ais.testmarker" + cmn.RandString(3)
		newName   = name + ".old"
		value     = "test-value-string" + cmn.RandString(3)
		mpathsCnt = len(mpaths)
	)

	_, _ = fs.PersistOnMpaths(name, value, mpathsCnt/2)
	moved := fs.MovePersisted(name, newName)

	tassert.Errorf(t, moved == mpathsCnt/2, "expected %d of %q to be moved, got %d", mpathsCnt/2, name, moved)
	foundOld := fs.FindPersisted(name)
	tassert.Errorf(t, len(foundOld) == 0, "expected 0 of %q to be found, got %d", name, len(foundOld))
	foundNew := fs.FindPersisted(newName)
	tassert.Errorf(t, len(foundNew) == mpathsCnt/2, "expected %d of %q to be found, got %d", mpathsCnt/2, name, len(foundNew))
}
