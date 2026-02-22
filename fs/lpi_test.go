// Package fs_test provides tests for fs package
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */

package fs_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/fs/lpi"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	lpiTestPageSize = 10
	lpiTestVerbose  = false
)

func TestLPIPageBySize(t *testing.T) {
	root := t.TempDir()
	expected := genTree(t, root, defaultDirs(), 5, 32)

	sizes := []int{1, 2, 5, len(expected), len(expected) + 10}
	for _, sz := range sizes {
		t.Run("size-"+strconv.Itoa(sz), func(t *testing.T) {
			names, _, _ := collectAll(t, root, "", sz)
			tassert.Errorf(t, len(names) == len(expected),
				"page-size %d: got %d, want %d", sz, len(names), len(expected))
			for _, exp := range expected {
				_, ok := names[exp]
				tassert.Errorf(t, ok, "missing: %s", exp)
			}
		})
	}
}

func TestLPIPageByEOP(t *testing.T) {
	root := t.TempDir()
	expected := genTree(t, root, defaultDirs(), 5, 32)

	size := rand.IntN(lpiTestPageSize<<1) + lpiTestPageSize>>1
	_, eops, _ := collectAll(t, root, "", size)

	// replay using EOP markers
	it, err := lpi.New(root, "", nil)
	tassert.CheckFatal(t, err)

	var (
		previous string
		num      int
		page     = make(lpi.Page, 100)
		seen     = make(map[string]struct{}, len(expected))
	)
	for _, eop := range eops {
		msg := lpi.Msg{EOP: eop}
		err := it.Next(msg, page)
		tassert.CheckFatal(t, err)
		num += len(page)

		for name := range page {
			if _, dup := seen[name]; dup {
				t.Fatalf("EOP replay: duplicate %q", name)
			}
			seen[name] = struct{}{}
		}

		if lpiTestVerbose {
			fmt.Printf("Range (%q - %q]: %d entries\n", previous, eop, len(page))
		}
		if it.Pos() == "" {
			break
		}
		previous = eop
	}
	tassert.Errorf(t, num == len(expected), "EOP replay: got %d, want %d", num, len(expected))
}

func TestLPIPrefix(t *testing.T) {
	root := t.TempDir()
	allExpected := genTree(t, root, defaultDirs(), 5, 32)

	prefixes := []string{"aaa", "aaa/nested", "bbb", "bbb/sub1", "ddd", "zzz"}
	for _, pfx := range prefixes {
		t.Run("prefix-"+strings.ReplaceAll(pfx, "/", "_"), func(t *testing.T) {
			// count expected
			var (
				want     int
				pfxSlash = pfx + "/"
			)
			for _, name := range allExpected {
				if strings.HasPrefix(name, pfxSlash) || strings.HasPrefix(name, pfx) {
					want++
				}
			}

			names, _, _ := collectAll(t, root, pfx, lpiTestPageSize)
			tassert.Errorf(t, len(names) == want,
				"prefix %q: got %d, want %d", pfx, len(names), want)

			// verify all returned names actually match prefix
			for name := range names {
				tassert.Errorf(t, strings.HasPrefix(name, pfxSlash), "prefix %q: unexpected name %q", pfx, name)
			}
		})
	}
}

func TestLPIPrefixNoMatch(t *testing.T) {
	root := t.TempDir()
	genTree(t, root, defaultDirs(), 3, 16)

	names, _, _ := collectAll(t, root, "nonexistent-prefix", lpiTestPageSize)
	tassert.Errorf(t, len(names) == 0, "non-matching prefix: got %d, want 0", len(names))
}

func TestLPIEmptyRoot(t *testing.T) {
	root := t.TempDir()

	it, err := lpi.New(root, "", nil)
	tassert.CheckFatal(t, err)

	page := make(lpi.Page, 10)
	msg := lpi.Msg{Size: 10}
	err = it.Next(msg, page)
	tassert.CheckFatal(t, err)

	tassert.Errorf(t, len(page) == 0, "empty root: got %d entries, want 0", len(page))
	tassert.Errorf(t, it.Pos() == "", "empty root: Pos() should be empty, got %q", it.Pos())
}

func TestLPISingleFile(t *testing.T) {
	root := t.TempDir()
	expected := genTree(t, root, []string{"solo"}, 1, 64)

	names, _, pages := collectAll(t, root, "", 100)
	tassert.Errorf(t, len(names) == 1, "single file: got %d, want 1", len(names))
	tassert.Errorf(t, pages == 1, "single file: got %d pages, want 1", pages)

	_, ok := names[expected[0]]
	tassert.Errorf(t, ok, "single file: missing %s", expected[0])
}

func TestLPIDeterminism(t *testing.T) {
	root := t.TempDir()
	genTree(t, root, defaultDirs(), 8, 16)

	const pageSize = 7

	names1, eops1, _ := collectAll(t, root, "", pageSize)
	names2, eops2, _ := collectAll(t, root, "", pageSize)

	// same set of names
	tassert.Errorf(t, len(names1) == len(names2),
		"determinism: count mismatch %d vs %d", len(names1), len(names2))
	for name := range names1 {
		_, ok := names2[name]
		tassert.Errorf(t, ok, "determinism: %s in run1 but not run2", name)
	}

	// same page boundaries
	tassert.Errorf(t, len(eops1) == len(eops2),
		"determinism: eop count %d vs %d", len(eops1), len(eops2))
	for i := range eops1 {
		tassert.Errorf(t, eops1[i] == eops2[i],
			"determinism: eop[%d] %q vs %q", i, eops1[i], eops2[i])
	}
}

func TestLPICompleteness(t *testing.T) {
	root := t.TempDir()
	genTree(t, root, defaultDirs(), 6, 24)

	// ground truth via filepath.WalkDir
	walkNames := make(map[string]struct{}, 128)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		walkNames[rel] = struct{}{}
		return nil
	})
	tassert.CheckFatal(t, err)

	// LPI
	lpiNames, _, _ := collectAll(t, root, "", lpiTestPageSize)

	tassert.Errorf(t, len(lpiNames) == len(walkNames),
		"completeness: lpi=%d, walk=%d", len(lpiNames), len(walkNames))

	for name := range walkNames {
		_, ok := lpiNames[name]
		tassert.Errorf(t, ok, "completeness: %s found by WalkDir but not LPI", name)
	}
	for name := range lpiNames {
		_, ok := walkNames[name]
		tassert.Errorf(t, ok, "completeness: %s found by LPI but not WalkDir", name)
	}
}

func TestLPINoDuplicates(t *testing.T) {
	root := t.TempDir()
	genTree(t, root, defaultDirs(), 10, 8)

	it, err := lpi.New(root, "", nil)
	tassert.CheckFatal(t, err)

	seen := make(map[string]struct{}, 256)
	page := make(lpi.Page, 16)
	msg := lpi.Msg{Size: 3} // small pages to maximize boundary crossings

	for {
		err := it.Next(msg, page)
		tassert.CheckFatal(t, err)
		for name := range page {
			_, dup := seen[name]
			tassert.Errorf(t, !dup, "duplicate: %s", name)
			seen[name] = struct{}{}
		}
		if it.Pos() == "" {
			break
		}
	}
}

func TestLPIFileSize(t *testing.T) {
	root := t.TempDir()

	const fileSize int64 = 1234
	expected := genTree(t, root, []string{"a", "b"}, 3, fileSize)

	names, _, _ := collectAll(t, root, "", 100)
	tassert.Errorf(t, len(names) == len(expected), "size check: got %d, want %d", len(names), len(expected))

	for name, size := range names {
		tassert.Errorf(t, size == fileSize,
			"size check: %s got %d, want %d", name, size, fileSize)
	}
}

func TestLPIDeepNesting(t *testing.T) {
	root := t.TempDir()
	dirs := []string{
		"a/b/c/d/e/f/g",
		"a/b/c/d/e/f/h",
		"a/b/c/d/e/i",
		"x/y/z",
	}
	expected := genTree(t, root, dirs, 2, 8)

	names, _, _ := collectAll(t, root, "", 3)
	tassert.Errorf(t, len(names) == len(expected),
		"deep nesting: got %d, want %d", len(names), len(expected))
}

func TestLPIPageBySize_Random(t *testing.T) {
	root := t.TempDir()
	expected := genTree(t, root, defaultDirs(), 7, 16)

	size := rand.IntN(lpiTestPageSize<<1) + lpiTestPageSize>>1
	t.Logf("random page size: %d (total files: %d)", size, len(expected))

	names, eops, _ := collectAll(t, root, "", size)
	tassert.Errorf(t, len(names) == len(expected),
		"random page-size %d: got %d, want %d", size, len(names), len(expected))

	// also validate EOP replay
	t.Run("eop-replay", func(t *testing.T) {
		it, err := lpi.New(root, "", nil)
		tassert.CheckFatal(t, err)

		var (
			num  int
			page = make(lpi.Page, 100)
		)
		for _, eop := range eops {
			msg := lpi.Msg{EOP: eop}
			err := it.Next(msg, page)
			tassert.CheckFatal(t, err)
			num += len(page)
			if it.Pos() == "" {
				break
			}
		}
		tassert.Errorf(t, num == len(expected), "EOP replay: got %d, want %d", num, len(expected))
	})
}

//
// static local helpers ---------------------
//

// create a deterministic nested directory tree under root, and
// return sorted list of all relative file paths
func genTree(t *testing.T, root string, dirs []string, filesPerDir int, fileSize int64) []string {
	t.Helper()
	var all []string
	for _, dir := range dirs {
		dpath := filepath.Join(root, dir)
		err := os.MkdirAll(dpath, 0o755)
		tassert.CheckFatal(t, err)
		for i := range filesPerDir {
			name := fmt.Sprintf("file-%04d", i)
			fpath := filepath.Join(dpath, name)
			err := os.WriteFile(fpath, make([]byte, fileSize), 0o644)
			tassert.CheckFatal(t, err)

			rel, _ := filepath.Rel(root, fpath)
			all = append(all, rel)
		}
	}
	sort.Strings(all)
	return all
}

func defaultDirs() []string {
	return []string{
		"aaa",
		"aaa/nested",
		"aaa/nested/deep",
		"bbb",
		"bbb/sub1",
		"bbb/sub2",
		"ccc",
		"ddd/a/b/c",
		"zzz",
	}
}

// iterate the entire root using page-by-size and return
// all names, the page count, and the list of EOPs
func collectAll(t *testing.T, root, prefix string, pageSize int) (names map[string]int64, eops []string, pages int) {
	t.Helper()
	it, err := lpi.New(root, prefix, nil)
	tassert.CheckFatal(t, err)

	names = make(map[string]int64, 128)
	page := make(lpi.Page, pageSize+1)
	msg := lpi.Msg{Size: pageSize}

	for {
		err := it.Next(msg, page)
		tassert.CheckFatal(t, err)
		pages++
		for name, size := range page {
			if _, dup := names[name]; dup {
				t.Fatalf("duplicate: %q", name)
			}
			names[name] = size
		}
		if it.Pos() == "" {
			eops = append(eops, lpi.AllPages)
			break
		}
		eops = append(eops, it.Pos())
	}
	return
}
