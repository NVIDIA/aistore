// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type dirTreeDesc struct {
	dirs  int  // number of (initially empty) directories at each depth (we recurse into single directory at each depth)
	depth int  // depth of tree/nesting
	empty bool // determines if there is a file somewhere in the directories
}

func TestIsDirEmpty(t *testing.T) {
	tests := []dirTreeDesc{
		{dirs: 0, depth: 1, empty: true},
		{dirs: 0, depth: 1, empty: false},

		{dirs: 50, depth: 1, empty: true},
		{dirs: 50, depth: 1, empty: false},

		{dirs: 50, depth: 8, empty: true},
		{dirs: 2000, depth: 2, empty: true},

		{dirs: 3000, depth: 2, empty: false},
	}

	for _, test := range tests {
		testName := fmt.Sprintf("dirs=%d#depth=%d#empty=%t", test.dirs, test.depth, test.empty)
		t.Run(testName, func(t *testing.T) {
			topDirName := prepareDirTree(t, test)
			defer os.RemoveAll(topDirName)

			_, empty, err := fs.IsDirEmpty(topDirName)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, empty == test.empty,
				"expected directory to be empty=%t, got: empty=%t", test.empty, empty,
			)
		})
	}
}

func TestIsDirEmptyNonExist(t *testing.T) {
	_, _, err := fs.IsDirEmpty("/this/dir/does/not/exist")
	tassert.Fatalf(t, err != nil, "expected error")
}

func BenchmarkIsDirEmpty(b *testing.B) {
	benches := []dirTreeDesc{
		{dirs: 0, depth: 1, empty: true},
		{dirs: 0, depth: 1, empty: false},

		{dirs: 50, depth: 1, empty: true},
		{dirs: 50, depth: 1, empty: false},
		{dirs: 50, depth: 8, empty: true},
		{dirs: 50, depth: 8, empty: false},

		{dirs: 2000, depth: 3, empty: true},
		{dirs: 2000, depth: 3, empty: false},

		{dirs: 3000, depth: 1, empty: true},
		{dirs: 3000, depth: 1, empty: false},
		{dirs: 3000, depth: 3, empty: true},
		{dirs: 3000, depth: 3, empty: false},
	}

	for _, bench := range benches {
		benchName := fmt.Sprintf("dirs=%d#depth=%d#empty=%t", bench.dirs, bench.depth, bench.empty)
		b.Run(benchName, func(b *testing.B) {
			topDirName := prepareDirTree(b, bench)
			defer os.RemoveAll(topDirName)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, empty, err := fs.IsDirEmpty(topDirName)
				tassert.CheckFatal(b, err)
				tassert.Errorf(
					b, empty == bench.empty,
					"expected directory to be empty=%t, got: empty=%t", bench.empty, empty,
				)
			}
		})
	}
}

func prepareDirTree(tb testing.TB, desc dirTreeDesc) string {
	topDirName, err := ioutil.TempDir("", "")
	tassert.CheckFatal(tb, err)

	nestedDirectoryName := topDirName
	for depth := 1; depth <= desc.depth; depth++ {
		names := make([]string, 0, desc.dirs)
		for i := 1; i <= desc.dirs; i++ {
			name, err := ioutil.TempDir(nestedDirectoryName, "")
			tassert.CheckFatal(tb, err)
			names = append(names, name)
		}
		sort.Strings(names)
		if desc.dirs > 0 {
			// We only recurse into last directory.
			nestedDirectoryName = names[len(names)-1]
		}
	}

	if !desc.empty {
		f, err := ioutil.TempFile(nestedDirectoryName, "")
		tassert.CheckFatal(tb, err)
		f.Close()
	}
	return topDirName
}
