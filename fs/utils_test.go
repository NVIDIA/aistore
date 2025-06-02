// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestIsDirEmpty(t *testing.T) {
	tests := []tools.DirTreeDesc{
		{Dirs: 0, Depth: 1, Empty: true},
		{Dirs: 0, Depth: 1, Empty: false},

		{Dirs: 50, Depth: 1, Empty: true},
		{Dirs: 50, Depth: 1, Empty: false},

		{Dirs: 50, Depth: 8, Empty: true},
		{Dirs: 2000, Depth: 2, Empty: true},

		{Dirs: 3000, Depth: 2, Empty: false},
	}

	for _, test := range tests {
		testName := fmt.Sprintf("dirs=%d#depth=%d#empty=%t", test.Dirs, test.Depth, test.Empty)
		t.Run(testName, func(t *testing.T) {
			topDirName, _ := tools.PrepareDirTree(t, test)
			defer os.RemoveAll(topDirName)

			_, empty, err := fs.IsDirEmpty(topDirName)
			tassert.CheckFatal(t, err)
			tassert.Errorf(
				t, empty == test.Empty,
				"expected directory to be empty=%t, got: empty=%t", test.Empty, empty,
			)
		})
	}
}

func TestIsDirEmptyNonExist(t *testing.T) {
	_, _, err := fs.IsDirEmpty("/this/dir/does/not/exist")
	tassert.Fatalf(t, err != nil, "expected error")
}

func BenchmarkIsDirEmpty(b *testing.B) {
	benches := []tools.DirTreeDesc{
		{Dirs: 0, Depth: 1, Empty: true},
		{Dirs: 0, Depth: 1, Empty: false},

		{Dirs: 50, Depth: 1, Empty: true},
		{Dirs: 50, Depth: 1, Empty: false},
		{Dirs: 50, Depth: 8, Empty: true},
		{Dirs: 50, Depth: 8, Empty: false},

		{Dirs: 2000, Depth: 3, Empty: true},
		{Dirs: 2000, Depth: 3, Empty: false},

		{Dirs: 3000, Depth: 1, Empty: true},
		{Dirs: 3000, Depth: 1, Empty: false},
		{Dirs: 3000, Depth: 3, Empty: true},
		{Dirs: 3000, Depth: 3, Empty: false},
	}

	for _, bench := range benches {
		benchName := fmt.Sprintf("dirs=%d#depth=%d#empty=%t", bench.Dirs, bench.Depth, bench.Empty)
		b.Run(benchName, func(b *testing.B) {
			topDirName, _ := tools.PrepareDirTree(b, bench)
			defer os.RemoveAll(topDirName)

			for b.Loop() {
				_, empty, err := fs.IsDirEmpty(topDirName)
				tassert.CheckFatal(b, err)
				tassert.Errorf(
					b, empty == bench.Empty,
					"expected directory to be empty=%t, got: empty=%t", bench.Empty, empty,
				)
			}
		})
	}
}
