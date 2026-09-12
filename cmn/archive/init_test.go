// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"github.com/NVIDIA/aistore/memsys"
)

func init() {
	memsys.PageMM()
	memsys.ByteMM()
}
