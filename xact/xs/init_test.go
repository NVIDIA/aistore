// Package xs_test tests xaction implementations without a running cluster.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"github.com/NVIDIA/aistore/memsys"
)

func init() {
	memsys.PageMM()
	memsys.ByteMM()
}
