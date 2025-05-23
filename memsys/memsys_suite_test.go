// Package memsys provides memory management and Slab allocation
// with io.Reader and io.Writer interfaces on top of a scatter-gather lists
// (of reusable buffers)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package memsys_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func init() {
	hk.Init(false)
}

func TestMemsys(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
