// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 *
 */
package core_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func init() {
	hk.Init(false)
	initMemsys()
}

func initMemsys() {
	memsys.PageMM()
	memsys.ByteMM()
}

func TestCore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
