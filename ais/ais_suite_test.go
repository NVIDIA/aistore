// Package ais_tests provides tests of AIS cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package ais_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xreg"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	xreg.Init()
	hk.TestInit()
}

func TestAIS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AIS Suite")
}
