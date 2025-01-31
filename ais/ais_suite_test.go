// Package ais_tests provides tests of AIS cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 *
 */
package ais_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/xact/xreg"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func init() {
	xreg.Init()
	hk.Init(false)
}

func TestAIS(t *testing.T) {
	RegisterFailHandler(Fail)
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	RunSpecs(t, t.Name())
}
