// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 *
 */
package core_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func init() {
	hk.TestInit()
}

func TestCore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
