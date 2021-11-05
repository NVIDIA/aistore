// Package cluster_test provides tests for cluster package
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package cluster_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	hk.TestInit()
}

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cluster Suite")
}
