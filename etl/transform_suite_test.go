// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTransform(t *testing.T) {
	RegisterFailHandler(Fail)
	cluster.InitTarget()
	RunSpecs(t, "Transformer Suite")
}
