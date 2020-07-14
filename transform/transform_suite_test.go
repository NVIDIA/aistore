// Package transform provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transform

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTransform(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transformer Suite")
}
