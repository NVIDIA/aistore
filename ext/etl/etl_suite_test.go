// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestETLInternals(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
