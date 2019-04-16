// Package atomic provides simple wrappers around numerics to enforce atomic
// access.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package atomic

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAtomic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Atomic Suite")
}
