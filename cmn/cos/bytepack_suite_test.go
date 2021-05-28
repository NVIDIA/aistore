// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBytePacker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "BytePacker suite")
}
