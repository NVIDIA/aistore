// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCmn(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cmn Suite")
}
