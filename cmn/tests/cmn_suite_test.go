// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/tools"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCmn(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
