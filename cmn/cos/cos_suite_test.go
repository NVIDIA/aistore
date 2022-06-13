// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"testing"

	"github.com/NVIDIA/aistore/devtools/tutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCos(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
