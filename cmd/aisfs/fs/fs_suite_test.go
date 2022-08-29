// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAISFS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
