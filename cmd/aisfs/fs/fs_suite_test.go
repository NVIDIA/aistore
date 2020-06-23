// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "fuse/fs suite")
}
