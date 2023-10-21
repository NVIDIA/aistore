// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestExtract(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
