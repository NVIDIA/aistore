// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRebPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rebalance Suite")
}
