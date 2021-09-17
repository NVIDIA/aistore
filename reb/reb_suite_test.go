// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
