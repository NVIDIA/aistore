// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package reb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestRebPkg(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
