// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dsort_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	hk.TestInit()
}

func TestDSort(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
