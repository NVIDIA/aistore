/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDSort(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DSort Extract Suite")
}
