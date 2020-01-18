/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 *
 */
package ais_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAIS(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AIS Suite")
}
