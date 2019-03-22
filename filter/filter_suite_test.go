// Package filter implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package filter

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFilter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filter Suite")
}
