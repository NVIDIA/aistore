/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 *
 */
package mirror

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMirror(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mirror Suite")
}
