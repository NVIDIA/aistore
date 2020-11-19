// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMirror(t *testing.T) {
	RegisterFailHandler(Fail)
	cluster.InitLomLocker()
	RunSpecs(t, "Mirror Suite")
}
