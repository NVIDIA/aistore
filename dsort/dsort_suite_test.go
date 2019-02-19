// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"testing"

	"github.com/NVIDIA/aistore/transport"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDSort(t *testing.T) {
	RegisterFailHandler(Fail)
	sc := transport.Init()
	sc.Setname("stream-collector")
	go sc.Run()
	RunSpecs(t, "DSort Suite")
}
