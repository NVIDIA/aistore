// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package hk_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestHousekeeper(t *testing.T) {
	hk.Init(false)
	go hk.HK.Run()
	hk.WaitStarted()
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}
