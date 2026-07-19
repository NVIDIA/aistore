// Package hk_test provides unit tests for `hk`
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package hk_test

import (
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/hk"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func startHK(t *testing.T) {
	hk.Init(true)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		hk.HK.Run()
	}()

	hk.WaitStarted()
	RegisterFailHandler(Fail)
	t.Cleanup(func() {
		hk.HK.Stop(nil)
		wg.Wait()
	})
}

func TestHousekeeper(t *testing.T) {
	startHK(t)
	RunSpecs(t, t.Name())
}
