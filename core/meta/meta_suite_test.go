// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"os"
	"os/signal"
	"syscall"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
	// restore default SIGINT/SIGTERM (ginkgo never stops its interrupt handler)
	signal.Reset(os.Interrupt, syscall.SIGTERM)
}
