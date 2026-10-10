// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/NVIDIA/aistore/tools"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCmn(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
	// restore default SIGINT/SIGTERM (ginkgo never stops its interrupt handler)
	signal.Reset(os.Interrupt, syscall.SIGTERM)
}
