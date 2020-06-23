// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHousekeeper(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Housekeeper Suite")
}
