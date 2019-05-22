// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDSort(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, fmt.Sprintf("%s Suite", cmn.DSortName))
}
