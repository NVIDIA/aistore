// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
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
