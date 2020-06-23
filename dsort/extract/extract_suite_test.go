// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestExtract(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, fmt.Sprintf("%s Extract Suite", cmn.DSortName))
}
