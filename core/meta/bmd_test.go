// Package meta_test: unit tests for the package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package meta_test

import (
	"github.com/NVIDIA/aistore/cmn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BMD", func() {
	Describe("validateBucketName", func() {
		DescribeTable("should accept bucket name",
			func(bckName string) {
				bck := cmn.Bck{Name: bckName}
				Expect(bck.ValidateName()).NotTo(HaveOccurred())
			},
			Entry(
				"regular name bucket",
				"bucket-1024",
			),
			Entry(
				"with dots",
				".bucket.name",
			),
			Entry(
				"with '_' and '-'",
				"bucket_name-1024",
			),
		)

		DescribeTable("should reject bucket name",
			func(bckName string) {
				bck := cmn.Bck{Name: bckName}
				Expect(bck.ValidateName()).To(HaveOccurred())
			},
			Entry(
				"empty bucket",
				"",
			),
			Entry(
				"contains '$'",
				"jhljs$lsf",
			),
			Entry(
				"contains '/'",
				"bucket/name",
			),
			Entry(
				"contains '*'",
				"bucket$name",
			),
			Entry(
				"contains space",
				"space bucket",
			),
			Entry(
				"contains only dots",
				"...........",
			),
			Entry(
				"only space",
				" ",
			),
		)
	})
})
