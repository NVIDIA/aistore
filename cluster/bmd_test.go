// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("BMD", func() {
	Describe("validateBucketName", func() {
		DescribeTable("should accept bucket name",
			func(bucket string) {
				valid := validateBucketName(bucket)
				Expect(valid).To(BeTrue())
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
			func(bucket string) {
				valid := validateBucketName(bucket)
				Expect(valid).To(BeFalse())
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
