// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package s3 //nolint:testpackage // We use private functions here...

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Presigned", func() {
	Describe("makeS3URL", func() {
		DescribeTable("virtualHostedRequestStyle", func(region, bucketName, objName, query string) {
			got, err := makeS3URL(virtualHostedRequestStyle, region, bucketName, objName, query)
			Expect(err).ToNot(HaveOccurred())
			expected := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s?%s", bucketName, region, objName, query)
			Expect(got).To(Equal(expected))
		},
			Entry("empty query", "us-west-1", "bucket", "object", ""),
			Entry("with query", "us-west-1", "bucket", "object", "&key=value"),
		)

		DescribeTable("pathRequestStyle", func(region, bucketName, objName, query string) {
			got, err := makeS3URL(pathRequestStyle, region, bucketName, objName, query)
			Expect(err).ToNot(HaveOccurred())
			expected := fmt.Sprintf("https://s3.%s.amazonaws.com/%s/%s?%s", region, bucketName, objName, query)
			Expect(got).To(Equal(expected))
		},
			Entry("empty query", "us-west-1", "bucket", "object", ""),
			Entry("with query", "us-west-1", "bucket", "object", "&key=value"),
		)

		It("should return error if request style is not recognized", func() {
			_, err := makeS3URL("something", "us-west-1", "bucket", "object", "&key=value")
			Expect(err).To(HaveOccurred())
		})
	})
})
