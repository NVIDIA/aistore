// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3 //nolint:testpackage // We use private functions here...

import (
	"fmt"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Presigned", func() {
	It("rejects SigV4 without a region", func() {
		q := url.Values{HeaderCredentials: {"key/20260803//s3/aws4_request"}}
		_, err := NewPresignedReq(&http.Request{Header: http.Header{}}, nil, nil, q, "us-east-1")
		Expect(err).To(HaveOccurred())
	})

	DescribeTable("NewPresignedReq", func(signed, configured string, shouldFail bool) {
		q := url.Values{}
		if signed != "" {
			q.Set(HeaderCredentials, "key/20260803/"+signed+"/s3/aws4_request")
		}
		_, err := NewPresignedReq(&http.Request{Header: http.Header{}}, nil, nil, q, configured)
		Expect(err != nil).To(Equal(shouldFail))
	},
		Entry("matching", "us-east-1", "us-east-1", false),
		Entry("host hijack", "us-east-1@attacker.com?", "us-east-1", true),
		Entry("unsigned fallback", "", "us-east-1", false),
	)

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

		It("escapes object names", func() {
			const objName = "dir/object?#100% done"
			got, err := makeS3URL(virtualHostedRequestStyle, "us-west-1", "bucket", objName, "key=value")
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal("https://bucket.s3.us-west-1.amazonaws.com/dir/object%3F%23100%25%20done?key=value"))

			got, err = makeS3URL(pathRequestStyle, "us-west-1", "bucket", objName, "key=value")
			Expect(err).ToNot(HaveOccurred())
			Expect(got).To(Equal("https://s3.us-west-1.amazonaws.com/bucket/dir/object%3F%23100%25%20done?key=value"))
		})
	})
})
