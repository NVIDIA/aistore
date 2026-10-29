// Package s3_test provides tests for the Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"github.com/NVIDIA/aistore/ais/s3"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseCopyObjSrc", func() {
	DescribeTable("valid",
		func(src, bucket, object, versionID string) {
			b, o, v, err := s3.ParseCopyObjSrc(src)
			Expect(err).NotTo(HaveOccurred())
			Expect(b).To(Equal(bucket))
			Expect(o).To(Equal(object))
			Expect(v).To(Equal(versionID))
		},
		Entry("leading slash", "/bkt/obj", "bkt", "obj", ""),
		Entry("no leading slash", "bkt/dir/obj", "bkt", "dir/obj", ""),
		Entry("trailing slash", "bkt/dir/", "bkt", "dir", ""),
		Entry("literal plus", "bkt/a+b.txt", "bkt", "a+b.txt", ""),
		Entry("escaped plus", "bkt/a%2Bb.txt", "bkt", "a+b.txt", ""),
		Entry("escaped space", "bkt/a%20b", "bkt", "a b", ""),
		Entry("escaped question mark", "bkt/k%3Fq", "bkt", "k?q", ""),
		Entry("escaped slash", "bkt%2Fobj", "bkt", "obj", ""),
		Entry("malformed escape: raw", "bkt/100%", "bkt", "100%", ""),
		Entry("version ID", "/bkt/key?versionId=v1", "bkt", "key", "v1"),
		Entry("version ID, escaped question mark", "/bkt/k%3Fq?versionId=v1", "bkt", "k?q", "v1"),
		Entry("version ID, escaped value", "/bkt/key?versionId=a%2Bb", "bkt", "key", "a+b"),
	)

	DescribeTable("invalid",
		func(src string) {
			b, o, v, err := s3.ParseCopyObjSrc(src)
			Expect(err).To(HaveOccurred())
			Expect(b).To(BeEmpty())
			Expect(o).To(BeEmpty())
			Expect(v).To(BeEmpty())
		},
		Entry("empty", ""),
		Entry("bucket only", "bkt"),
		Entry("bucket only, slashes", "/bkt/"),
		Entry("consecutive slashes", "bkt//obj"),
		Entry("consecutive slashes, leading", "//bkt/obj"),
		Entry("consecutive slashes, trailing", "/bkt/obj//"),
		Entry("escaped consecutive slashes", "bkt%2F%2Fobj"),
		Entry("empty version ID", "/bkt/key?versionId="),
		Entry("unexpected query param", "/bkt/key?foo=bar"),
		Entry("version ID and unexpected query param", "/bkt/key?versionId=v1&foo=bar"),
		Entry("malformed query", "/bkt/key?versionId=%zz"),
	)
})
