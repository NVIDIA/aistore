// Package s3_test provides tests for the Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"net/url"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ListObjects", func() {
	Describe("max-keys", func() {
		DescribeTable("effective page size",
			func(maxKeys string, bckMax int64, expected int, expectErr bool) {
				q := url.Values{}
				if maxKeys != "" {
					q.Set(s3.QparamMaxKeys, maxKeys)
				}
				msg := &apc.LsoMsg{}
				got, err := s3.FillLsoMsg(q, msg, bckMax)
				if expectErr {
					Expect(err).To(HaveOccurred())
					return
				}
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(int64(expected)))
				Expect(msg.PageSize).To(Equal(int64(expected)))
			},
			Entry("default (absent)", "", int64(apc.MaxPageSizeAIS), apc.MaxPageSizeAWS, false),
			Entry("honored as requested", "2", int64(apc.MaxPageSizeAIS), 2, false),
			Entry("clamped to the bucket max", "999999", int64(apc.MaxPageSizeAWS), apc.MaxPageSizeAWS, false),
			Entry("zero => list nothing", "0", int64(apc.MaxPageSizeAIS), 0, false),
			Entry("negative => InvalidArgument", "-1", int64(apc.MaxPageSizeAIS), 0, true),
			Entry("non-numeric => InvalidArgument", "abc", int64(apc.MaxPageSizeAIS), 0, true),
			Entry("oversized explicit value is capped at the s3 max, not the bucket max",
				"5000", int64(apc.MaxPageSizeAIS), apc.MaxPageSizeAWS, false),
		)
	})

	Describe("pagination", func() {
		newQuery := func() url.Values {
			return url.Values{s3.QparamMaxKeys: {"2"}, s3.QparamPrefix: {"dir/"}}
		}
		It("truncates and hands back a usable next-continuation-token", func() {
			msg := &apc.LsoMsg{}
			maxKeys, err := s3.FillLsoMsg(newQuery(), msg, apc.MaxPageSizeAIS)
			Expect(err).NotTo(HaveOccurred())
			Expect(msg.Prefix).To(Equal("dir/"))

			resp := s3.NewListObjectResult("testbkt", maxKeys)
			resp.FromLsoResult(&cmn.LsoRes{
				Entries:           cmn.LsoEntries{{Name: "dir/a"}, {Name: "dir/b"}},
				ContinuationToken: "dir/b",
			}, msg.ContinuationToken)

			Expect(resp.KeyCount).To(Equal(2))
			Expect(resp.MaxKeys).To(Equal(2))
			Expect(resp.IsTruncated).To(BeTrue())
			Expect(resp.NextContinuationToken).To(Equal("dir/b"))

			q := newQuery()
			q.Set(s3.QparamContinuationToken, resp.NextContinuationToken)
			next := &apc.LsoMsg{}
			_, err = s3.FillLsoMsg(q, next, apc.MaxPageSizeAIS)
			Expect(err).NotTo(HaveOccurred())
			Expect(next.ContinuationToken).To(Equal("dir/b"))
		})
		It("reports the last page as not truncated", func() {
			maxKeys, err := s3.FillLsoMsg(newQuery(), &apc.LsoMsg{}, apc.MaxPageSizeAIS)
			Expect(err).NotTo(HaveOccurred())
			resp := s3.NewListObjectResult("testbkt", maxKeys)
			resp.FromLsoResult(&cmn.LsoRes{Entries: cmn.LsoEntries{{Name: "dir/e"}}}, "")
			Expect(resp.IsTruncated).To(BeFalse())
			Expect(resp.NextContinuationToken).To(BeEmpty())
		})
	})
})
