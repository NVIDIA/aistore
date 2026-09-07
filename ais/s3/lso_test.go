// Package s3_test provides tests for the Amazon S3 compatibility layer
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package s3_test

import (
	"encoding/base64"
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

var _ = Describe("Delimiter", func() {
	DescribeTable("rejects anything but slash",
		func(delimiter string) {
			q := url.Values{s3.QparamDelimiter: []string{delimiter}}
			_, err := s3.FillLsoMsg(q, &apc.LsoMsg{}, apc.MaxPageSizeAIS)
			Expect(err).To(HaveOccurred())
		},
		Entry("comma", ","),
		Entry("dash", "-"),
		Entry("multi-char", "::"),
	)
	It("accepts slash and sets no-recursion", func() {
		msg := &apc.LsoMsg{}
		q := url.Values{s3.QparamDelimiter: []string{"/"}}
		_, err := s3.FillLsoMsg(q, msg, apc.MaxPageSizeAIS)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.IsFlagSet(apc.LsNoRecursion)).To(BeTrue())
	})
	It("leaves no-recursion unset when absent", func() {
		msg := &apc.LsoMsg{}
		_, err := s3.FillLsoMsg(url.Values{}, msg, apc.MaxPageSizeAIS)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.IsFlagSet(apc.LsNoRecursion)).To(BeFalse())
	})
})

var _ = Describe("Continuation token", func() {
	Describe("compound (v5.1)", func() {
		DescribeTable("round-trip",
			func(uuid, tok string) {
				gotUUID, gotTok := s3.DecodeToken(s3.EncodeToken(uuid, tok))
				Expect(gotUUID).To(Equal(uuid))
				Expect(gotTok).To(Equal(tok))
			},
			Entry("object name", "Xk3nZq4i1", "dir/b"),
			Entry("name with dots", "Xk3nZq4i1", "images/2026/report.pdf"),
			Entry("single byte", "Xk3nZq4i1", "a"),
			// the inner token is opaque on the R-flow: anything goes, NUL included
			Entry("opaque remote token", "A9vehq4i1cV2", "1/2.3\x00weird&=+/tok"),
		)
		It("encodes end-of-listing as empty", func() {
			Expect(s3.EncodeToken("Xk3nZq4i1", "")).To(BeEmpty())
		})
		It("is url-safe and unpadded", func() {
			Expect(s3.EncodeToken("Xk3nZq4i1", "a/b+c=d?e&f")).To(MatchRegexp(`^[A-Za-z0-9_-]+$`))
		})
	})

	Describe("legacy (v5.0)", func() {
		DescribeTable("passes through with no uuid",
			func(tok string) {
				uuid, got := s3.DecodeToken(tok)
				Expect(uuid).To(BeEmpty())
				Expect(got).To(Equal(tok))
			},
			Entry("plain object name", "images/2026/report.pdf"),
			Entry("name that is valid base64", "dGVzdA"),
			Entry("short name", "abcd"),
			// announces itself as compound, then fails to parse => still legacy
			Entry("version byte, no separator", base64.RawURLEncoding.EncodeToString([]byte{0x01, 'X', 'k'})),
			Entry("version byte, empty uuid", base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 'x'})),
			Entry("short uuid", base64.RawURLEncoding.EncodeToString([]byte("\x01Xk3nZq\x00dir/b"))),
			Entry("invalid uuid character", base64.RawURLEncoding.EncodeToString([]byte("\x01Xk3nZq4.1\x00dir/b"))),
			Entry("unknown version", base64.RawURLEncoding.EncodeToString([]byte("\x02Xk3nZq4i1\x00dir/b"))),
		)
		It("decodes the empty token to nothing", func() {
			uuid, tok := s3.DecodeToken("")
			Expect(uuid).To(BeEmpty())
			Expect(tok).To(BeEmpty())
		})
	})
})
