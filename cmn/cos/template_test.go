// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"math"

	"github.com/NVIDIA/aistore/cmn/cos"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Template", func() {
	Context("ParseFmtTemplate", func() {
		DescribeTable("parse fmt template without error",
			func(template string, expectedPt cos.ParsedTemplate) {
				pt, err := cos.ParseFmtTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("simple", "%d", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 0,
					Gap:        "",
				}},
			}),
			Entry("with prefix", "prefix-%d", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 0,
					Gap:        "",
				}},
			}),
			Entry("with prefix and suffix", "prefix-%d-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 0,
					Gap:        "-suffix",
				}},
			}),
			Entry("with 0 digits", "prefix-%00d-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 0,
					Gap:        "-suffix",
				}},
			}),
			Entry("with multiple digits", "prefix-%06d-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 6,
					Gap:        "-suffix",
				}},
			}),
			Entry("with large number of digits", "prefix-%0152d-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 152,
					Gap:        "-suffix",
				}},
			}),
		)

		DescribeTable("parse fmt template with error",
			func(template string) {
				_, err := cos.ParseFmtTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing %", "prefix-06d-suffix"),
			Entry("missing d", "prefix-%06-suffix"),
			Entry("% after d", "prefix-d%06-suffix"),
			Entry("negative digits", "prefix-%0-6d-suffix"),
			Entry("no digits specified", "prefix-%0d-suffix"),
			Entry("no zero specified", "prefix-%6d-suffix"),
			// Currently rejecting even if looks quite OK.
			Entry("second %", "prefix-%06d-%"),
			Entry("double %%", "prefix-%%06d-suffix"),
			// Currently rejecting even if the second format is not used.
			Entry("second format string", "prefix-%06d-%06d"),
		)
	})

	Context("ParseBashTemplate", func() {
		DescribeTable("parse bash template without error",
			func(template string, expectedPt cos.ParsedTemplate) {
				pt, err := cos.ParseBashTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("with step", "prefix-{0010..0111..2}-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       2,
					DigitCount: 4,
					Gap:        "-suffix",
				}},
			}),
			Entry("without step and suffix", "prefix-{0010..0111}", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("minimal", "{1..2}", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal multiple digits", "{110..220..10}", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      110,
					End:        220,
					Step:       10,
					DigitCount: 3,
					Gap:        "",
				}},
			}),
			Entry("minimal with digit count", "{1..02}", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal with special suffix", "{1..02}}", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      1,
					End:        2,
					Step:       1,
					DigitCount: 1,
					Gap:        "}",
				}},
			}),
			Entry("multi-range", "prefix-{0010..0111..2}-gap-{10..12}-gap2-{0040..0099..4}-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{
					{
						Start:      10,
						End:        111,
						Step:       2,
						DigitCount: 4,
						Gap:        "-gap-",
					},
					{
						Start:      10,
						End:        12,
						Step:       1,
						DigitCount: 2,
						Gap:        "-gap2-",
					},
					{
						Start:      40,
						End:        99,
						Step:       4,
						DigitCount: 4,
						Gap:        "-suffix",
					},
				},
			}),
		)

		DescribeTable("parse bash template with error",
			func(template string) {
				_, err := cos.ParseBashTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing {", "prefix-0010..0111..2}-suffix"),
			Entry("missing }", "prefix-{001..009-suffix"),
			Entry("start after end", "prefix-{002..001}-suffix"),
			Entry("negative start", "prefix-{-001..009}-suffix"),
			Entry("negative step", "prefix-{001..009..-1}-suffix"),
			Entry("invalid step", "prefix-{0010..0111..2s}-suffix"),
			Entry("invalid end", "prefix-{0010..0111s..2}-suffix"),
			Entry("invalid start", "prefix-{0010s..0111..2}-suffix"),
			Entry("too many arms", "prefix-{00..10..0111..2}-suffix"),
			Entry("missing end", "prefix-{010..}-suffix"),
			Entry("missing start", "prefix-{..009}-suffix"),
			Entry("missing start and end", "prefix-{..}-suffix"),
			Entry("empty template with prefix and suffix", "prefix-{}-suffix"),
			Entry("empty template", "{}"),
			Entry("nested templates", "{{}}"),
			Entry("nested templates with numbers", "{{1..2}}"),
			Entry("interleaving templates", "{1..2{3..4}}"),
		)
	})

	Context("ParseAtTemplate", func() {
		DescribeTable("parse at template without error",
			func(template string, expectedPt cos.ParsedTemplate) {
				pt, err := cos.ParseAtTemplate(template)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(pt).To(Equal(expectedPt))
			},
			Entry("full featured template", "prefix-@010-suffix", cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 3,
					Gap:        "-suffix",
				}},
			}),
			Entry("minimal with prefix", "pref@9", cos.ParsedTemplate{
				Prefix: "pref",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        9,
					Step:       1,
					DigitCount: 1,
					Gap:        "",
				}},
			}),
			Entry("minimal", "@0010", cos.ParsedTemplate{
				Prefix: "",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        10,
					Step:       1,
					DigitCount: 4,
					Gap:        "",
				}},
			}),
			Entry("multi-range", "pref@9-gap-@0100-suffix", cos.ParsedTemplate{
				Prefix: "pref",
				Ranges: []cos.TemplateRange{
					{
						Start:      0,
						End:        9,
						Step:       1,
						DigitCount: 1,
						Gap:        "-gap-",
					},
					{
						Start:      0,
						End:        100,
						Step:       1,
						DigitCount: 4,
						Gap:        "-suffix",
					},
				},
			}),
		)

		DescribeTable("parse at template with error",
			func(template string) {
				_, err := cos.ParseAtTemplate(template)
				Expect(err).Should(HaveOccurred())
			},
			Entry("missing @", "prefix-01-suffix"),
			Entry("negative end", "prefix-@-0001-suffix"),
			Entry("repetitive @", "prefix-@@0010-suffix"),
			Entry("non-digit range", "prefix-@d@0010-suffix"),
		)
	})

	Context("ParsedTemplate", func() {
		DescribeTable("iter method",
			func(template string, expectedStrs ...string) {
				pt, err := cos.ParseBashTemplate(template)
				Expect(err).NotTo(HaveOccurred())

				var i int
				pt.InitIter()
				for str, hasNext := pt.Next(); hasNext; str, hasNext = pt.Next() {
					Expect(str).To(Equal(expectedStrs[i]))
					i++
				}
				Expect(i).To(Equal(len(expectedStrs)))
				Expect(i).To(Equal(int(pt.Count())))
			},
			Entry(
				"simple template", "prefix-{0010..0013..2}-suffix",
				"prefix-0010-suffix", "prefix-0012-suffix",
			),
			Entry(
				"multi-range template", "prefix-{0010..0013..2}-gap-{1..2}-suffix",
				"prefix-0010-gap-1-suffix", "prefix-0010-gap-2-suffix", "prefix-0012-gap-1-suffix", "prefix-0012-gap-2-suffix",
			),
			Entry(
				"large step", "prefix-{0010..0013..2}-gap-{1..2..3}-suffix",
				"prefix-0010-gap-1-suffix", "prefix-0012-gap-1-suffix",
			),
		)
	})
})
