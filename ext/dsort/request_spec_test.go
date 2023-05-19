// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"math"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RequestSpec", func() {
	BeforeEach(func() {
		fs.TestNew(nil)

		config := cmn.GCO.BeginUpdate()
		config.DSort.DefaultMaxMemUsage = "90%"
		cmn.GCO.CommitUpdate(config)
	})

	Context("requests specs which should pass", func() {
		It("should parse minimal spec", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111..2}-suffix",
				OutputFormat:    "prefix-{10..111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "80%",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.Bck.Name).To(Equal("test"))
			Expect(parsed.Bck.Provider).To(Equal(apc.AIS))
			Expect(parsed.OutputBck.Name).To(Equal("test"))
			Expect(parsed.OutputBck.Provider).To(Equal(apc.AIS))
			Expect(parsed.Extension).To(Equal(archive.ExtTar))

			Expect(parsed.InputFormat.Template).To(Equal(cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       2,
					DigitCount: 4,
					Gap:        "-suffix",
				}},
			}))

			Expect(parsed.OutputFormat.Template).To(Equal(cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      10,
					End:        111,
					Step:       1,
					DigitCount: 2,
					Gap:        "-suffix",
				}},
			}))

			Expect(parsed.OutputShardSize).To(BeEquivalentTo(10 * cos.KiB))

			Expect(parsed.MaxMemUsage.Type).To(Equal(cos.QuantityPercent))
			Expect(parsed.MaxMemUsage.Value).To(BeEquivalentTo(80))
		})

		It("should set buckets correctly", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Provider: apc.AWS, Name: "test"},
				OutputBck:       cmn.Bck{Provider: apc.AWS, Name: "testing"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111..2}-suffix",
				OutputFormat:    "prefix-{10..111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "80%",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.Bck.Name).To(Equal("test"))
			Expect(parsed.Bck.Provider).To(Equal(apc.AWS))
			Expect(parsed.OutputBck.Name).To(Equal("testing"))
			Expect(parsed.OutputBck.Provider).To(Equal(apc.AWS))
		})

		It("should parse spec with mem usage as bytes", func() {
			rs := RequestSpec{
				Bck: cmn.Bck{Name: "test"},

				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "80 GB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.MaxMemUsage.Type).To(Equal(cos.QuantityBytes))
			Expect(parsed.MaxMemUsage.Value).To(BeEquivalentTo(80 * 1024 * 1024 * 1024))
		})

		It("should parse spec with .tgz extension", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTgz,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.Extension).To(Equal(archive.ExtTgz))
		})

		It("should parse spec with .tar.gz extension", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTarTgz,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.Extension).To(Equal(archive.ExtTarTgz))
		})

		It("should parse spec with .tar.gz extension", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtZip,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.Extension).To(Equal(archive.ExtZip))
		})

		It("should parse spec with %06d syntax", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTgz,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-%06d-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.OutputFormat.Template).To(Equal(cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        math.MaxInt64 - 1,
					Step:       1,
					DigitCount: 6,
					Gap:        "-suffix",
				}},
			}))
		})

		It("should parse spec with @ syntax", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTgz,
				InputFormat:     "prefix@0111-suffix",
				OutputFormat:    "prefix-@000111-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.InputFormat.Type).To(Equal(templAt))
			Expect(parsed.InputFormat.Template).To(Equal(cos.ParsedTemplate{
				Prefix: "prefix",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        111,
					Step:       1,
					DigitCount: 4,
					Gap:        "-suffix",
				}},
			}))

			Expect(parsed.OutputFormat.Template).To(Equal(cos.ParsedTemplate{
				Prefix: "prefix-",
				Ranges: []cos.TemplateRange{{
					Start:      0,
					End:        111,
					Step:       1,
					DigitCount: 6,
					Gap:        "-suffix",
				}},
			}))
		})

		It("should parse spec and set default conc limits", func() {
			rs := RequestSpec{
				Bck:                 cmn.Bck{Name: "test"},
				Extension:           archive.ExtTar,
				InputFormat:         "prefix-{0010..0111}-suffix",
				OutputFormat:        "prefix-{0010..0111}-suffix",
				OutputShardSize:     "10KB",
				CreateConcMaxLimit:  0,
				ExtractConcMaxLimit: 0,
				Algorithm:           SortAlgorithm{Kind: SortKindNone},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.CreateConcMaxLimit).To(BeEquivalentTo(0))
			Expect(parsed.ExtractConcMaxLimit).To(BeEquivalentTo(0))
		})

		It("should parse spec and set the global config values or override them", func() {
			cfg := cmn.GCO.BeginUpdate()
			cfg.DSort.DSorterMemThreshold = "80%"
			cfg.DSort.MissingShards = cmn.IgnoreReaction
			cmn.GCO.CommitUpdate(cfg)

			rs := RequestSpec{
				Bck:                 cmn.Bck{Name: "test"},
				Extension:           archive.ExtTar,
				InputFormat:         "prefix-{0010..0111}-suffix",
				OutputFormat:        "prefix-{0010..0111}-suffix",
				OutputShardSize:     "10KB",
				CreateConcMaxLimit:  0,
				ExtractConcMaxLimit: 0,
				Algorithm:           SortAlgorithm{Kind: SortKindNone},

				DSortConf: cmn.DSortConf{
					DuplicatedRecords:   cmn.AbortReaction,
					MissingShards:       "", // should be set to default
					EKMMalformedLine:    cmn.IgnoreReaction,
					EKMMissingKey:       cmn.WarnReaction,
					DSorterMemThreshold: "",
				},
			}
			parsed, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			Expect(parsed.DuplicatedRecords).To(Equal(cmn.AbortReaction))
			Expect(parsed.MissingShards).To(Equal(cmn.IgnoreReaction))
			Expect(parsed.EKMMalformedLine).To(Equal(cmn.IgnoreReaction))
			Expect(parsed.EKMMissingKey).To(Equal(cmn.WarnReaction))
			Expect(parsed.DSorterMemThreshold).To(Equal("80%"))
		})

		It("should pass when output shard is zero and bash or @ template is used for output format", func() {
			rs := RequestSpec{
				Bck:          cmn.Bck{Name: "test"},
				Extension:    archive.ExtTar,
				InputFormat:  "prefix-{0010..0111..2}-suffix",
				OutputFormat: "prefix-{10..111}-suffix",
				MaxMemUsage:  "80%",
			}
			_, err := rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())

			rs = RequestSpec{
				Bck:          cmn.Bck{Name: "test"},
				Extension:    archive.ExtTar,
				InputFormat:  "prefix-{0010..0111..2}-suffix",
				OutputFormat: "prefix-@111-suffix",
				MaxMemUsage:  "80%",
			}
			_, err = rs.Parse()
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Context("request specs which shall NOT pass", func() {
		It("should fail due to missing bucket property", func() {
			rs := RequestSpec{
				Extension:       ".txt",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errMissingBucket))
		})

		It("should fail due to invalid bucket provider", func() {
			rs := RequestSpec{
				Bck:       cmn.Bck{Provider: "invalid", Name: "test"},
				Extension: ".txt",
				Algorithm: SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).Should(MatchError(&cmn.ErrInvalidBackendProvider{}))
		})

		It("should fail due to invalid output bucket provider", func() {
			rs := RequestSpec{
				Bck:       cmn.Bck{Provider: apc.AIS, Name: "test"},
				OutputBck: cmn.Bck{Provider: "invalid", Name: "test"},
				Extension: ".txt",
				Algorithm: SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).Should(MatchError(&cmn.ErrInvalidBackendProvider{}))
		})

		It("should fail due to start after end in input format", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				OutputShardSize: "10KB",
				InputFormat:     "prefix-{0112..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errInvalidInputTemplate))
		})

		It("should fail due to start after end in output format", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				OutputShardSize: "10KB",
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0112..0111}-suffix",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errInvalidOutputTemplate))
		})

		It("should fail due invalid parentheses", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				OutputShardSize: "10KB",
				InputFormat:     "prefix-}{0001..0111}-suffix",
				OutputFormat:    "prefix-}{0010..0111}-suffix",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errInvalidInputTemplate))
		})

		It("should fail due to invalid extension", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       ".jpg",
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errInvalidExtension))
		})

		It("should fail due to invalid mem usage specification", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "80",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(cos.ErrInvalidQuantityUsage))
		})

		It("should fail due to invalid mem usage percent specified", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "120%",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(cos.ErrInvalidQuantityPercent))
		})

		It("should fail due to invalid mem usage bytes specified", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111}-suffix",
				OutputFormat:    "prefix-{0010..0111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "-1 GB",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(cos.ErrInvalidQuantityUsage))
		})

		It("should fail due to invalid extract concurrency specified", func() {
			rs := RequestSpec{
				Bck:                 cmn.Bck{Name: "test"},
				Extension:           archive.ExtTar,
				InputFormat:         "prefix-{0010..0111}-suffix",
				OutputFormat:        "prefix-{0010..0111}-suffix",
				OutputShardSize:     "10KB",
				ExtractConcMaxLimit: -1,
				Algorithm:           SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errNegativeConcurrencyLimit))
		})

		It("should fail due to invalid create concurrency specified", func() {
			rs := RequestSpec{
				Bck:                cmn.Bck{Name: "test"},
				Extension:          archive.ExtTar,
				InputFormat:        "prefix-{0010..0111}-suffix",
				OutputFormat:       "prefix-{0010..0111}-suffix",
				OutputShardSize:    "10KB",
				CreateConcMaxLimit: -1,
				Algorithm:          SortAlgorithm{Kind: SortKindNone},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
			Expect(err).To(Equal(errNegativeConcurrencyLimit))
		})

		It("should fail due to invalid dsort config value", func() {
			rs := RequestSpec{
				Bck:             cmn.Bck{Name: "test"},
				Extension:       archive.ExtTar,
				InputFormat:     "prefix-{0010..0111..2}-suffix",
				OutputFormat:    "prefix-{10..111}-suffix",
				OutputShardSize: "10KB",
				MaxMemUsage:     "80%",
				Algorithm:       SortAlgorithm{Kind: SortKindNone},
				DSortConf:       cmn.DSortConf{DuplicatedRecords: "something"},
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
		})

		It("should fail when output shard size is empty and output format is %06d", func() {
			rs := RequestSpec{
				Bck:          cmn.Bck{Name: "test"},
				Extension:    archive.ExtTar,
				InputFormat:  "prefix-{0010..0111..2}-suffix",
				OutputFormat: "prefix-%06d-suffix",
				MaxMemUsage:  "80%",
			}
			_, err := rs.Parse()
			Expect(err).Should(HaveOccurred())
		})
	})
})
