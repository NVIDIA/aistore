/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"github.com/NVIDIA/dfcpub/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Init", func() {
	ctx.smap = newTestSmap("target")
	ctx.node = ctx.smap.Get().Tmap["target"]
	fs.Mountpaths = fs.NewMountedFS()

	It("should init with tar extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		_, ok := m.extractCreator.(*tarExtractCreator)
		Expect(ok).To(BeTrue())
		Expect(m.extractCreator.UsingCompression()).To(BeFalse())
	})

	It("should init with tgz extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extTarTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		_, ok := m.extractCreator.(*tarExtractCreator)
		Expect(ok).To(BeTrue())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with tar.gz extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		_, ok := m.extractCreator.(*tarExtractCreator)
		Expect(ok).To(BeTrue())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with zip extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extZip, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		_, ok := m.extractCreator.(*zipExtractCreator)
		Expect(ok).To(BeTrue())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})
})

var _ = Describe("Records", func() {
	const objectSize = 100

	Context("insert", func() {
		It("should insert record", func() {
			records := newRecords(0)
			records.insert(&record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*recordObj{
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})
			records.insert(&record{
				Key:         "some_key1",
				ContentPath: "some_key1",
				Objects: []*recordObj{
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
				},
			})

			Expect(records.len()).To(Equal(2))
		})

		It("should insert record but merge it", func() {
			records := newRecords(0)
			records.insert(&record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*recordObj{
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".cls",
					},
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".txt",
					},
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".jpg",
					},
				},
			})

			Expect(records.len()).To(Equal(1))
			Expect(records.objectCount()).To(Equal(3))
			r := records.all()[0]
			Expect(r.totalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))

			records.insert(&record{
				Key:         "some_key",
				ContentPath: "some_key",
				Objects: []*recordObj{
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".xml",
					},
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".png",
					},
					&recordObj{
						MetadataSize: 10,
						Size:         objectSize,
						Extension:    ".tar",
					},
				},
			})

			Expect(records.len()).To(Equal(1))
			Expect(records.objectCount()).To(Equal(6))
			r = records.all()[0]
			Expect(r.totalSize()).To(BeEquivalentTo(len(r.Objects) * objectSize))
		})
	})
})
