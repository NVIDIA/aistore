// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
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
		Expect(m.extractCreator.UsingCompression()).To(BeFalse())
	})

	It("should init with tgz extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extTarTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with tar.gz extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extTgz, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})

	It("should init with zip extension", func() {
		m := &Manager{}
		sr := &ParsedRequestSpec{Extension: extZip, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
		Expect(m.init(sr)).NotTo(HaveOccurred())
		Expect(m.extractCreator.UsingCompression()).To(BeTrue())
	})
})
