/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"os"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testingConfigDir = "/tmp/dfc_tests"
)

var _ = Describe("ManagerGroup", func() {
	var mgrp *ManagerGroup

	BeforeEach(func() {
		err := os.MkdirAll(testingConfigDir, 0750)
		Expect(err).ShouldNot(HaveOccurred())

		config := cmn.GCO.BeginUpdate()
		config.Confdir = testingConfigDir
		cmn.GCO.CommitUpdate(config)
		mgrp = NewManagerGroup()

		fs.Mountpaths = fs.NewMountedFS()
		fs.Mountpaths.Add(testingConfigDir)
	})

	Context("add", func() {
		It("should add a manager without an error", func() {
			m, err := mgrp.Add("uuid")
			m.unlock()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
			Expect(m.ManagerUUID).To(Equal("uuid"))
			Expect(mgrp.managers).To(HaveLen(1))
		})

		It("should not add a manager when other manager with same uuid already exists", func() {
			m, err := mgrp.Add("uuid")
			m.unlock()
			Expect(err).ShouldNot(HaveOccurred())
			_, err = mgrp.Add("uuid")
			Expect(err).Should(HaveOccurred())
			Expect(mgrp.managers).To(HaveLen(1))
		})
	})

	Context("get", func() {
		It("should return 'not exists' when getting manager with non-existing uuid", func() {
			_, exists := mgrp.Get("uuid")
			Expect(exists).To(BeFalse())
		})

		It("should return manager when manager with given uuid exists", func() {
			m, err := mgrp.Add("uuid")
			m.unlock()
			Expect(err).ShouldNot(HaveOccurred())
			m, exists := mgrp.Get("uuid")
			Expect(exists).To(BeTrue())
			Expect(m).ToNot(BeNil())
			Expect(m.ManagerUUID).To(Equal("uuid"))
		})
	})

	Context("persist", func() {
		ctx.smap = newTestSmap("target")
		ctx.node = ctx.smap.Get().Tmap["target"]

		It("should persist manager when requested", func() {
			m, err := mgrp.Add("uuid")
			rs := &ParsedRequestSpec{Extension: extTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}}
			m.init(rs)
			m.unlock()
			m.setInProgressTo(false)

			Expect(err).ShouldNot(HaveOccurred())
			mgrp.persist("uuid", true)
			m, exists := mgrp.Get("uuid")
			Expect(exists).To(BeTrue())
			Expect(m).ToNot(BeNil())
			Expect(m.ManagerUUID).To(Equal("uuid"))
		})
	})

	AfterEach(func() {
		err := os.RemoveAll(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())
	})
})
