/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testingConfigDir = "/tmp/ais_tests"
)

var _ = Describe("ManagerGroup", func() {
	var mgrp *ManagerGroup

	BeforeEach(func() {
		err := cmn.CreateDir(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())

		config := cmn.GCO.BeginUpdate()
		config.Confdir = testingConfigDir
		cmn.GCO.CommitUpdate(config)
		mgrp = NewManagerGroup()

		fs.InitMountedFS()
		fs.Mountpaths.Add(testingConfigDir)
	})

	AfterEach(func() {
		err := os.RemoveAll(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())
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

		It("should persist manager but not return by default", func() {
			m, err := mgrp.Add("uuid")
			rs := &ParsedRequestSpec{Extension: ExtTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
			m.init(rs)
			m.unlock()
			m.setInProgressTo(false)

			Expect(err).ShouldNot(HaveOccurred())
			mgrp.persist("uuid")
			m, exists := mgrp.Get("uuid")
			Expect(exists).To(BeFalse())
			Expect(m).To(BeNil())
		})

		It("should persist manager and return it when requested", func() {
			m, err := mgrp.Add("uuid")
			rs := &ParsedRequestSpec{Extension: ExtTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cmn.ParsedQuantity{Type: cmn.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
			m.init(rs)
			m.unlock()
			m.setInProgressTo(false)

			Expect(err).ShouldNot(HaveOccurred())
			mgrp.persist("uuid")
			m, exists := mgrp.Get("uuid", true /*allowPersisted*/)
			Expect(exists).To(BeTrue())
			Expect(m).ToNot(BeNil())
			Expect(m.ManagerUUID).To(Equal("uuid"))
		})
	})
})
