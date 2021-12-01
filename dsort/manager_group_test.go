// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"os"
	"time"

	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testingConfigDir = "/tmp/ais_tests"
)

var _ = Describe("ManagerGroup", func() {
	var (
		mgrp    *ManagerGroup
		validRS = &ParsedRequestSpec{Extension: cos.ExtTar, Algorithm: &SortAlgorithm{Kind: SortKindNone}, MaxMemUsage: cos.ParsedQuantity{Type: cos.QuantityPercent, Value: 0}, DSorterType: DSorterGeneralType}
	)

	BeforeEach(func() {
		err := cos.CreateDir(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())

		config := cmn.GCO.BeginUpdate()
		config.ConfigDir = testingConfigDir
		cmn.GCO.CommitUpdate(config)
		db := mock.NewDBDriver()
		mgrp = NewManagerGroup(db, false /* skip hk*/)

		fs.TestNew(nil)
		fs.Add(testingConfigDir, "daeID")
	})

	AfterEach(func() {
		err := os.RemoveAll(testingConfigDir)
		Expect(err).ShouldNot(HaveOccurred())
		hk.Unreg(cmn.DSortNameLowercase)
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
		ctx.smapOwner = newTestSmap("target")
		ctx.node = ctx.smapOwner.Get().Tmap["target"]

		It("should persist manager but not return by default", func() {
			m, err := mgrp.Add("uuid")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(m.init(validRS)).NotTo(HaveOccurred())
			m.setInProgressTo(false)
			m.unlock()

			mgrp.persist("uuid")
			m, exists := mgrp.Get("uuid")
			Expect(exists).To(BeFalse())
			Expect(m).To(BeNil())
		})

		It("should persist manager and return it when requested", func() {
			m, err := mgrp.Add("uuid")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(m.init(validRS)).ToNot(HaveOccurred())
			m.setInProgressTo(false)
			m.unlock()

			mgrp.persist("uuid")
			m, exists := mgrp.Get("uuid", true /*allowPersisted*/)
			Expect(exists).To(BeTrue())
			Expect(m).ToNot(BeNil())
			Expect(m.ManagerUUID).To(Equal("uuid"))
		})
	})

	Context("housekeep", func() {
		persistManager := func(uuid string, finishedAgo time.Duration) {
			m, err := mgrp.Add(uuid)
			Expect(err).ShouldNot(HaveOccurred())
			err = m.init(validRS)
			Expect(err).ShouldNot(HaveOccurred())
			m.Metrics.Extraction.End = time.Now().Add(-finishedAgo)
			m.unlock()
			mgrp.persist(m.ManagerUUID)
		}

		It("should not clean anything when manager group is empty", func() {
			Expect(mgrp.housekeep()).To(Equal(hk.DayInterval))
			jobs := mgrp.List(nil)
			Expect(jobs).To(HaveLen(0))
		})

		It("should clean managers which are old", func() {
			persistManager("uuid1", 48*time.Hour)
			persistManager("uuid2", 24*time.Hour)
			persistManager("uuid3", 23*time.Hour)
			persistManager("uuid4", 1*time.Hour)

			Expect(mgrp.housekeep()).To(Equal(hk.DayInterval))
			jobs := mgrp.List(nil)
			Expect(jobs).To(HaveLen(2))
			Expect(jobs[0].ID).To(Equal("uuid3"))
			Expect(jobs[1].ID).To(Equal("uuid4"))
		})
	})
})
