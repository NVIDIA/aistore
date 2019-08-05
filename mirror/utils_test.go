// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mirror", func() {
	const (
		TestLocalBucketName = "TEST_LOCAL_MIRROR_BUCKET"
		mpath               = "/tmp/mirrortest_mpath/1"
		mpath2              = "/tmp/mirrortest_mpath/2"
		testObjectName      = "mirrortestobj.ext"
		testObjectSize      = 1234
	)

	_ = cmn.CreateDir(mpath)
	_ = cmn.CreateDir(mpath2)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.InitMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	_ = fs.Mountpaths.Add(mpath)
	_ = fs.Mountpaths.Add(mpath2)
	_ = fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	var (
		tMock      = cluster.NewTargetMock(cluster.NewBaseBownerMock(TestLocalBucketName))
		testDir    = filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, TestLocalBucketName)
		testFQN    = filepath.Join(testDir, testObjectName)
		copyBuf    = make([]byte, testObjectSize)
		mpathInfo2 *fs.MountpathInfo
	)

	av, _ := fs.Mountpaths.Get()
	mpathInfo2 = av[mpath2]

	BeforeEach(func() {
		_ = os.RemoveAll(mpath)
		_ = os.RemoveAll(mpath2)
		_ = cmn.CreateDir(mpath)
		_ = cmn.CreateDir(mpath2)
		_ = cmn.CreateDir(testDir)
	})
	AfterEach(func() {
		_ = os.RemoveAll(mpath)
		_ = os.RemoveAll(mpath2)
	})

	Describe("copyTo", func() {
		It("should copy corectly file and set Xattributes", func() {
			expectedCopyFQN := filepath.Join(mpath2, fs.ObjectType, cmn.LocalBs, TestLocalBucketName, testObjectName)
			createTestFile(testDir, testObjectName, testObjectSize)
			lom := newBasicLom(testFQN, tMock)
			lom.SetSize(testObjectSize)
			Expect(lom.Persist()).NotTo(HaveOccurred())

			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
			clone, err := copyTo(lom, mpathInfo2, copyBuf)
			Expect(clone.IsCopy()).To(BeTrue())
			Expect(err).ShouldNot(HaveOccurred())
			stat, err := os.Stat(expectedCopyFQN)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stat.Size()).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			Expect(clone.GetCopies()).To(HaveLen(1))
			_, ok := clone.GetCopies()[testFQN]
			Expect(ok).To(BeTrue())

			newLom := newBasicLom(testFQN, tMock)
			err = newLom.Load(false)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(newLom.GetCopies()).To(HaveLen(1))
			_, ok = newLom.GetCopies()[expectedCopyFQN]
			Expect(ok).To(BeTrue())

			// Check msic copy data
			lomCopy, err := cluster.LOM{T: tMock, FQN: expectedCopyFQN}.Init("")
			Expect(err).ShouldNot(HaveOccurred())

			err = lomCopy.Load(false)
			Expect(err).ShouldNot(HaveOccurred())
			copyCksm, err := lomCopy.CksumComputeIfMissing()
			Expect(err).ShouldNot(HaveOccurred())
			_, copyCksmVal := copyCksm.Get()
			_, orgCksmVal := lom.Cksum().Get()
			Expect(copyCksmVal).To(BeEquivalentTo(orgCksmVal))

			Expect(lomCopy.HrwFQN).To(BeEquivalentTo(lom.HrwFQN))
			Expect(lom.IsCopy()).To(BeFalse())
			Expect(lom.HasCopies()).To(BeTrue())
			Expect(lomCopy.IsCopy()).To(BeTrue())
			Expect(lomCopy.HasCopies()).To(BeFalse())
		})
	})
})

func createTestFile(filepath, objname string, size int64) {
	_, err := tutils.NewFileReader(filepath, objname, size, false)
	Expect(err).ShouldNot(HaveOccurred())
}

func newBasicLom(fqn string, t cluster.Target) *cluster.LOM {
	lom, err := cluster.LOM{T: t, FQN: fqn}.Init("")
	Expect(err).NotTo(HaveOccurred())
	lom.Uncache()
	return lom
}
