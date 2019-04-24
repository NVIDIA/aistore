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
	fs.Mountpaths = fs.NewMountedFS()
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

			Expect(lom.ValidateChecksum(true)).To(BeEmpty())
			Expect(copyTo(lom, mpathInfo2, copyBuf)).ShouldNot(HaveOccurred())
			stat, err := os.Stat(expectedCopyFQN)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(stat.Size()).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			newCopyLom, err := tutils.GetXattrLom(expectedCopyFQN, cmn.LocalBs, tMock)
			Expect(err).NotTo(HaveOccurred())
			Expect(newCopyLom.CopyFQN()).To(HaveLen(1))
			Expect(newCopyLom.CopyFQN()[0]).To(BeEquivalentTo(testFQN))

			newLom, err := tutils.GetXattrLom(testFQN, cmn.LocalBs, tMock)
			Expect(err).NotTo(HaveOccurred())
			Expect(newLom.CopyFQN()).To(HaveLen(1))
			Expect(newLom.CopyFQN()[0]).To(BeEquivalentTo(expectedCopyFQN))

			// Check msic copy data
			lomCopy, errstr := cluster.LOM{T: tMock, FQN: expectedCopyFQN}.Init()
			Expect(errstr).To(BeEmpty())

			_, errstr = lomCopy.Load(false)
			Expect(errstr).To(BeEmpty())
			copyCksm, errstr := lomCopy.CksumComputeIfMissing()
			Expect(errstr).To(BeEmpty())
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
	lom, err := cluster.LOM{T: t, FQN: fqn}.Init()
	Expect(err).To(BeEmpty())
	lom.Uncache()
	return lom
}
