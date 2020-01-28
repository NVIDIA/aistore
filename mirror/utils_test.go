// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mirror", func() {
	const (
		testDir = "/tmp/mirror-test_q/"

		testBucketName = "TEST_LOCAL_MIRROR_BUCKET"
		mpath          = testDir + "mirrortest_mpath/1"
		mpath2         = testDir + "mirrortest_mpath/2"

		testObjectName = "mirrortestobj.ext"
		testObjectSize = 1234
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
		bmdMock         = cluster.NewBaseBownerMock()
		tMock           = cluster.NewTargetMock(bmdMock)
		mi              = fs.MountpathInfo{Path: mpath}
		mi2             = fs.MountpathInfo{Path: mpath2}
		bck             = cmn.Bck{Name: testBucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		bucketPath      = mi.MakePathBucket(fs.ObjectType, bck)
		defaultObjFQN   = mi.MakePathBucketObject(fs.ObjectType, bck, testObjectName)
		expectedCopyFQN = mi2.MakePathBucketObject(fs.ObjectType, bck, testObjectName)
	)

	cbck := cluster.NewBckEmbed(bck)
	cbck.Props = &cmn.BucketProps{
		Cksum:  cmn.CksumConf{Type: cmn.ChecksumXXHash},
		LRU:    cmn.LRUConf{Enabled: true},
		Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
	}
	bmdMock.Add(cbck)

	BeforeEach(func() {
		_ = cmn.CreateDir(mpath)
		_ = cmn.CreateDir(mpath2)
	})

	AfterEach(func() {
		_ = os.RemoveAll(testDir)
	})

	Describe("copyTo", func() {
		It("should copy correctly object and set xattrs", func() {
			createTestFile(bucketPath, testObjectName, testObjectSize)
			lom := newBasicLom(defaultObjFQN, tMock)
			lom.SetSize(testObjectSize)
			Expect(lom.Persist()).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

			// Make copy
			clone, err := copyTo(lom, &mi2, nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expectedCopyFQN).To(BeARegularFile())
			Expect(clone.Size()).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			Expect(clone.IsCopy()).To(BeTrue())
			Expect(clone.NumCopies()).To(Equal(2))
			Expect(clone.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			/*
			 * Reload default LOM and copyLOM
			 */

			// Check reloaded default LOM
			newLOM := newBasicLom(defaultObjFQN, tMock)
			Expect(newLOM.Load(false)).ShouldNot(HaveOccurred())
			Expect(newLOM.IsCopy()).To(BeFalse())
			Expect(newLOM.HasCopies()).To(BeTrue())
			Expect(newLOM.NumCopies()).To(Equal(2))
			Expect(newLOM.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			// Check reloaded copyLOM
			copyLOM := newBasicLom(expectedCopyFQN, tMock)
			Expect(copyLOM.Load(false)).ShouldNot(HaveOccurred())
			copyCksum, err := copyLOM.CksumComputeIfMissing()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(copyCksum.Value()).To(Equal(newLOM.Cksum().Value()))
			Expect(copyLOM.HrwFQN).To(BeEquivalentTo(lom.HrwFQN))
			Expect(copyLOM.IsCopy()).To(BeTrue())
			Expect(copyLOM.HasCopies()).To(BeTrue())
		})
	})
})

func createTestFile(filePath, objName string, size int64) {
	err := cmn.CreateDir(filePath)
	Expect(err).ShouldNot(HaveOccurred())

	_, err = tutils.NewFileReader(filePath, objName, size, false)
	Expect(err).ShouldNot(HaveOccurred())
}

func newBasicLom(fqn string, t cluster.Target) *cluster.LOM {
	lom := &cluster.LOM{T: t, FQN: fqn}
	err := lom.Init(cmn.Bck{})
	Expect(err).NotTo(HaveOccurred())
	lom.Uncache()
	return lom
}
