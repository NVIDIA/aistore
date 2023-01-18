// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"os"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mirror", func() {
	const (
		testDir = "/tmp/mirror-test_q/"

		testBucketName = "TEST_LOCAL_MIRROR_BUCKET"
		mpath          = testDir + "mirrortest_mpath/111"
		mpath2         = testDir + "mirrortest_mpath/222"

		testObjectName = "mirrortestobj.ext"
		testObjectSize = 1234
	)

	_ = cos.CreateDir(mpath)
	_ = cos.CreateDir(mpath2)

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	fs.TestDisableValidation()
	_, _ = fs.Add(mpath, "daeID")
	_, _ = fs.Add(mpath2, "daeID")
	_ = fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{})

	var (
		props = &cmn.BucketProps{
			Cksum:  cmn.CksumConf{Type: cos.ChecksumXXHash},
			LRU:    cmn.LRUConf{Enabled: true},
			Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
			BID:    1,
		}
		bck             = cluster.Bck{Name: testBucketName, Provider: apc.AIS, Ns: cmn.NsGlobal, Props: props}
		bmdMock         = mock.NewBaseBownerMock(&bck)
		mi              = fs.MountpathInfo{Path: mpath}
		mi2             = fs.MountpathInfo{Path: mpath2}
		bucketPath      = mi.MakePathCT(bck.Bucket(), fs.ObjectType)
		defaultObjFQN   = mi.MakePathFQN(bck.Bucket(), fs.ObjectType, testObjectName)
		expectedCopyFQN = mi2.MakePathFQN(bck.Bucket(), fs.ObjectType, testObjectName)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(mpath)
		_ = cos.CreateDir(mpath2)
		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_ = os.RemoveAll(testDir)
	})

	// NOTE:
	// the test creates copies; there's a built-in assumption that `mi` will be
	// the HRW mountpath,
	// while `mi2` will not (and, therefore, can be used to place the copy).
	// Ultimately, this depends on the specific HRW hash; adding
	// Expect(lom.IsHRW()).To(BeTrue()) to catch that sooner.

	Describe("copyTo", func() {
		It("should copy correctly object and set xattrs", func() {
			createTestFile(bucketPath, testObjectName, testObjectSize)
			lom := newBasicLom(defaultObjFQN)
			Expect(lom.IsHRW()).To(BeTrue())
			lom.SetSize(testObjectSize)
			lom.SetAtimeUnix(time.Now().UnixNano())
			Expect(lom.Persist()).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

			// Make copy
			lom.Lock(true)
			defer lom.Unlock(true)
			copyFQN := mi2.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
			clone, err := lom.Copy2FQN(copyFQN, nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expectedCopyFQN).To(BeARegularFile())
			Expect(clone.SizeBytes(true)).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			Expect(clone.IsCopy()).To(BeTrue())
			Expect(clone.NumCopies()).To(Equal(2))
			Expect(clone.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			/*
			 * Reload default LOM and copyLOM
			 */

			// Check reloaded default LOM
			newLOM := newBasicLom(defaultObjFQN)
			Expect(newLOM.IsHRW()).To(BeTrue())
			Expect(newLOM.Load(false, true)).ShouldNot(HaveOccurred())
			Expect(newLOM.IsCopy()).To(BeFalse())
			Expect(newLOM.HasCopies()).To(BeTrue())
			Expect(newLOM.NumCopies()).To(Equal(2))
			Expect(newLOM.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			// Check reloaded copyLOM
			copyLOM := newBasicLom(expectedCopyFQN)
			Expect(copyLOM.Load(false, false)).ShouldNot(HaveOccurred())
			copyCksum, err := copyLOM.ComputeSetCksum()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(copyCksum.Value()).To(Equal(newLOM.Checksum().Value()))
			Expect(copyLOM.HrwFQN).To(BeEquivalentTo(lom.HrwFQN))
			Expect(copyLOM.IsCopy()).To(BeTrue())
			Expect(copyLOM.HasCopies()).To(BeTrue())
		})
	})
})

func createTestFile(filePath, objName string, size int64) {
	err := cos.CreateDir(filePath)
	Expect(err).ShouldNot(HaveOccurred())

	r, err := readers.NewFileReader(filePath, objName, size, cos.ChecksumNone)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(r.Close()).ShouldNot(HaveOccurred())
}

func newBasicLom(fqn string) *cluster.LOM {
	lom := &cluster.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())
	lom.Uncache(false)
	return lom
}
