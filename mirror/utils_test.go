// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"os"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Mirror", func() {
	const (
		testDir = "/tmp/mirror-test_q/"

		testBucketName = "TEST_LOCAL_MIRROR_BUCKET"
		mpath          = testDir + "mirrortest_mpath/2"
		mpath2         = testDir + "mirrortest_mpath/1"

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
	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	var (
		props = &cmn.BucketProps{
			Cksum:  cmn.CksumConf{Type: cos.ChecksumXXHash},
			LRU:    cmn.LRUConf{Enabled: true},
			Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
			BID:    1,
		}
		bck             = cmn.Bck{Name: testBucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal, Props: props}
		bmdMock         = cluster.NewBaseBownerMock(&cluster.Bck{Bck: bck})
		mi              = fs.MountpathInfo{Path: mpath}
		mi2             = fs.MountpathInfo{Path: mpath2}
		bucketPath      = mi.MakePathCT(bck, fs.ObjectType)
		defaultObjFQN   = mi.MakePathFQN(bck, fs.ObjectType, testObjectName)
		expectedCopyFQN = mi2.MakePathFQN(bck, fs.ObjectType, testObjectName)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(mpath)
		_ = cos.CreateDir(mpath2)
		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_ = os.RemoveAll(testDir)
	})

	Describe("copyTo", func() {
		It("should copy correctly object and set xattrs", func() {
			createTestFile(bucketPath, testObjectName, testObjectSize)
			lom := newBasicLom(defaultObjFQN)
			lom.SetSize(testObjectSize)
			lom.SetAtimeUnix(time.Now().UnixNano())
			Expect(lom.Persist()).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

			// Make copy
			lom.Lock(true)
			defer lom.Unlock(true)
			copyFQN := mi2.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
			clone, err := lom.CopyObject(copyFQN, nil)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(expectedCopyFQN).To(BeARegularFile())
			Expect(clone.SizeBytes()).To(BeEquivalentTo(testObjectSize))

			// Check copy set
			Expect(clone.IsCopy()).To(BeTrue())
			Expect(clone.NumCopies()).To(Equal(2))
			Expect(clone.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			/*
			 * Reload default LOM and copyLOM
			 */

			// Check reloaded default LOM
			newLOM := newBasicLom(defaultObjFQN)
			Expect(newLOM.Load(false, true)).ShouldNot(HaveOccurred())
			Expect(newLOM.IsCopy()).To(BeFalse())
			Expect(newLOM.HasCopies()).To(BeTrue())
			Expect(newLOM.NumCopies()).To(Equal(2))
			Expect(newLOM.GetCopies()).To(And(HaveKey(defaultObjFQN), HaveKey(expectedCopyFQN)))

			// Check reloaded copyLOM
			copyLOM := newBasicLom(expectedCopyFQN)
			Expect(copyLOM.Load(false, false)).ShouldNot(HaveOccurred())
			copyCksum, err := copyLOM.ComputeCksumIfMissing()
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
	lom := &cluster.LOM{FQN: fqn}
	err := lom.Init(cmn.Bck{})
	Expect(err).NotTo(HaveOccurred())
	lom.Uncache(false)
	return lom
}
