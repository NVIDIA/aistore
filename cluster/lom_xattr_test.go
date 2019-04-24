/*
* Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster_test

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

const xattrmapth = "/tmp/lomxattrtest_mpath/1"

var _ = Describe("LOM Xattributes", func() {

	_ = cmn.CreateDir(xattrmapth)

	_ = fs.Mountpaths.Add(xattrmapth)
	fs.Mountpaths.DisableFsIDCheck()
	_ = fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	tMock := cluster.NewTargetMock(cluster.NewBaseBownerMock(bucketLocalB))

	BeforeEach(func() {
		_ = os.RemoveAll(xattrmapth)
		_ = cmn.CreateDir(xattrmapth)
	})
	AfterEach(func() {
		_ = os.RemoveAll(xattrmapth)

	})

	Describe("xattrs", func() {
		testFileSize := 456
		testObjectName := "xattr-foldr/test-obj.ext"
		//Bucket needs to have checksum enabled
		localFQN := filepath.Join(xattrmapth, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)

		Describe("Persist", func() {
			It("should save correct meta to disk", func() {
				createTestFile(localFQN, testFileSize)
				lom := NewBasicLom(localFQN, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum"))
				lom.SetVersion("dummyversion")
				lom.SetCopyFQN([]string{"some/copy/fqn", "some/other/copy/fqn"})

				Expect(lom.Persist()).NotTo(HaveOccurred())

				b, errstr := fs.GetXattr(localFQN, cmn.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(errstr).To(BeEmpty())

				newLom, err := tutils.GetXattrLom(localFQN, cmn.LocalBs, tMock)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Cksum()).To(BeEquivalentTo(newLom.Cksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.CopyFQN()).To(BeEquivalentTo(newLom.CopyFQN()))
			})

			It("should override old values", func() {
				createTestFile(localFQN, testFileSize)
				lom := NewBasicLom(localFQN, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum1"))
				lom.SetVersion("dummyversion1")
				lom.SetCopyFQN([]string{"some/copy/fqn/1", "some/other/copy/fqn/1"})

				Expect(lom.Persist()).NotTo(HaveOccurred())

				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum2"))
				lom.SetVersion("dummyversion2")
				lom.SetCopyFQN([]string{"some/copy/fqn/2", "some/other/copy/fqn/2"})

				Expect(lom.Persist()).NotTo(HaveOccurred())

				b, errstr := fs.GetXattr(localFQN, cmn.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(errstr).To(BeEmpty())

				newLom, err := tutils.GetXattrLom(localFQN, cmn.LocalBs, tMock)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Cksum()).To(BeEquivalentTo(newLom.Cksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.CopyFQN()).To(BeEquivalentTo(newLom.CopyFQN()))
			})
		})

		Describe("LoadMetaFromFS", func() {
			It("should read fresh meta from fs", func() {
				createTestFile(localFQN, testFileSize)
				lom1 := NewBasicLom(localFQN, tMock)
				lom2 := NewBasicLom(localFQN, tMock)
				lom1.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum"))
				lom1.SetVersion("dummyversion")
				lom1.SetCopyFQN([]string{"some/copy/fqn", "some/other/copy/fqn"})

				Expect(lom1.Persist()).NotTo(HaveOccurred())
				Expect(lom2.LoadMetaFromFS()).NotTo(HaveOccurred())

				Expect(lom1.Cksum()).To(BeEquivalentTo(lom2.Cksum()))
				Expect(lom1.Version()).To(BeEquivalentTo(lom2.Version()))
				Expect(lom1.CopyFQN()).To(BeEquivalentTo(lom2.CopyFQN()))
			})

			It("should fail when checksum does not match", func() {
				createTestFile(localFQN, testFileSize)
				lom := NewBasicLom(localFQN, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum"))
				lom.SetVersion("dummyversion")
				lom.SetCopyFQN([]string{"some/copy/fqn", "some/other/copy/fqn"})

				Expect(lom.Persist()).NotTo(HaveOccurred())

				b, errstr := fs.GetXattr(localFQN, cmn.XattrLOM)
				Expect(errstr).To(BeEmpty())
				b[0] = b[0] + 1 // changing first byte of meta checksum
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, b)).To(BeEmpty())

				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
			})

			It("should fail when meta is corrupted", func() {
				// This test is supposed to end with LoadMetaFromFS error
				// not with nil pointer exception / panic
				createTestFile(localFQN, testFileSize)
				lom := NewBasicLom(localFQN, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "testchecksum"))
				lom.SetVersion("dummyversion")
				lom.SetCopyFQN([]string{"some/copy/fqn", "some/other/copy/fqn"})

				Expect(lom.Persist()).NotTo(HaveOccurred())

				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("1321\nwr;as\n;, ;\n\n;;,,dadsa;aa\n"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("nochecksumnumber\n\n"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("123\n;;;\n;;;\n;da;dsa;\n"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())

				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("1321\xc5\xdfwr\xae\xbdas\xc5\xdf\xae\xbd, \xae\xbd\xc5\xdf\xc5\xdf\xae\xbd\xae\xbd,,dadsa\xae\xbdaa\xc5\xdf"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("nochecksumnumber\xc5\xdf\xc5\xdf"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("123\xc5\xdf\xae\xbd\xae\xbd\xae\xbd\xc5\xdf\xae\xbd\xae\xbd\xae\xbd\xc5\xdf\xae\xbdda\xae\xbddsa\xae\xbd\xc5\xdf"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
				Expect(fs.SetXattr(localFQN, cmn.XattrLOM, []byte("\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf\xc5\xdf"))).To(BeEmpty())
				Expect(lom.LoadMetaFromFS()).To(HaveOccurred())
			})
		})
	})
})
