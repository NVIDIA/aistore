// Package cluster_test provides tests of cluster package
/*
* Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster_test

import (
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LOM Xattributes", func() {
	const (
		tmpDir     = "/tmp/lom_xattr_test"
		xattrMpath = tmpDir + "/xattr"
		copyMpath  = tmpDir + "/copy"

		bucketLocal = "LOM_TEST_Local"
	)

	var (
		localBck = cmn.Bck{Name: bucketLocal, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
	)

	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	var (
		copyMpathInfo *fs.MountpathInfo
		mix           = fs.MountpathInfo{Path: xattrMpath}
		bmdMock       = cluster.NewBaseBownerMock()
		tMock         = cluster.NewTargetMock(bmdMock)
	)
	bmdMock.Add(cluster.NewBck(bucketLocal, cmn.ProviderAIS, cmn.NsGlobal,
		&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}))

	BeforeEach(func() {
		_ = cmn.CreateDir(xattrMpath)
		_ = cmn.CreateDir(copyMpath)

		fs.Mountpaths.DisableFsIDCheck()
		_ = fs.Mountpaths.Add(xattrMpath)
		_ = fs.Mountpaths.Add(copyMpath)

		available, _ := fs.Mountpaths.Get()
		copyMpathInfo = available[copyMpath]
	})

	AfterEach(func() {
		_ = fs.Mountpaths.Remove(xattrMpath)
		_ = fs.Mountpaths.Remove(copyMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("xattrs", func() {
		var (
			testFileSize   = 456
			testObjectName = "xattr-foldr/test-obj.ext"

			// Bucket needs to have checksum enabled
			localFQN = mix.MakePathFQN(localBck, fs.ObjectType, testObjectName)

			fqns = []string{
				copyMpath + "/copy/fqn",
				copyMpath + "/other/copy/fqn",
			}
		)

		Describe("Persist", func() {
			It("should save correct meta to disk", func() {
				lom := filePut(localFQN, testFileSize, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "test_checksum"))
				lom.SetVersion("dummy_version")
				lom.SetCustomMD(cmn.SimpleKVs{
					cluster.SourceObjMD:        cluster.SourceGoogleObjMD,
					cluster.GoogleVersionObjMD: "version",
					cluster.GoogleCRC32CObjMD:  "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				lom.Uncache()
				newLom := NewBasicLom(localFQN, tMock)
				err = newLom.Load(false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Cksum()).To(BeEquivalentTo(newLom.Cksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
				Expect(lom.CustomMD()).To(HaveLen(3))
				Expect(lom.CustomMD()).To(BeEquivalentTo(newLom.CustomMD()))
			})

			It("should override old values", func() {
				lom := filePut(localFQN, testFileSize, tMock)
				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "test_checksum"))
				lom.SetVersion("dummy_version1")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())

				lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "test_checksum"))
				lom.SetVersion("dummy_version2")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				lom.Uncache()
				newLom := NewBasicLom(localFQN, tMock)
				err = newLom.Load(false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Cksum()).To(BeEquivalentTo(newLom.Cksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
			})
		})

		Describe("LoadMetaFromFS", func() {
			It("should read fresh meta from fs", func() {
				createTestFile(localFQN, testFileSize)
				lom1 := NewBasicLom(localFQN, tMock)
				lom2 := NewBasicLom(localFQN, tMock)
				lom1.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "test_checksum"))
				lom1.SetVersion("dummy_version")
				Expect(lom1.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom1.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom1.Persist()).NotTo(HaveOccurred())

				err := lom2.LoadMetaFromFS()
				Expect(err).NotTo(HaveOccurred())

				Expect(lom1.Cksum()).To(BeEquivalentTo(lom2.Cksum()))
				Expect(lom1.Version()).To(BeEquivalentTo(lom2.Version()))
				Expect(lom1.GetCopies()).To(HaveLen(3))
				Expect(lom1.GetCopies()).To(BeEquivalentTo(lom2.GetCopies()))
			})

			Describe("error cases", func() {
				var lom *cluster.LOM

				BeforeEach(func() {
					createTestFile(localFQN, testFileSize)
					lom = NewBasicLom(localFQN, tMock)
					lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "test_checksum"))
					lom.SetVersion("dummy_version")
					Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(lom.Persist()).NotTo(HaveOccurred())
				})

				It("should fail when checksum does not match", func() {
					b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[2]++ // changing first byte of meta checksum
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError(&cmn.BadCksumError{}))
				})

				It("should fail when checksum type is invalid", func() {
					b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 200 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("invalid lmeta: unknown checksum 200"))

					b, err = fs.GetXattr(localFQN, cluster.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 0 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("invalid lmeta: unknown checksum 0"))
				})

				It("should fail when metadata version is invalid", func() {
					b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[0] = 128 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("invalid lmeta: unknown version 128"))

					b[0] = 0 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("invalid lmeta: unknown version 0"))
				})

				It("should fail when metadata is too short", func() {
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, []byte{1})).NotTo(HaveOccurred())
					err := lom.LoadMetaFromFS()
					Expect(err).To(HaveOccurred())

					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, []byte{1, 1, 2})).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("invalid lmeta: too short (3)"))
				})

				It("should fail when meta is corrupted", func() {
					// This test is supposed to end with LoadMetaFromFS error
					// not with nil pointer exception / panic
					b, err := fs.GetXattr(localFQN, cluster.XattrLOM)
					Expect(err).NotTo(HaveOccurred())
					copy(b[40:], "1321wr")
					Expect(fs.SetXattr(localFQN, cluster.XattrLOM, b)).NotTo(HaveOccurred())

					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError(&cmn.BadCksumError{}))
				})
			})
		})
	})
})
