// Package core_test provides tests for cluster package
/*
* Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"os"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("LOM Xattributes", func() {
	const (
		tmpDir     = "/tmp/lom_xattr_test"
		xattrMpath = tmpDir + "/xattr"
		copyMpath  = tmpDir + "/copy"

		bucketLocal  = "LOM_TEST_Local"
		bucketCached = "LOM_TEST_Cached"
	)

	localBck := cmn.Bck{Name: bucketLocal, Provider: apc.AIS, Ns: cmn.NsGlobal}
	cachedBck := cmn.Bck{Name: bucketCached, Provider: apc.AIS, Ns: cmn.NsGlobal}

	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}, true)

	var (
		copyMpathInfo *fs.Mountpath
		mix           = fs.Mountpath{Path: xattrMpath}
		bmdMock       = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketLocal, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, BID: 201},
			),
			meta.NewBck(
				bucketCached, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{
					Cksum:       cmn.CksumConf{Type: cos.ChecksumOneXxh},
					WritePolicy: cmn.WritePolicyConf{Data: apc.WriteImmediate, MD: apc.WriteNever},
					BID:         202,
				},
			),
		)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(xattrMpath)
		_ = cos.CreateDir(copyMpath)

		_, _ = fs.Add(xattrMpath, "daeID")
		_, _ = fs.Add(copyMpath, "daeID")

		available := fs.GetAvail()
		copyMpathInfo = available[copyMpath]

		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_, _ = fs.Remove(xattrMpath)
		_, _ = fs.Remove(copyMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("xattrs", func() {
		var (
			testFileSize   = 456
			testObjectName = "xattr-foldr/test-obj.ext"

			// Bucket needs to have checksum enabled
			localFQN  = mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName+".qqq")
			cachedFQN = mix.MakePathFQN(&cachedBck, fs.ObjectType, testObjectName)

			fqns []string
		)

		BeforeEach(func() {
			fqns = []string{
				copyMpathInfo.MakePathFQN(&localBck, fs.ObjectType, "copy/111/fqn"),
				copyMpathInfo.MakePathFQN(&localBck, fs.ObjectType, "other/copy/fqn"),
			}

			// NOTE:
			// the test creates copies; there's a built-in assumption that `mi` will be
			// the HRW mountpath,
			// while `mi2` will not (and, therefore, can be used to place the copy).
			// Ultimately, this depends on the specific HRW hash; adding
			// Expect(lom.IsHRW()).To(Be...)) to catch that sooner.

			for _, fqn := range fqns {
				lom := filePut(fqn, testFileSize)
				Expect(lom.IsHRW()).To(BeFalse())
			}
		})

		Describe("Persist", func() {
			It("should save correct meta to disk", func() {
				lom := filePut(localFQN, testFileSize)
				Expect(lom.IsHRW()).To(BeTrue())
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
				lom.SetVersion("dummy_version")
				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, core.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				newLom := NewBasicLom(localFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
				Expect(lom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should _not_ save meta to disk", func() {
				lom := filePut(cachedFQN, testFileSize)
				Expect(lom.IsHRW()).To(BeTrue())
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
				lom.SetVersion("dummy_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(cachedFQN, core.XattrLOM)
				Expect(b).To(BeEmpty())
				Expect(err).To(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				ver := lom.Version()
				lom.UncacheUnless()

				newLom := NewBasicLom(cachedFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(ver).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
				Expect(lom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should copy object with meta in memory", func() {
				lom := filePut(cachedFQN, testFileSize)
				lom.Lock(true)
				defer lom.Unlock(true)
				cksumHash, err := lom.ComputeCksum(lom.CksumType(), true)
				Expect(err).NotTo(HaveOccurred())
				lom.SetCksum(cksumHash.Clone())
				lom.SetVersion("first_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				lom.SetVersion("second_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(cachedFQN, core.XattrLOM)
				Expect(b).To(BeEmpty())
				Expect(err).To(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				lom.UncacheUnless()
				lom.Load(true, false)
				Expect(lom.Version()).To(BeEquivalentTo("second_version"))
				Expect(lom.GetCopies()).To(HaveLen(3))

				buf := make([]byte, cos.KiB)
				newLom, err := lom.Copy2FQN(cachedFQN+"-copy", buf)
				Expect(err).NotTo(HaveOccurred())

				err = newLom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(newLom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should override old values", func() {
				lom := filePut(localFQN, testFileSize)
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
				lom.SetVersion("dummy_version1")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())

				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
				lom.SetVersion("dummy_version2")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, core.XattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				newLom := NewBasicLom(localFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
			})
		})

		Describe("LoadMetaFromFS", func() {
			It("should read fresh meta from fs", func() {
				createTestFile(localFQN, testFileSize)
				lom1 := NewBasicLom(localFQN)
				lom2 := NewBasicLom(localFQN)
				lom1.Lock(true)
				defer lom1.Unlock(true)
				lom1.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
				lom1.SetVersion("dummy_version")
				Expect(lom1.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom1.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom1)).NotTo(HaveOccurred())

				err := lom2.LoadMetaFromFS()
				Expect(err).NotTo(HaveOccurred())

				Expect(lom1.Checksum()).To(BeEquivalentTo(lom2.Checksum()))
				Expect(lom1.Version(true)).To(BeEquivalentTo(lom2.Version(true)))
				Expect(lom1.GetCopies()).To(HaveLen(3))
				Expect(lom1.GetCopies()).To(BeEquivalentTo(lom2.GetCopies()))
			})

			Describe("error cases", func() {
				var lom *core.LOM

				BeforeEach(func() {
					createTestFile(localFQN, testFileSize)
					lom = NewBasicLom(localFQN)
					lom.Lock(true)
					defer lom.Unlock(true)
					lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "test_checksum"))
					lom.SetVersion("dummy_version")
					Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(persist(lom)).NotTo(HaveOccurred())
				})

				It("should fail when checksum does not match", func() {
					b, err := fs.GetXattr(localFQN, core.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[2]++ // changing first byte of meta checksum
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
				})

				It("should fail when checksum type is invalid", func() {
					b, err := fs.GetXattr(localFQN, core.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 200 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown checksum 200"))

					b, err = fs.GetXattr(localFQN, core.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 0 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown checksum 0"))
				})

				It("should fail when metadata version is invalid", func() {
					b, err := fs.GetXattr(localFQN, core.XattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[0] = 128 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown version 128"))

					b[0] = 0 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown version 0"))
				})

				It("should fail when metadata is too short", func() {
					Expect(fs.SetXattr(localFQN, core.XattrLOM, []byte{1})).NotTo(HaveOccurred())
					err := lom.LoadMetaFromFS()
					Expect(err).To(HaveOccurred())

					Expect(fs.SetXattr(localFQN, core.XattrLOM, []byte{1, 1, 2})).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: too short (3)"))
				})

				It("should fail when meta is corrupted", func() {
					// This test is supposed to end with LoadMetaFromFS error
					// not with nil pointer exception / panic
					b, err := fs.GetXattr(localFQN, core.XattrLOM)
					Expect(err).NotTo(HaveOccurred())
					copy(b[40:], "1321wr")
					Expect(fs.SetXattr(localFQN, core.XattrLOM, b)).NotTo(HaveOccurred())

					err = lom.LoadMetaFromFS()
					Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
				})
			})
		})
	})
})
