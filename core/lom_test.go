// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

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

var _ = Describe("LOM", func() {
	const (
		tmpDir    = "/tmp/lom_test"
		numMpaths = 3

		bucketLocalA = "LOM_TEST_Local_A"
		bucketLocalB = "LOM_TEST_Local_B"
		bucketLocalC = "LOM_TEST_Local_C"

		bucketCloudA = "LOM_TEST_Cloud_A"
		bucketCloudB = "LOM_TEST_Cloud_B"

		sameBucketName = "LOM_TEST_Local_and_Cloud"
	)

	var (
		localBckA = cmn.Bck{Name: bucketLocalA, Provider: apc.AIS, Ns: cmn.NsGlobal}
		localBckB = cmn.Bck{Name: bucketLocalB, Provider: apc.AIS, Ns: cmn.NsGlobal}
		cloudBckA = cmn.Bck{Name: bucketCloudA, Provider: apc.AWS, Ns: cmn.NsGlobal}
	)

	var (
		mpaths []string
		mis    []*fs.Mountpath

		oldCloudProviders = cmn.GCO.Get().Backend.Providers
	)

	for i := range numMpaths {
		mpath := fmt.Sprintf("%s/mpath%d", tmpDir, i)
		mpaths = append(mpaths, mpath)
		mis = append(mis, &fs.Mountpath{Path: mpath})
		_ = cos.CreateDir(mpath)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	for _, mpath := range mpaths {
		_, _ = fs.Add(mpath, "daeID")
	}

	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}, true)

	bmd := mock.NewBaseBownerMock(
		meta.NewBck(
			bucketLocalA, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}, BID: 1},
		),
		meta.NewBck(
			bucketLocalB, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, LRU: cmn.LRUConf{Enabled: true}, BID: 2},
		),
		meta.NewBck(
			bucketLocalC, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{
				Cksum:  cmn.CksumConf{Type: cos.ChecksumOneXxh},
				LRU:    cmn.LRUConf{Enabled: true},
				Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
				BID:    3,
			},
		),
		meta.NewBck(sameBucketName, apc.AIS, cmn.NsGlobal, &cmn.Bprops{BID: 4}),
		meta.NewBck(bucketCloudA, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 5}),
		meta.NewBck(bucketCloudB, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 6}),
		meta.NewBck(sameBucketName, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 7}),
	)

	BeforeEach(func() {
		// Dummy backend provider for tests involving cloud buckets
		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = map[string]cmn.Ns{
			apc.AWS: cmn.NsGlobal,
		}

		cmn.GCO.CommitUpdate(config)

		for _, mpath := range mpaths {
			_, _ = fs.Add(mpath, "daeID")
		}
		_ = mock.NewTarget(bmd)
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)

		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = oldCloudProviders
		cmn.GCO.CommitUpdate(config)
	})

	Describe("FQN Resolution", func() {
		testObject := "foldr/test-obj.ext"
		desiredLocalFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

		When("run for an ais bucket", func() {
			It("Should populate fields from Bucket and ObjName", func() {
				fs.Disable(mpaths[1]) // Ensure that it matches desiredLocalFQN
				fs.Disable(mpaths[2]) // Ensure that it matches desiredLocalFQN

				lom := &core.LOM{ObjName: testObject}
				err := lom.InitBck(&cmn.Bck{Name: bucketLocalA, Provider: apc.AIS})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredLocalFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(apc.AIS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.Mountpath().Path).To(BeEquivalentTo(mpaths[0]))
				expectEqualBck(lom.Bucket(), &localBckA)
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				fs.Enable(mpaths[1])
				fs.Enable(mpaths[2])
			})

			It("Should populate fields from a FQN", func() {
				lom := &core.LOM{}
				err := lom.InitFQN(desiredLocalFQN, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bck().Name).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(apc.AIS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.Mountpath().Path).To(BeEquivalentTo(mpaths[0]))
				expectEqualBck(lom.Bucket(), &localBckA)
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))
			})

			It("Should resolve work files", func() {
				testPid := strconv.FormatInt(9876, 16)
				testTieIndex := strconv.FormatInt(1355314332000000, 16)[5:]
				workObject := "foldr/get.test-obj.ext" + "." + testTieIndex + "." + testPid
				localFQN := mis[0].MakePathFQN(&cloudBckA, fs.WorkfileType, workObject)

				var parsed fs.ParsedFQN
				_, err := core.ResolveFQN(localFQN, &parsed)
				Expect(err).NotTo(HaveOccurred())
				Expect(parsed.ContentType).To(BeEquivalentTo(fs.WorkfileType))
			})
		})

		When("run for a cloud bucket", func() {
			testObject := "foldr/test-obj.ext"
			desiredCloudFQN := mis[0].MakePathFQN(&cloudBckA, fs.ObjectType, testObject)

			It("Should populate fields from Bucket and ObjName", func() {
				// Ensure that it matches desiredCloudFQN
				fs.Disable(mpaths[1])
				fs.Disable(mpaths[2])

				lom := &core.LOM{ObjName: testObject}
				err := lom.InitBck(&cmn.Bck{Name: bucketCloudA, Provider: apc.AWS, Ns: cmn.NsGlobal})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredCloudFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(apc.AWS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.Mountpath().Path).To(Equal(mpaths[0]))
				expectEqualBck(lom.Bucket(), &cloudBckA)
				Expect(lom.ObjName).To(Equal(testObject))

				fs.Enable(mpaths[2])
				fs.Enable(mpaths[1])
			})

			It("Should populate fields from a FQN", func() {
				lom := &core.LOM{}
				err := lom.InitFQN(desiredCloudFQN, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bck().Name).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(apc.AWS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.Mountpath().Path).To(Equal(mpaths[0]))
				expectEqualBck(lom.Bucket(), &cloudBckA)
				Expect(lom.ObjName).To(Equal(testObject))
			})
		})

		When("run for invalid FQN", func() {
			DescribeTable("should return error",
				func(fqn string) {
					lom := &core.LOM{}
					err := lom.InitFQN(fqn, nil)
					Expect(err).To(HaveOccurred())
				},
				Entry(
					"invalid object name",
					mis[0].MakePathFQN(
						&cmn.Bck{Name: bucketCloudA, Provider: apc.AIS, Ns: cmn.NsGlobal},
						fs.ObjectType,
						" ??? ",
					),
				),
				Entry(
					"invalid fqn",
					"?/.,",
				),
				Entry(
					"missing content type",
					mpaths[0],
				),
				Entry(
					"missing bucket type",
					filepath.Join(mpaths[0], fs.ObjectType),
				),
				Entry(
					"missing bucket",
					mis[0].MakePathBck(
						&cmn.Bck{Name: "", Provider: apc.AIS, Ns: cmn.NsGlobal},
					),
				),
				Entry(
					"missing object",
					mis[0].MakePathCT(
						&cmn.Bck{Name: bucketLocalA, Provider: apc.AIS, Ns: cmn.NsGlobal},
						fs.ObjectType,
					),
				),
			)
		})
	})

	Describe("Load", func() {
		Describe("Exists", func() {
			testFileSize := 123
			testObjectName := "fstat-foldr/test-obj.ext"
			localFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObjectName)

			It("should find out that object does not exist", func() {
				os.Remove(localFQN)
				lom := &core.LOM{}
				err := lom.InitFQN(localFQN, nil)
				Expect(err).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(cos.IsNotExist(err)).To(BeTrue())
			})

			It("should find out that object exists", func() {
				createTestFile(localFQN, testFileSize)
				lom := &core.LOM{}
				err := lom.InitFQN(localFQN, nil)
				lom.SetSize(int64(testFileSize))
				Expect(err).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Lsize()).To(BeEquivalentTo(testFileSize))
			})
		})

		Describe("Atime", func() {
			desiredAtime := time.Unix(1500000000, 0)
			testObjectName := "foldr/test-obj.ext"

			It("should fetch atime for bucket with LRU disabled", func() {
				localFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &core.LOM{}
				err := lom.InitFQN(localFQN, nil)
				Expect(err).NotTo(HaveOccurred())
				lom.TestAtime()
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())

				Expect(time.Unix(0, lom.AtimeUnix())).To(BeEquivalentTo(desiredAtime))
			})
			It("should fetch atime for bucket with LRU enabled", func() {
				localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &core.LOM{}
				err := lom.InitFQN(localFQN, nil)
				Expect(err).NotTo(HaveOccurred())
				lom.TestAtime()
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())

				Expect(time.Unix(0, lom.AtimeUnix())).To(BeEquivalentTo(desiredAtime))
			})
		})

		Describe("checksum", func() {
			testFileSize := 456
			testObjectName := "cksum-foldr/test-obj.ext"
			// Bucket needs to have checksum enabled
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObjectName)
			dummyCksm := cos.NewCksum(cos.ChecksumOneXxh, "dummycksm")

			Describe("ComputeCksumIfMissing", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					cksum, err := lom.ComputeSetCksum(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).To(BeNil())
				})

				It("should not compute if not missing", func() {
					lom := filePut(localFQN, testFileSize)
					lom.SetCksum(dummyCksm)
					cksum, err := lom.ComputeSetCksum(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).NotTo(BeEquivalentTo(dummyCksm))
					Expect(cksum.Value()).NotTo(BeEquivalentTo(""))
					_, err = lom.ComputeSetCksum(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Checksum()).To(BeEquivalentTo(cksum))
				})

				It("should compute missing checksum", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					cksum, err := lom.ComputeSetCksum(false)
					Expect(err).NotTo(HaveOccurred())
					cksumType, cksumValue := cksum.Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumOneXxh))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))
					Expect(lom.Checksum().Equal(cksum)).To(BeTrue())

					newLom := NewBasicLom(lom.FQN)
					err = newLom.Load(false, false)
					Expect(err).NotTo(HaveOccurred())
					cksumType, _ = newLom.Checksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))
				})
			})

			Describe("ValidateMetaChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					err := lom.ValidateMetaChecksum()
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Checksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN)
					err := fsLOM.Load(false, false)
					Expect(err).NotTo(HaveOccurred())

					cksumType, _ := fsLOM.Checksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))

					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
					lom.UncacheUnless()

					Expect(lom.Checksum()).ToNot(BeNil())
					_, val := lom.Checksum().Get()
					fsLOM = NewBasicLom(localFQN)
					err = fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())
					_, fsVal := fsLOM.Checksum().Get()
					Expect(fsVal).To(BeEquivalentTo(expectedChecksum))
					Expect(val).To(BeEquivalentTo(expectedChecksum))
				})

				It("should accept when filesystem and memory checksums match", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should accept when both filesystem and memory checksums are nil", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when memory has wrong checksum", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())

					lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "wrong checksum"))
					Expect(persist(lom)).NotTo(HaveOccurred())
					Expect(lom.ValidateContentChecksum(false)).To(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())

					Expect(os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)).To(BeNil())

					Expect(lom.ValidateContentChecksum(false)).To(HaveOccurred())
				})

				It("should not check object content when recompute false", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())

					Expect(os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)).To(BeNil())
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when xattr has wrong checksum", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())

					lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "wrong checksum"))
					Expect(lom.ValidateMetaChecksum()).To(HaveOccurred())
				})

				// This may happen when the checksum was set to `none` previously and someone updated the config.
				// After that, old objects will have old checksum type saved, whereas in the `lom.CksumType()`
				// the new checksum type will be returned.
				It("should correctly validate meta checksum after the checksum type has changed", func() {
					// Using bucket that has checksum type that is *not* `none`.
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					// Set checksum type to `none` to simulate LOM with old checksum type (set to `none`).
					orig := lom.Bck().Props.Cksum.Type
					lom.Bck().Props.Cksum.Type = cos.ChecksumNone
					lom.SetCksum(cos.NewCksum(cos.ChecksumNone, ""))

					Expect(persist(lom)).NotTo(HaveOccurred())
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())

					lom.Bck().Props.Cksum.Type = orig
				})
			})

			// copy-paste of some of ValidateMetaChecksum tests, however if there's no
			// mocking solution, it's needed to have the same tests for both methods
			Describe("ValidateContentChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					err := lom.ValidateContentChecksum(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Checksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN)
					err := fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())

					cksumType, _ := fsLOM.Checksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))

					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
					lom.UncacheUnless()

					Expect(lom.Checksum()).ToNot(BeNil())
					_, cksumValue := lom.Checksum().Get()

					fsLOM = NewBasicLom(localFQN)
					err = fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())

					_, fsCksmVal := fsLOM.Checksum().Get()
					Expect(fsCksmVal).To(BeEquivalentTo(expectedChecksum))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))
				})

				It("should accept when filesystem and memory checksums match", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
				})

				It("should accept when both filesystem and memory checksums are nil", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)

					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())

					err := os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)
					Expect(err).ShouldNot(HaveOccurred())

					Expect(lom.ValidateContentChecksum(false)).To(HaveOccurred())
				})

				It("should correctly validate content checksum after the checksum type has changed", func() {
					// Using bucket that has checksum type that is *not* `none`.
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					// Set checksum type to `none` to simulate LOM with old checksum type (set to `none`).
					lom.SetCksum(cos.NewCksum(cos.ChecksumNone, ""))

					Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
				})
			})

			Describe("FromFS", func() {
				It("should error if file does not exist", func() {
					testObject := "foldr/test-obj-doesnt-exist.ext"
					noneFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)
					lom := NewBasicLom(noneFQN)

					Expect(lom.FromFS()).To(HaveOccurred())
				})

				It("should fill object with correct meta", func() {
					startTime := time.Now()
					time.Sleep(50 * time.Millisecond)
					lom1 := filePut(localFQN, testFileSize)
					lom2 := NewBasicLom(localFQN)
					Expect(lom1.Persist()).NotTo(HaveOccurred())

					Expect(lom1.ValidateContentChecksum(false)).NotTo(HaveOccurred())
					Expect(lom1.Persist()).ToNot(HaveOccurred())

					Expect(lom2.Load(false, false)).ToNot(HaveOccurred()) // Calls `FromFS`.
					Expect(lom2.Checksum()).To(BeEquivalentTo(lom1.Checksum()))
					Expect(lom2.Version()).To(BeEquivalentTo(lom1.Version()))
					Expect(lom2.Lsize()).To(BeEquivalentTo(testFileSize))
					Expect(time.Unix(0, lom2.AtimeUnix()).After(startTime)).To(BeTrue())
				})
			})
		})

		Describe("Version", func() {
			testObject := "foldr/test-obj.ext"
			desiredVersion := "9001"
			localFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

			It("should be able to get version", func() {
				lom := filePut(localFQN, 0)
				lom.SetVersion(desiredVersion)
				Expect(persist(lom)).NotTo(HaveOccurred())

				err := lom.Load(false, false)
				Expect(err).ToNot(HaveOccurred())
				Expect(lom.Version()).To(BeEquivalentTo(desiredVersion))
			})
		})

		Describe("CustomMD", func() {
			testObject := "foldr/test-obj.ext"
			localFQN := mis[0].MakePathFQN(&localBckA, fs.ObjectType, testObject)

			It("should correctly set and get custom metadata", func() {
				lom := filePut(localFQN, 0)
				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				value, exists := lom.GetCustomKey(cmn.SourceObjMD)
				Expect(exists).To(BeTrue())
				Expect(value).To(Equal(apc.GCP))
				_, exists = lom.GetCustomKey("unknown")
				Expect(exists).To(BeFalse())
			})
		})
	})

	Describe("copy object methods", func() {
		const (
			testObjectName = "foldr/test-obj.ext"
			testFileSize   = 101
			desiredVersion = "9002"
		)

		findMpath := func(objectName, bucket string, defaultLoc bool, ignoreFQNs ...string) string {
		OuterLoop:
			for _, mi := range mis {
				bck := cmn.Bck{Name: bucket, Provider: apc.AIS, Ns: cmn.NsGlobal}
				fqn := mi.MakePathFQN(&bck, fs.ObjectType, objectName)
				for _, ignoreFQN := range ignoreFQNs {
					if fqn == ignoreFQN {
						continue OuterLoop
					}
				}

				var parsed fs.ParsedFQN
				hrw, _ := core.ResolveFQN(fqn, &parsed)
				if defaultLoc && hrw == fqn {
					return fqn
				} else if !defaultLoc && hrw != fqn {
					return fqn
				}
			}
			cos.Assert(false)
			return ""
		}

		mirrorFQNs := []string{
			// Bucket with redundancy
			findMpath(testObjectName, bucketLocalC, true /*defaultLoc*/),
			findMpath(testObjectName, bucketLocalC, false /*defaultLoc*/),
		}
		// Add another mirrorFQN but it must be different than the second one.
		mirrorFQNs = append(
			mirrorFQNs,
			findMpath(testObjectName, bucketLocalC, false /*defaultLoc*/, mirrorFQNs[1]),
		)
		// Object with different name as default one.
		renamedObjFQN := findMpath("other.txt", bucketLocalC, true /*defaultLoc*/)

		copyFQNs := []string{
			// Bucket with no redundancy
			findMpath(testObjectName, bucketLocalB, true /*defaultLoc*/),
			findMpath(testObjectName, bucketLocalB, false /*defaultLoc*/),
		}

		prepareLOM := func(fqn string) (lom *core.LOM) {
			// Prepares a basic lom with a copy
			createTestFile(fqn, testFileSize)
			lom = &core.LOM{}
			err := lom.InitFQN(fqn, nil)

			lom.SetSize(int64(testFileSize))
			lom.SetVersion(desiredVersion)
			Expect(persist(lom)).NotTo(HaveOccurred())
			lom.UncacheUnless()
			Expect(err).NotTo(HaveOccurred())
			err = lom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum(false)).NotTo(HaveOccurred())
			return
		}

		prepareCopy := func(lom *core.LOM, fqn string, locked ...bool) (dst *core.LOM) {
			var (
				err error
				bck = lom.Bck()
			)
			if len(locked) == 0 {
				lom.Lock(true)
				defer lom.Unlock(true)
			}
			dst, err = lom.Copy2FQN(fqn, make([]byte, testFileSize))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dst.FQN).To(BeARegularFile())
			Expect(dst.Lsize(true)).To(BeEquivalentTo(testFileSize))

			hrwLom := &core.LOM{ObjName: lom.ObjName}
			Expect(hrwLom.InitBck(bck.Bucket())).NotTo(HaveOccurred())
			hrwLom.UncacheUnless()

			// Reload copy, to make sure it is fresh
			dst = NewBasicLom(dst.FQN)
			Expect(dst.Load(false, true)).NotTo(HaveOccurred())
			lck := dst.IsLocked() > apc.LockNone
			Expect(dst.ValidateContentChecksum(lck)).NotTo(HaveOccurred())
			hrwLom.UncacheUnless()
			return
		}

		checkCopies := func(defaultLOM *core.LOM, copiesFQNs ...string) {
			expectedHash := getTestFileHash(defaultLOM.FQN)

			for _, copyFQN := range copiesFQNs {
				copyLOM := NewBasicLom(copyFQN)
				Expect(copyLOM.Load(false, true)).NotTo(HaveOccurred())

				_, cksumValue := copyLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))

				Expect(copyLOM.IsCopy()).To(Equal(copyFQN != defaultLOM.FQN))
				Expect(copyLOM.HasCopies()).To(BeTrue())
				Expect(copyLOM.NumCopies()).To(Equal(len(copiesFQNs)))
				for _, cfqn := range copiesFQNs {
					Expect(copyLOM.GetCopies()).To(HaveKey(cfqn))
				}

				// Check that the content of the copy is correct.
				copyObjHash := getTestFileHash(copyFQN)
				Expect(copyObjHash).To(BeEquivalentTo(expectedHash))
			}
		}

		Describe("CopyObject", func() {
			It("should successfully copy the object", func() {
				lom := prepareLOM(copyFQNs[0])
				copyLOM := prepareCopy(lom, copyFQNs[1])
				expectedHash := getTestFileHash(lom.FQN)

				lom.Lock(false)
				defer lom.Unlock(false)
				// Check that no copies were added to metadata
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeFalse())
				Expect(lom.NumCopies()).To(Equal(1))
				Expect(lom.GetCopies()).To(BeNil())

				// Check copy created
				Expect(copyLOM.FQN).NotTo(Equal(lom.FQN))
				_, cksumValue := copyLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))
				Expect(copyLOM.IsCopy()).To(BeFalse())
				Expect(copyLOM.HasCopies()).To(BeFalse())
				Expect(copyLOM.NumCopies()).To(Equal(1))
				Expect(copyLOM.GetCopies()).To(BeNil())

				// Check copy contents are correct
				copyObjHash := getTestFileHash(copyFQNs[1])
				Expect(copyObjHash).To(BeEquivalentTo(expectedHash))
			})

			It("should successfully copy the object in case it is mirror copy", func() {
				lom := prepareLOM(mirrorFQNs[0])
				copyLOM := prepareCopy(lom, mirrorFQNs[1])
				expectedHash := getTestFileHash(lom.FQN)

				// Check that copies were added to metadata
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Check copy created
				Expect(copyLOM.FQN).NotTo(Equal(lom.FQN))
				_, cksumValue := copyLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))

				Expect(copyLOM.IsCopy()).To(BeTrue())
				Expect(copyLOM.HasCopies()).To(BeTrue())
				Expect(copyLOM.NumCopies()).To(Equal(lom.NumCopies()))
				Expect(copyLOM.GetCopies()).To(Equal(lom.GetCopies()))

				// Check that the content of the copy is correct.
				copyObjHash := getTestFileHash(mirrorFQNs[1])
				Expect(copyObjHash).To(BeEquivalentTo(expectedHash))
			})

			It("should successfully copy the object and update metadata for other copies", func() {
				lom := prepareLOM(mirrorFQNs[0])
				lom.Lock(true)
				defer lom.Unlock(true)
				_ = prepareCopy(lom, mirrorFQNs[1], true)
				_ = prepareCopy(lom, mirrorFQNs[2], true)

				// Check that copies were added to metadata.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(3))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1]), HaveKey(mirrorFQNs[2])))

				// Check metadata of created copies (it also checks default object).
				checkCopies(lom, mirrorFQNs[0], mirrorFQNs[1], mirrorFQNs[2])
			})

			It("should check for missing copies during `syncMetaWithCopies`", func() {
				lom := prepareLOM(mirrorFQNs[0])
				lom.Lock(true)
				defer lom.Unlock(true)
				_ = prepareCopy(lom, mirrorFQNs[1], true)

				// Check that copies were added to metadata.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Make one copy disappear.
				cos.RemoveFile(mirrorFQNs[1])

				// Prepare another one (to trigger `syncMetaWithCopies`).
				_ = prepareCopy(lom, mirrorFQNs[2], true)

				// Check metadata of left copies (it also checks default object).
				checkCopies(lom, mirrorFQNs[0], mirrorFQNs[2])
			})

			It("should copy object without adding it to copies if dst bucket does not support mirroring", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])
				nonMirroredLOM := prepareCopy(lom, copyFQNs[0])
				expectedHash := getTestFileHash(lom.FQN)

				// Check that copies were added to metadata.
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Check that nothing has changed in the src.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Check destination lom.
				nonMirroredLOM.Lock(false)
				defer nonMirroredLOM.Unlock(false)
				Expect(nonMirroredLOM.FQN).NotTo(Equal(lom.FQN))
				_, cksumValue := nonMirroredLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(nonMirroredLOM.Version()).To(Equal("1"))
				Expect(nonMirroredLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))

				Expect(nonMirroredLOM.IsCopy()).To(BeFalse())
				Expect(nonMirroredLOM.HasCopies()).To(BeFalse())
				Expect(nonMirroredLOM.NumCopies()).To(Equal(1))
				Expect(nonMirroredLOM.GetCopies()).To(BeNil())

				// Check that the content of the copy is correct.
				copyObjHash := getTestFileHash(nonMirroredLOM.FQN)
				Expect(copyObjHash).To(BeEquivalentTo(expectedHash))
			})

			// This test case can happen when we rename object to some different name.
			It("should not count object as mirror/copy if new object has different name", func() {
				lom := prepareLOM(mirrorFQNs[0])
				copyLOM := prepareCopy(lom, renamedObjFQN)
				expectedHash := getTestFileHash(lom.FQN)

				// Check that no copies were added to metadata.
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.IsHRW()).To(BeTrue())
				Expect(lom.HasCopies()).To(BeFalse())
				Expect(lom.NumCopies()).To(Equal(1))
				Expect(lom.GetCopies()).To(BeNil())

				// Check copy created.
				copyLOM.Lock(false)
				defer copyLOM.Unlock(false)
				Expect(copyLOM.FQN).NotTo(Equal(lom.FQN))
				_, cksumValue := copyLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion)) // TODO: ???
				Expect(copyLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))
				Expect(copyLOM.IsHRW()).To(BeTrue())
				Expect(copyLOM.IsCopy()).To(BeFalse())
				Expect(copyLOM.HasCopies()).To(BeFalse())
				Expect(copyLOM.NumCopies()).To(Equal(1))
				Expect(copyLOM.GetCopies()).To(BeNil())
			})

			// This test case can happen when user adds new mountpath and all existing
			// copies become non-HRW and now we need to create a HRW object.
			It("should copy object if mirroring the non-HRW object to HRW object", func() {
				copyLOM := prepareLOM(mirrorFQNs[1])
				// Recreate main/HRW object from the copy.
				lom := prepareCopy(copyLOM, mirrorFQNs[0])

				// Check that HRW object was created correctly.
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))
				Expect(lom.Version()).To(Equal(desiredVersion))
				Expect(lom.Lsize(true)).To(BeEquivalentTo(testFileSize))

				// Check that copy from which HRW object was created is also updated.
				Expect(copyLOM.FQN).NotTo(Equal(lom.FQN))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Lsize(true)).To(BeEquivalentTo(testFileSize))
				Expect(copyLOM.IsCopy()).To(BeTrue())
				Expect(copyLOM.HasCopies()).To(BeTrue())
				Expect(copyLOM.NumCopies()).To(Equal(lom.NumCopies()))
				Expect(copyLOM.GetCopies()).To(Equal(lom.GetCopies()))
			})
		})

		Describe("DelCopies", func() {
			It("should delete mirrored copy", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])

				// Check that no copies were added to metadata.
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Delete copy and check if it's gone.
				Expect(lom.DelCopies(mirrorFQNs[1])).ToNot(HaveOccurred())
				Expect(persist(lom)).ToNot(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())

				// Reload default object and check if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0])
				Expect(lom.Load(false, true)).ToNot(HaveOccurred())
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeFalse())
				Expect(lom.NumCopies()).To(Equal(1))
				Expect(lom.GetCopies()).To(BeNil())
			})

			It("should delete mirrored copy and update other copies", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])
				_ = prepareCopy(lom, mirrorFQNs[2])
				expectedHash := getTestFileHash(lom.FQN)

				// Check that copies were added to metadata.
				lom.Lock(true)
				defer lom.Unlock(true)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(3))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1]), HaveKey(mirrorFQNs[2])))

				// Delete copy and check if it's gone.
				Expect(lom.DelCopies(mirrorFQNs[1])).ToNot(HaveOccurred())
				Expect(persist(lom)).ToNot(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())

				// Reload default object and check if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0])
				Expect(lom.Load(false, true)).ToNot(HaveOccurred())
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[2])))

				// Check that left copy was correctly updated.
				copyLOM := NewBasicLom(mirrorFQNs[2])
				Expect(copyLOM.Load(false, true)).NotTo(HaveOccurred())
				_, cksumValue := copyLOM.Checksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Lsize()).To(BeEquivalentTo(testFileSize))

				Expect(copyLOM.IsCopy()).To(BeTrue())
				Expect(copyLOM.HasCopies()).To(BeTrue())
				Expect(copyLOM.NumCopies()).To(Equal(lom.NumCopies()))
				Expect(copyLOM.GetCopies()).To(Equal(lom.GetCopies()))
			})
		})

		Describe("DelAllCopies", func() {
			It("should be able to delete all copies", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])
				_ = prepareCopy(lom, mirrorFQNs[2])

				// Sanity check for default object.
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(3))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1]), HaveKey(mirrorFQNs[2])))

				// Delete all copies and check if they are gone.
				Expect(lom.DelAllCopies()).NotTo(HaveOccurred())
				Expect(persist(lom)).ToNot(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())
				Expect(mirrorFQNs[2]).NotTo(BeAnExistingFile())

				// Reload default object and see if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0])
				Expect(lom.Load(false, true)).ToNot(HaveOccurred())
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeFalse())
				Expect(lom.NumCopies()).To(Equal(1))
				Expect(lom.GetCopies()).To(BeNil())
			})
		})
	})

	Describe("local and cloud bucket with the same name", func() {
		It("should have different fqn", func() {
			testObject := "foldr/test-obj.ext"
			localSameBck := cmn.Bck{Name: sameBucketName, Provider: apc.AIS, Ns: cmn.NsGlobal}
			cloudSameBck := cmn.Bck{Name: sameBucketName, Provider: apc.AWS, Ns: cmn.NsGlobal}
			desiredLocalFQN := mis[0].MakePathFQN(&localSameBck, fs.ObjectType, testObject)
			desiredCloudFQN := mis[0].MakePathFQN(&cloudSameBck, fs.ObjectType, testObject)

			fs.Disable(mpaths[1]) // Ensure that it matches desiredCloudFQN
			fs.Disable(mpaths[2]) // ditto

			lomLocal := &core.LOM{ObjName: testObject}
			err := lomLocal.InitBck(&cmn.Bck{Name: sameBucketName, Provider: apc.AIS})
			Expect(err).NotTo(HaveOccurred())
			err = lomLocal.Load(false, false)
			Expect(cos.IsNotExist(err)).To(BeTrue())
			Expect(lomLocal.FQN).To(Equal(desiredLocalFQN))
			uname := lomLocal.Bck().MakeUname(testObject)
			Expect(lomLocal.Uname()).To(Equal(cos.UnsafeS(uname)))
			Expect(lomLocal.Bck().Provider).To(Equal(apc.AIS))
			Expect(lomLocal.Mountpath().Path).To(Equal(mpaths[0]))
			expectEqualBck(lomLocal.Bucket(), &localSameBck)
			Expect(lomLocal.ObjName).To(Equal(testObject))

			lomCloud := &core.LOM{ObjName: testObject}
			err = lomCloud.InitBck(&cmn.Bck{Name: sameBucketName, Provider: apc.AWS})
			Expect(err).NotTo(HaveOccurred())
			err = lomCloud.Load(false, false)
			Expect(cos.IsNotExist(err)).To(BeTrue())
			Expect(lomCloud.FQN).To(Equal(desiredCloudFQN))

			uname = lomCloud.Bck().MakeUname(testObject)
			Expect(lomCloud.Uname()).To(Equal(cos.UnsafeS(uname)))
			Expect(lomCloud.Bck().Provider).To(Equal(apc.AWS))
			Expect(lomCloud.Mountpath().Path).To(Equal(mpaths[0]))
			expectEqualBck(lomCloud.Bucket(), &cloudSameBck)
			Expect(lomCloud.ObjName).To(Equal(testObject))

			fs.Enable(mpaths[1])
			fs.Enable(mpaths[2])
		})
	})
})

//
// HELPERS
//

// needs to be called inside of gomega scope like Describe/It
func NewBasicLom(fqn string) *core.LOM {
	lom := &core.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())
	return lom
}

func filePut(fqn string, size int) *core.LOM {
	createTestFile(fqn, size)
	lom := NewBasicLom(fqn)
	lom.SetSize(int64(size))
	lom.IncVersion()
	Expect(persist(lom)).NotTo(HaveOccurred())
	lom.UncacheUnless()
	return lom
}

func createTestFile(fqn string, size int) {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		buff := make([]byte, size)
		_, _ = cryptorand.Read(buff)
		_, err := testFile.Write(buff)
		_ = testFile.Close()

		Expect(err).ShouldNot(HaveOccurred())
	}
}

func getTestFileHash(fqn string) (hash string) {
	reader, _ := os.Open(fqn)
	_, cksum, err := cos.CopyAndChecksum(io.Discard, reader, nil, cos.ChecksumOneXxh)
	Expect(err).NotTo(HaveOccurred())
	hash = cksum.Value()
	reader.Close()
	return
}

func expectEqualBck(left, right *cmn.Bck) {
	p := right.Props
	right.Props = left.Props
	_ = Expect(left).To(Equal(right))
	right.Props = p
}

func persist(lom *core.LOM) error {
	if lom.AtimeUnix() == 0 {
		lom.SetAtimeUnix(time.Now().UnixNano())
	}
	return lom.Persist()
}
