// Package cluster_test provides tests for cluster package
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
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
		localBckA = cmn.Bck{Name: bucketLocalA, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		localBckB = cmn.Bck{Name: bucketLocalB, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
		cloudBckA = cmn.Bck{Name: bucketCloudA, Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}
	)

	var (
		mpaths []string
		mis    []*fs.MountpathInfo

		oldCloudProviders = cmn.GCO.Get().Backend.Providers
	)

	for i := 0; i < numMpaths; i++ {
		mpath := fmt.Sprintf("%s/mpath%d", tmpDir, i)
		mpaths = append(mpaths, mpath)
		mis = append(mis, &fs.MountpathInfo{Path: mpath})
		_ = cos.CreateDir(mpath)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.Init()
	fs.DisableFsIDCheck()
	for _, mpath := range mpaths {
		_, _ = fs.Add(mpath, "daeID")
	}

	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	var (
		bmd = cluster.NewBaseBownerMock(
			cluster.NewBck(
				bucketLocalA, cmn.ProviderAIS, cmn.NsGlobal,
				&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}, BID: 1},
			),
			cluster.NewBck(
				bucketLocalB, cmn.ProviderAIS, cmn.NsGlobal,
				&cmn.BucketProps{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}, LRU: cmn.LRUConf{Enabled: true}, BID: 2},
			),
			cluster.NewBck(
				bucketLocalC, cmn.ProviderAIS, cmn.NsGlobal,
				&cmn.BucketProps{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumXXHash},
					LRU:    cmn.LRUConf{Enabled: true},
					Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
					BID:    3,
				},
			),
			cluster.NewBck(sameBucketName, cmn.ProviderAIS, cmn.NsGlobal, &cmn.BucketProps{BID: 4}),
			cluster.NewBck(bucketCloudA, cmn.ProviderAmazon, cmn.NsGlobal, &cmn.BucketProps{BID: 5}),
			cluster.NewBck(bucketCloudB, cmn.ProviderAmazon, cmn.NsGlobal, &cmn.BucketProps{BID: 6}),
			cluster.NewBck(sameBucketName, cmn.ProviderAmazon, cmn.NsGlobal, &cmn.BucketProps{BID: 7}),
		)
		tMock cluster.Target
	)

	BeforeEach(func() {
		// Dummy backend provider for tests involving cloud buckets
		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = map[string]cmn.Ns{
			cmn.ProviderAmazon: cmn.NsGlobal,
		}

		cmn.GCO.CommitUpdate(config)

		for _, mpath := range mpaths {
			_, _ = fs.Add(mpath, "daeID")
		}
		tMock = cluster.NewTargetMock(bmd)
		cluster.Init(tMock)
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)

		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = oldCloudProviders
		cmn.GCO.CommitUpdate(config)
	})

	Describe("FQN Resolution", func() {
		testObject := "foldr/test-obj.ext"
		desiredLocalFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

		When("run for an ais bucket", func() {
			It("Should populate fields from Bucket and ObjName", func() {
				fs.Disable(mpaths[1]) // Ensure that it matches desiredLocalFQN
				fs.Disable(mpaths[2]) // Ensure that it matches desiredLocalFQN

				lom := &cluster.LOM{ObjName: testObject}
				err := lom.Init(cmn.Bck{Name: bucketLocalA, Provider: cmn.ProviderAIS})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredLocalFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(cmn.ProviderAIS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.MpathInfo().Path).To(BeEquivalentTo(mpaths[0]))
				expectEqualBck(lom.Bucket(), localBckA)
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				fs.Enable(mpaths[1])
				fs.Enable(mpaths[2])
			})

			It("Should populate fields from a FQN", func() {
				lom := &cluster.LOM{FQN: desiredLocalFQN}
				err := lom.Init(cmn.Bck{})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bck().Name).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(cmn.ProviderAIS))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.MpathInfo().Path).To(BeEquivalentTo(mpaths[0]))
				expectEqualBck(lom.Bucket(), localBckA)
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))
			})

			It("Should resolve work files", func() {
				testPid := strconv.FormatInt(9876, 16)
				testTieIndex := strconv.FormatInt(1355314332000000, 16)[5:]
				workObject := "foldr/get.test-obj.ext" + "." + testTieIndex + "." + testPid
				localFQN := mis[0].MakePathFQN(cloudBckA, fs.WorkfileType, workObject)

				parsedFQN, _, err := cluster.ResolveFQN(localFQN)
				Expect(err).NotTo(HaveOccurred())
				Expect(parsedFQN.ContentType).To(BeEquivalentTo(fs.WorkfileType))
			})
		})

		When("run for a cloud bucket", func() {
			testObject := "foldr/test-obj.ext"
			desiredCloudFQN := mis[0].MakePathFQN(cloudBckA, fs.ObjectType, testObject)

			It("Should populate fields from Bucket and ObjName", func() {
				// Ensure that it matches desiredCloudFQN
				fs.Disable(mpaths[1])
				fs.Disable(mpaths[2])

				lom := &cluster.LOM{ObjName: testObject}
				err := lom.Init(cmn.Bck{Name: bucketCloudA, Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredCloudFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(cmn.ProviderAmazon))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.MpathInfo().Path).To(Equal(mpaths[0]))
				expectEqualBck(lom.Bucket(), cloudBckA)
				Expect(lom.ObjName).To(Equal(testObject))

				fs.Enable(mpaths[2])
				fs.Enable(mpaths[1])
			})

			It("Should populate fields from a FQN", func() {
				lom := &cluster.LOM{FQN: desiredCloudFQN}
				err := lom.Init(cmn.Bck{})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bck().Name).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ObjName).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(lom.Bck().MakeUname(testObject)))
				Expect(lom.Bck().Provider).To(Equal(cmn.ProviderAmazon))

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.MpathInfo().Path).To(Equal(mpaths[0]))
				expectEqualBck(lom.Bucket(), cloudBckA)
				Expect(lom.ObjName).To(Equal(testObject))
			})
		})

		When("run for invalid FQN", func() {
			DescribeTable("should return error",
				func(fqn string) {
					lom := &cluster.LOM{FQN: fqn}
					err := lom.Init(cmn.Bck{})
					Expect(err).To(HaveOccurred())
				},
				Entry(
					"invalid object name",
					mis[0].MakePathFQN(
						cmn.Bck{Name: bucketCloudA, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
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
					filepath.Join(mpaths[0]),
				),
				Entry(
					"missing bucket type",
					filepath.Join(mpaths[0], fs.ObjectType),
				),
				Entry(
					"missing bucket",
					mis[0].MakePathBck(
						cmn.Bck{Name: "", Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
					),
				),
				Entry(
					"missing object",
					mis[0].MakePathCT(
						cmn.Bck{Name: bucketLocalA, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal},
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
			localFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObjectName)

			It("should find out that object does not exist", func() {
				os.Remove(localFQN)
				lom := &cluster.LOM{FQN: localFQN}
				err := lom.Init(cmn.Bck{})
				Expect(err).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(cmn.IsObjNotExist(err)).To(BeTrue())
			})

			It("should find out that object exists", func() {
				createTestFile(localFQN, testFileSize)
				lom := &cluster.LOM{FQN: localFQN}
				err := lom.Init(cmn.Bck{})
				lom.SetSize(int64(testFileSize))
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Size()).To(BeEquivalentTo(testFileSize))
			})
		})

		Describe("Atime", func() {
			desiredAtime := time.Unix(1500000000, 0)
			testObjectName := "foldr/test-obj.ext"

			It("should fetch atime for bucket with LRU disabled", func() {
				localFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{FQN: localFQN}
				err := lom.Init(cmn.Bck{})
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())

				Expect(time.Unix(0, lom.AtimeUnix())).To(BeEquivalentTo(desiredAtime))
			})
			It("should fetch atime for bucket with LRU enabled", func() {
				localFQN := mis[0].MakePathFQN(localBckB, fs.ObjectType, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{FQN: localFQN}
				err := lom.Init(cmn.Bck{})
				Expect(err).NotTo(HaveOccurred())
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
			localFQN := mis[0].MakePathFQN(localBckB, fs.ObjectType, testObjectName)
			dummyCksm := cos.NewCksum(cos.ChecksumXXHash, "dummycksm")

			Describe("ComputeCksumIfMissing", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					cksum, err := lom.ComputeCksumIfMissing()
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).To(BeNil())
				})

				It("should not compute if not missing", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(localBckB, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					lom.SetCksum(dummyCksm)
					cksum, err := lom.ComputeCksumIfMissing()
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).To(BeEquivalentTo(dummyCksm))
				})

				It("should compute missing checksum", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					cksum, err := lom.ComputeCksumIfMissing()
					Expect(err).NotTo(HaveOccurred())
					cksumType, cksumValue := cksum.Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumXXHash))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))
					Expect(lom.Cksum().Equal(cksum)).To(BeTrue())

					newLom := NewBasicLom(lom.FQN)
					err = newLom.Load(false, false)
					Expect(err).NotTo(HaveOccurred())
					cksumType, _ = newLom.Cksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))
				})
			})

			Describe("ValidateMetaChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					err := lom.ValidateMetaChecksum()
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Cksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN)
					err := fsLOM.Load(false, false)
					Expect(err).NotTo(HaveOccurred())

					cksumType, _ := fsLOM.Cksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
					lom.Uncache(false)

					Expect(lom.Cksum()).ToNot(BeNil())
					_, val := lom.Cksum().Get()
					fsLOM = NewBasicLom(localFQN)
					err = fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())
					_, fsVal := fsLOM.Cksum().Get()
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

					lom.SetCksum(cos.NewCksum(cos.ChecksumXXHash, "wrong checksum"))
					lom.Persist()
					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					Expect(os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)).To(BeNil())

					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})

				It("should not check object content when recompute false", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					Expect(os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)).To(BeNil())
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when xattr has wrong checksum", func() {
					lom := filePut(localFQN, testFileSize)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					lom.SetCksum(cos.NewCksum(cos.ChecksumXXHash, "wrong checksum"))
					Expect(lom.ValidateMetaChecksum()).To(HaveOccurred())
				})
			})

			// copy-paste of some of ValidateMetaChecksum tests, however if there's no
			// mocking solution, it's needed to have the same tests for both methods
			Describe("ValidateContentChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

					lom := NewBasicLom(noneFQN)
					err := lom.ValidateContentChecksum()
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Cksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN)
					err := fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())

					cksumType, _ := fsLOM.Cksum().Get()
					Expect(cksumType).To(BeEquivalentTo(cos.ChecksumNone))

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
					lom.Uncache(false)

					Expect(lom.Cksum()).ToNot(BeNil())
					_, cksumValue := lom.Cksum().Get()

					fsLOM = NewBasicLom(localFQN)
					err = fsLOM.Load(false, false)
					Expect(err).ShouldNot(HaveOccurred())

					_, fsCksmVal := fsLOM.Cksum().Get()
					Expect(fsCksmVal).To(BeEquivalentTo(expectedChecksum))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))
				})

				It("should accept when filesystem and memory checksums match", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
				})

				It("should accept when both filesystem and memory checksums are nil", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					err := os.WriteFile(localFQN, []byte("wrong file"), cos.PermRWR)
					Expect(err).ShouldNot(HaveOccurred())

					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})
			})

			Describe("FromFS", func() {
				It("should error if file does not exist", func() {
					testObject := "foldr/test-obj-doesnt-exist.ext"
					noneFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)
					lom := NewBasicLom(noneFQN)

					Expect(lom.FromFS()).To(HaveOccurred())
				})

				It("should fill object with correct meta", func() {
					startTime := time.Now()
					time.Sleep(50 * time.Millisecond)
					lom1 := filePut(localFQN, testFileSize)
					lom2 := NewBasicLom(localFQN)
					Expect(lom1.Persist()).NotTo(HaveOccurred())

					Expect(lom1.ValidateContentChecksum()).NotTo(HaveOccurred())
					Expect(lom1.Persist()).ToNot(HaveOccurred())

					Expect(lom2.Load(false, false)).ToNot(HaveOccurred()) // Calls `FromFS`.
					Expect(lom2.Cksum()).To(BeEquivalentTo(lom1.Cksum()))
					Expect(lom2.Version()).To(BeEquivalentTo(lom1.Version()))
					Expect(lom2.Size()).To(BeEquivalentTo(testFileSize))
					Expect(time.Unix(0, lom2.AtimeUnix()).After(startTime)).To(BeTrue())
				})
			})
		})

		Describe("Version", func() {
			testObject := "foldr/test-obj.ext"
			desiredVersion := "9001"
			localFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

			It("should be able to get version", func() {
				lom := filePut(localFQN, 0)
				lom.SetVersion(desiredVersion)
				Expect(lom.Persist()).NotTo(HaveOccurred())

				err := lom.Load(false, false)
				Expect(err).ToNot(HaveOccurred())
				Expect(lom.Version()).To(BeEquivalentTo(desiredVersion))
			})
		})

		Describe("CustomMD", func() {
			testObject := "foldr/test-obj.ext"
			localFQN := mis[0].MakePathFQN(localBckA, fs.ObjectType, testObject)

			It("should correctly set and get custom metadata", func() {
				lom := filePut(localFQN, 0)
				lom.SetCustomMD(cos.SimpleKVs{
					cluster.SourceObjMD:  cluster.SourceGoogleObjMD,
					cluster.VersionObjMD: "version",
					cluster.CRC32CObjMD:  "crc32",
				})
				value, exists := lom.GetCustomMD(cluster.SourceObjMD)
				Expect(exists).To(BeTrue())
				Expect(value).To(Equal(cluster.SourceGoogleObjMD))
				_, exists = lom.GetCustomMD("unknown")
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
				bck := cmn.Bck{Name: bucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
				fqn := mi.MakePathFQN(bck, fs.ObjectType, objectName)
				for _, ignoreFQN := range ignoreFQNs {
					if fqn == ignoreFQN {
						continue OuterLoop
					}
				}

				_, hrw, _ := cluster.ResolveFQN(fqn)
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

		prepareLOM := func(fqn string) (lom *cluster.LOM) {
			// Prepares a basic lom with a copy
			createTestFile(fqn, testFileSize)
			lom = &cluster.LOM{FQN: fqn}

			err := lom.Init(cmn.Bck{})

			lom.SetSize(int64(testFileSize))
			lom.SetVersion(desiredVersion)
			Expect(lom.Persist()).NotTo(HaveOccurred())
			lom.Uncache(false)
			Expect(err).NotTo(HaveOccurred())
			err = lom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
			return
		}

		prepareCopy := func(lom *cluster.LOM, fqn string, locked ...bool) (dst *cluster.LOM) {
			var (
				err error
				bck = lom.Bck()
			)
			if len(locked) == 0 {
				lom.Lock(true)
				defer lom.Unlock(true)
			}
			dst, err = lom.CopyObject(fqn, make([]byte, testFileSize))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dst.FQN).To(BeARegularFile())
			Expect(dst.Size()).To(BeEquivalentTo(testFileSize))

			hrwLom := &cluster.LOM{ObjName: lom.ObjName}
			Expect(hrwLom.Init(bck.Bucket())).NotTo(HaveOccurred())
			hrwLom.Uncache(false)

			// Reload copy, to make sure it is fresh
			dst = NewBasicLom(dst.FQN)
			Expect(dst.Load(false, true)).NotTo(HaveOccurred())
			Expect(dst.ValidateContentChecksum()).NotTo(HaveOccurred())
			hrwLom.Uncache(false)
			return
		}

		checkCopies := func(defaultLOM *cluster.LOM, copiesFQNs ...string) {
			expectedHash := getTestFileHash(defaultLOM.FQN)

			for _, copyFQN := range copiesFQNs {
				copyLOM := NewBasicLom(copyFQN)
				Expect(copyLOM.Load(false, true)).NotTo(HaveOccurred())

				_, cksumValue := copyLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))

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
				_, cksumValue := copyLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))
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
				_, cksumValue := copyLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))

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
				_, cksumValue := nonMirroredLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(nonMirroredLOM.Version()).To(Equal("1"))
				Expect(nonMirroredLOM.Size()).To(BeEquivalentTo(testFileSize))

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
				_, cksumValue := copyLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion)) // TODO: ???
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))
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
				Expect(lom.Size()).To(BeEquivalentTo(testFileSize))

				// Check that copy from which HRW object was created is also updated.
				Expect(copyLOM.FQN).NotTo(Equal(lom.FQN))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))
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
				Expect(lom.Persist()).ToNot(HaveOccurred())
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
				Expect(lom.Persist()).ToNot(HaveOccurred())
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
				_, cksumValue := copyLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(copyLOM.Version()).To(Equal(desiredVersion))
				Expect(copyLOM.Size()).To(BeEquivalentTo(testFileSize))

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
				Expect(lom.Persist()).ToNot(HaveOccurred())
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
			localSameBck := cmn.Bck{Name: sameBucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
			cloudSameBck := cmn.Bck{Name: sameBucketName, Provider: cmn.ProviderAmazon, Ns: cmn.NsGlobal}
			desiredLocalFQN := mis[0].MakePathFQN(localSameBck, fs.ObjectType, testObject)
			desiredCloudFQN := mis[0].MakePathFQN(cloudSameBck, fs.ObjectType, testObject)

			fs.Disable(mpaths[1]) // Ensure that it matches desiredCloudFQN
			fs.Disable(mpaths[2]) // ditto

			lomLocal := &cluster.LOM{ObjName: testObject}
			err := lomLocal.Init(cmn.Bck{Name: sameBucketName, Provider: cmn.ProviderAIS})
			Expect(err).NotTo(HaveOccurred())
			err = lomLocal.Load(false, false)
			Expect(cmn.IsObjNotExist(err)).To(BeTrue())
			Expect(lomLocal.FQN).To(Equal(desiredLocalFQN))
			Expect(lomLocal.Uname()).To(Equal(lomLocal.Bck().MakeUname(testObject)))
			Expect(lomLocal.Bck().Provider).To(Equal(cmn.ProviderAIS))
			Expect(lomLocal.MpathInfo().Path).To(Equal(mpaths[0]))
			expectEqualBck(lomLocal.Bucket(), localSameBck)
			Expect(lomLocal.ObjName).To(Equal(testObject))

			lomCloud := &cluster.LOM{ObjName: testObject}
			err = lomCloud.Init(cmn.Bck{Name: sameBucketName, Provider: cmn.ProviderAmazon})
			Expect(err).NotTo(HaveOccurred())
			err = lomCloud.Load(false, false)
			Expect(cmn.IsObjNotExist(err)).To(BeTrue())
			Expect(lomCloud.FQN).To(Equal(desiredCloudFQN))
			Expect(lomCloud.Uname()).To(Equal(lomCloud.Bck().MakeUname(testObject)))
			Expect(lomCloud.Bck().Provider).To(Equal(cmn.ProviderAmazon))
			Expect(lomCloud.MpathInfo().Path).To(Equal(mpaths[0]))
			expectEqualBck(lomCloud.Bucket(), cloudSameBck)
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
func NewBasicLom(fqn string) *cluster.LOM {
	lom := &cluster.LOM{FQN: fqn}
	err := lom.Init(cmn.Bck{})
	Expect(err).NotTo(HaveOccurred())
	return lom
}

func filePut(fqn string, size int) *cluster.LOM {
	createTestFile(fqn, size)
	lom := NewBasicLom(fqn)
	lom.SetSize(int64(size))
	lom.IncVersion()
	Expect(lom.Persist()).NotTo(HaveOccurred())
	lom.Uncache(false)
	return lom
}

func createTestFile(fqn string, size int) {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		buff := make([]byte, size)
		_, _ = rand.Read(buff)
		_, err := testFile.Write(buff)
		_ = testFile.Close()

		Expect(err).ShouldNot(HaveOccurred())
	}
}

func getTestFileHash(fqn string) (hash string) {
	reader, _ := os.Open(fqn)
	_, cksum, err := cos.CopyAndChecksum(io.Discard, reader, nil, cos.ChecksumXXHash)
	Expect(err).NotTo(HaveOccurred())
	hash = cksum.Value()
	reader.Close()
	return
}

func expectEqualBck(left, right cmn.Bck) {
	p := right.Props
	right.Props = left.Props
	_ = Expect(left).To(Equal(right))
	right.Props = p
}
