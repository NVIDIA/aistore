/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster_test

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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
		mpaths []string
		mis    []*fs.MountpathInfo

		oldCloudProvider = cmn.GCO.Get().CloudProvider
		config           = cmn.GCO.BeginUpdate()
	)

	for i := 0; i < numMpaths; i++ {
		mpath := fmt.Sprintf("%s/mpath%d", tmpDir, i)
		mpaths = append(mpaths, mpath)
		mis = append(mis, &fs.MountpathInfo{Path: mpath})
		_ = cmn.CreateDir(mpath)
	}

	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.InitMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	for _, mpath := range mpaths {
		_ = fs.Mountpaths.Add(mpath)
	}

	_ = fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	tMock := cluster.NewTargetMock(cluster.BownerMock{BMD: cluster.BMD{
		LBmap: map[string]*cmn.BucketProps{
			// Map ais buckets here referenced by test lom
			bucketLocalA: {
				Cksum: cmn.CksumConf{Type: cmn.ChecksumNone},
			},
			bucketLocalB: {
				Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash},
				LRU:   cmn.LRUConf{Enabled: true},
			},
			bucketLocalC: {
				Cksum:  cmn.CksumConf{Type: cmn.ChecksumXXHash},
				LRU:    cmn.LRUConf{Enabled: true},
				Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
			},
			sameBucketName: {},
		},
		CBmap: map[string]*cmn.BucketProps{
			// Map cloud buckets here referenced by test lom
			bucketCloudA:   {},
			bucketCloudB:   {},
			sameBucketName: {},
		},
		Version: 1,
	}})

	BeforeEach(func() {
		// Dummy cloud provider for tests involving cloud buckets
		cmn.GCO.Get().CloudProvider = cmn.ProviderAmazon

		for _, mpath := range mpaths {
			_ = fs.Mountpaths.Add(mpath)
		}
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)

		cmn.GCO.Get().CloudProvider = oldCloudProvider
	})

	Describe("FQN Resolution", func() {
		testObject := "foldr/test-obj.ext"
		desiredLocalFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)

		When("run for an ais bucket", func() {
			It("Should populate fields from Bucket and Objname", func() {
				fs.Mountpaths.Disable(mpaths[1]) // Ensure that it matches desiredLocalFQN
				fs.Mountpaths.Disable(mpaths[2]) // Ensure that it matches desiredLocalFQN

				lom := &cluster.LOM{T: tMock, Objname: testObject}
				err := lom.Init(bucketLocalA, cmn.AIS)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredLocalFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(cluster.Bo2Uname(bucketLocalA, testObject)))
				Expect(lom.IsAIS()).To(BeTrue())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.Provider).To(BeEquivalentTo(cmn.AIS))
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpaths[0]))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ParsedFQN.ObjName).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpaths[1])
				fs.Mountpaths.Enable(mpaths[2])
			})

			It("Should populate fields from a FQN", func() {

				lom := &cluster.LOM{T: tMock, FQN: desiredLocalFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bucket()).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(cluster.Bo2Uname(bucketLocalA, testObject)))
				Expect(lom.IsAIS()).To(BeTrue())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.Provider).To(BeEquivalentTo(cmn.AIS))
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpaths[0]))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ParsedFQN.ObjName).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))
			})

			It("Should resolve work files", func() {
				testPid := strconv.FormatInt(9876, 16)
				testTieIndex := strconv.FormatInt(1355314332000000, 16)[5:]
				workObject := "foldr/get.test-obj.ext" + "." + testTieIndex + "." + testPid
				localFQN := mis[0].MakePathBucketObject(fs.WorkfileType, bucketLocalA, cmn.AIS, workObject)

				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.WorkfileType))
			})
		})

		When("run for a cloud bucket", func() {
			testObject := "foldr/test-obj.ext"
			desiredCloudFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketCloudA, cmn.Cloud, testObject)

			It("Should populate fields from Bucket and Objname", func() {
				// Ensure that it matches desiredCloudFQN
				fs.Mountpaths.Disable(mpaths[1])
				fs.Mountpaths.Disable(mpaths[2])

				lom := &cluster.LOM{T: tMock, Objname: testObject}
				err := lom.Init(bucketCloudA, cmn.Cloud)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.FQN).To(BeEquivalentTo(desiredCloudFQN))

				Expect(lom.Uname()).To(BeEquivalentTo(cluster.Bo2Uname(bucketCloudA, testObject)))
				Expect(lom.IsAIS()).To(BeFalse())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.Provider).To(BeEquivalentTo(cmn.Cloud))
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpaths[0]))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ParsedFQN.ObjName).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpaths[2])
				fs.Mountpaths.Enable(mpaths[1])
			})

			It("Should populate fields from a FQN", func() {
				lom := &cluster.LOM{T: tMock, FQN: desiredCloudFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Bucket()).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname()).To(BeEquivalentTo(cluster.Bo2Uname(bucketCloudA, testObject)))
				Expect(lom.IsAIS()).To(BeFalse())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.Provider).To(BeEquivalentTo(cmn.Cloud))
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpaths[0]))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ParsedFQN.ObjName).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))
			})
		})

		When("run for invalid FQN", func() {
			DescribeTable("should return error",
				func(fqn string) {
					lom := &cluster.LOM{T: tMock, FQN: fqn}
					err := lom.Init("", "")
					Expect(err).To(HaveOccurred())
				},
				Entry(
					"invalid object name",
					mis[0].MakePathBucketObject(fs.ObjectType, bucketCloudA, cmn.AIS, " ??? "),
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
					mis[0].MakePathBucketObject(fs.ObjectType, "", cmn.AIS, " ??? "),
				),
				Entry(
					"missing object",
					mis[0].MakePathBucket(fs.ObjectType, bucketLocalA, cmn.AIS),
				),
			)
		})
	})

	Describe("Load", func() {
		Describe("Exists", func() {
			testFileSize := 123
			testObjectName := "fstat-foldr/test-obj.ext"
			localFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObjectName)

			It("should find out that object does not exist", func() {
				os.Remove(localFQN)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				err = lom.Load(false)
				Expect(cmn.IsNotObjExist(err)).To(BeTrue())
			})

			It("should find out that object exists", func() {
				createTestFile(localFQN, testFileSize)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				err := lom.Init("", "")
				lom.SetSize(int64(testFileSize))
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Size()).To(BeEquivalentTo(testFileSize))
			})
		})

		Describe("Atime", func() {
			desiredAtime := time.Unix(1500000000, 0)
			testObjectName := "foldr/test-obj.ext"

			It("should fetch atime for bucket with LRU disabled", func() {
				localFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false)
				Expect(err).NotTo(HaveOccurred())

				Expect(lom.Atime()).To(BeEquivalentTo(desiredAtime))
			})
			It("should fetch atime for bucket with LRU enabled", func() {
				localFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalB, cmn.AIS, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				err := lom.Init("", "")
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Persist()).NotTo(HaveOccurred())
				err = lom.Load(false)
				Expect(err).NotTo(HaveOccurred())

				Expect(lom.Atime()).To(BeEquivalentTo(desiredAtime))
			})
		})

		Describe("checksum", func() {
			testFileSize := 456
			testObjectName := "cksum-foldr/test-obj.ext"
			// Bucket needs to have checksum enabled
			localFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalB, cmn.AIS, testObjectName)
			dummyCksm := cmn.NewCksum(cmn.ChecksumXXHash, "dummycksm")

			Describe("CksumComputeIfMissing", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)

					lom := NewBasicLom(noneFQN, tMock)
					cksum, err := lom.CksumComputeIfMissing()
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).To(BeNil())
				})

				It("should not compute if not missing", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalB, cmn.AIS, testObject)

					lom := NewBasicLom(noneFQN, tMock)
					lom.SetCksum(dummyCksm)
					cksum, err := lom.CksumComputeIfMissing()
					Expect(err).NotTo(HaveOccurred())
					Expect(cksum).To(BeEquivalentTo(dummyCksm))
				})

				It("should recompute checksum and not store anywhere", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					expectedChecksum := getTestFileHash(localFQN)

					cksum, err := lom.CksumComputeIfMissing()
					Expect(err).NotTo(HaveOccurred())
					cksumType, cksumValue := cksum.Get()
					Expect(cksumType).To(BeEquivalentTo(cmn.ChecksumXXHash))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))

					Expect(lom.Cksum()).To(BeNil())
					newLom := NewBasicLom(lom.FQN, tMock)
					err = newLom.Load(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(newLom.Cksum()).To(BeNil())
				})
			})

			Describe("ValidateMetaChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)

					lom := NewBasicLom(noneFQN, tMock)
					err := lom.ValidateMetaChecksum()
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Cksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN, tMock)
					err := fsLOM.Load(false)
					Expect(err).NotTo(HaveOccurred())
					Expect(fsLOM.Cksum()).To(BeNil())

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
					lom.Uncache()

					Expect(lom.Cksum()).ToNot(BeNil())
					_, val := lom.Cksum().Get()
					fsLOM = NewBasicLom(localFQN, tMock)
					err = fsLOM.Load(false)
					Expect(err).ShouldNot(HaveOccurred())
					_, fsVal := fsLOM.Cksum().Get()
					Expect(fsVal).To(BeEquivalentTo(expectedChecksum))
					Expect(val).To(BeEquivalentTo(expectedChecksum))
				})

				It("should accept when filesystem and memory checksums match", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should accept when both filesystem and memory checksums are nil", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when memory has wrong checksum", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())

					lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "wrong checksum"))
					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					Expect(ioutil.WriteFile(localFQN, []byte("wrong file"), 0644)).To(BeNil())

					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})

				It("should not check object content when recompute false", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					Expect(ioutil.WriteFile(localFQN, []byte("wrong file"), 0644)).To(BeNil())
					Expect(lom.ValidateMetaChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when xattr has wrong checksum", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, "wrong checksum"))
					Expect(lom.ValidateMetaChecksum()).To(HaveOccurred())
				})
			})

			// copy-paste of some of ValidateMetaChecksum tests, however if there's no
			// mocking solution, it's needed to have the same tests for both methods
			Describe("ValidateContentChecksum", func() {
				It("should ignore if bucket checksum is none", func() {
					testObject := "foldr/test-obj.ext"
					noneFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)

					lom := NewBasicLom(noneFQN, tMock)
					err := lom.ValidateContentChecksum()
					Expect(err).NotTo(HaveOccurred())
					Expect(lom.Cksum()).To(BeNil())
				})

				It("should fill object with checksum if was not present", func() {
					lom := filePut(localFQN, testFileSize, tMock)
					expectedChecksum := getTestFileHash(localFQN)

					fsLOM := NewBasicLom(localFQN, tMock)
					err := fsLOM.Load(false)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(fsLOM.Cksum()).To(BeNil())

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
					lom.Uncache()

					Expect(lom.Cksum()).ToNot(BeNil())
					_, cksumValue := lom.Cksum().Get()

					fsLOM = NewBasicLom(localFQN, tMock)
					err = fsLOM.Load(false)
					Expect(err).ShouldNot(HaveOccurred())

					_, fsCksmVal := fsLOM.Cksum().Get()
					Expect(fsCksmVal).To(BeEquivalentTo(expectedChecksum))
					Expect(cksumValue).To(BeEquivalentTo(expectedChecksum))
				})

				It("should accept when filesystem and memory checksums match", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN, tMock)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
				})

				It("should accept when both filesystem and memory checksums are nil", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN, tMock)

					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
				})

				It("should not accept when object content has changed", func() {
					createTestFile(localFQN, testFileSize)
					lom := NewBasicLom(localFQN, tMock)
					Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())

					err := ioutil.WriteFile(localFQN, []byte("wrong file"), 0644)
					Expect(err).ShouldNot(HaveOccurred())

					Expect(lom.ValidateContentChecksum()).To(HaveOccurred())
				})
			})

			Describe("FromFS", func() {
				It("should error if file does not exist", func() {
					testObject := "foldr/test-obj-doesnt-exist.ext"
					noneFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)
					lom := NewBasicLom(noneFQN, tMock)

					Expect(lom.FromFS()).To(HaveOccurred())
				})

				It("should fill object with correct meta", func() {
					startTime := time.Now()
					time.Sleep(50 * time.Millisecond)
					lom1 := filePut(localFQN, testFileSize, tMock)
					lom2 := NewBasicLom(localFQN, tMock)
					Expect(lom1.Persist()).NotTo(HaveOccurred())

					Expect(lom1.ValidateContentChecksum()).NotTo(HaveOccurred())
					Expect(lom1.Persist()).ToNot(HaveOccurred())

					Expect(lom2.FromFS()).ToNot(HaveOccurred())
					Expect(lom2.Cksum()).To(BeEquivalentTo(lom1.Cksum()))
					Expect(lom2.Version()).To(BeEquivalentTo(lom1.Version()))
					Expect(lom2.Size()).To(BeEquivalentTo(testFileSize))
					Expect(lom2.Atime().After(startTime)).To(BeTrue())
				})
			})
		})

		Describe("Version", func() {
			testObject := "foldr/test-obj.ext"
			desiredVersion := "9001"
			localFQN := mis[0].MakePathBucketObject(fs.ObjectType, bucketLocalA, cmn.AIS, testObject)

			It("should be able to get version", func() {
				lom := filePut(localFQN, 0, tMock)
				lom.SetVersion(desiredVersion)
				Expect(lom.Persist()).NotTo(HaveOccurred())

				err := lom.Load(false)
				Expect(err).ToNot(HaveOccurred())
				Expect(lom.Version()).To(BeEquivalentTo(desiredVersion))
			})
		})
	})

	Describe("copy object methods", func() {
		const (
			testObjectName = "foldr/test-obj.ext"
			testFileSize   = 101
			desiredVersion = "9002"
		)

		findMpath := func(bucket string, defaultLoc bool, ignoreFQNs ...string) string {
		OuterLoop:
			for _, mi := range mis {
				fqn := mi.MakePathBucketObject(fs.ObjectType, bucket, cmn.AIS, testObjectName)
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
			cmn.Assert(false)
			return ""
		}

		mirrorFQNs := []string{
			// Bucket with redundancy
			findMpath(bucketLocalC, true /*defaultLoc*/),
			findMpath(bucketLocalC, false /*defaultLoc*/),
		}
		// Add another mirrorFQN but it must be different than the second one.
		mirrorFQNs = append(
			mirrorFQNs,
			findMpath(bucketLocalC, false /*defaultLoc*/, mirrorFQNs[1]),
		)

		copyFQNs := []string{
			// Bucket with no redundancy
			findMpath(bucketLocalB, true /*defaultLoc*/),
			findMpath(bucketLocalB, false /*defaultLoc*/),
		}

		prepareLOM := func(fqn string) (lom *cluster.LOM) {
			// Prepares a basic lom with a copy
			createTestFile(fqn, testFileSize)
			lom = &cluster.LOM{T: tMock, FQN: fqn}
			err := lom.Init("", "")
			lom.SetSize(int64(testFileSize))
			lom.SetVersion(desiredVersion)
			Expect(lom.Persist()).NotTo(HaveOccurred())
			lom.Uncache()
			Expect(err).NotTo(HaveOccurred())
			err = lom.Load(false)
			Expect(err).NotTo(HaveOccurred())
			Expect(lom.ValidateContentChecksum()).NotTo(HaveOccurred())
			return
		}

		prepareCopy := func(lom *cluster.LOM, fqn string) (dst *cluster.LOM) {
			var err error
			dst, err = lom.CopyObject(fqn, make([]byte, testFileSize))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(dst.FQN).To(BeARegularFile())
			Expect(dst.Size()).To(BeEquivalentTo(testFileSize))
			lom.Uncache()

			// Reload copy, to make sure it is fresh
			dst = NewBasicLom(dst.FQN, tMock)
			Expect(dst.Load(false)).NotTo(HaveOccurred())
			Expect(dst.ValidateContentChecksum()).NotTo(HaveOccurred())
			lom.Uncache()
			return
		}

		checkCopies := func(defaultLOM *cluster.LOM, copiesFQNs ...string) {
			expectedHash := getTestFileHash(defaultLOM.FQN)

			for _, copyFQN := range copiesFQNs {
				copyLOM := NewBasicLom(copyFQN, tMock)
				Expect(copyLOM.Load(false)).NotTo(HaveOccurred())

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
				_ = prepareCopy(lom, mirrorFQNs[1])
				_ = prepareCopy(lom, mirrorFQNs[2])

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
				_ = prepareCopy(lom, mirrorFQNs[1])

				// Check that copies were added to metadata.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Make one copy disappear.
				cmn.RemoveFile(mirrorFQNs[1])

				// Prepare another one (to trigger `syncMetaWithCopies`).
				_ = prepareCopy(lom, mirrorFQNs[2])

				// Check metadata of left copies (it also checks default object).
				checkCopies(lom, mirrorFQNs[0], mirrorFQNs[2])
			})

			It("should copy object without adding it to copies if dst bucket does not support mirroring", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])
				expectedHash := getTestFileHash(lom.FQN)

				// Check that copies were added to metadata.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				nonMirroredLOM := prepareCopy(lom, copyFQNs[0])

				// Check that nothing has changed in the src.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Check destination lom.
				Expect(nonMirroredLOM.FQN).NotTo(Equal(lom.FQN))
				_, cksumValue := nonMirroredLOM.Cksum().Get()
				Expect(cksumValue).To(Equal(expectedHash))
				Expect(nonMirroredLOM.Version()).To(Equal(desiredVersion))
				Expect(nonMirroredLOM.Size()).To(BeEquivalentTo(testFileSize))

				Expect(nonMirroredLOM.IsCopy()).To(BeFalse())
				Expect(nonMirroredLOM.HasCopies()).To(BeFalse())
				Expect(nonMirroredLOM.NumCopies()).To(Equal(1))
				Expect(nonMirroredLOM.GetCopies()).To(BeNil())

				// Check that the content of the copy is correct.
				copyObjHash := getTestFileHash(nonMirroredLOM.FQN)
				Expect(copyObjHash).To(BeEquivalentTo(expectedHash))
			})
		})

		Describe("DelCopies", func() {
			It("should delete mirrored copy", func() {
				lom := prepareLOM(mirrorFQNs[0])
				_ = prepareCopy(lom, mirrorFQNs[1])

				// Check that no copies were added to metadata.
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1])))

				// Delete copy and check if it's gone.
				Expect(lom.DelCopies(mirrorFQNs[1])).ToNot(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())

				// Reload default object and check if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0], tMock)
				Expect(lom.Load(false)).ToNot(HaveOccurred())
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
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(3))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1]), HaveKey(mirrorFQNs[2])))

				// Delete copy and check if it's gone.
				Expect(lom.DelCopies(mirrorFQNs[1])).ToNot(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())

				// Reload default object and check if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0], tMock)
				Expect(lom.Load(false)).ToNot(HaveOccurred())
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(2))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[2])))

				// Check that left copy was correctly updated.
				copyLOM := NewBasicLom(mirrorFQNs[2], tMock)
				Expect(copyLOM.Load(false)).NotTo(HaveOccurred())
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
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lom.NumCopies()).To(Equal(3))
				Expect(lom.GetCopies()).To(And(HaveKey(mirrorFQNs[0]), HaveKey(mirrorFQNs[1]), HaveKey(mirrorFQNs[2])))

				// Delete all copies and check if they are gone.
				Expect(lom.DelAllCopies()).NotTo(HaveOccurred())
				Expect(mirrorFQNs[1]).NotTo(BeAnExistingFile())
				Expect(mirrorFQNs[2]).NotTo(BeAnExistingFile())

				// Reload default object and see if the lom was correctly updated.
				lom = NewBasicLom(mirrorFQNs[0], tMock)
				Expect(lom.Load(false)).ToNot(HaveOccurred())
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
			desiredLocalFQN := mis[0].MakePathBucketObject(fs.ObjectType, sameBucketName, cmn.AIS, testObject)
			desiredCloudFQN := mis[0].MakePathBucketObject(fs.ObjectType, sameBucketName, cmn.Cloud, testObject)

			fs.Mountpaths.Disable(mpaths[1]) // Ensure that it matches desiredCloudFQN

			lomEmpty := &cluster.LOM{T: tMock, Objname: testObject}
			err := lomEmpty.Init(sameBucketName, "")
			Expect(err).NotTo(HaveOccurred())
			err = lomEmpty.Load(false)
			Expect(cmn.IsNotObjExist(err)).To(BeTrue())
			Expect(lomEmpty.FQN).To(Equal(desiredLocalFQN))
			Expect(lomEmpty.Uname()).To(Equal(cluster.Bo2Uname(sameBucketName, testObject)))
			Expect(lomEmpty.IsAIS()).To(BeTrue())
			Expect(lomEmpty.ParsedFQN.Provider).To(BeEquivalentTo(cmn.AIS))
			Expect(lomEmpty.ParsedFQN.MpathInfo.Path).To(Equal(mpaths[0]))
			Expect(lomEmpty.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomEmpty.ParsedFQN.ObjName).To(Equal(testObject))
			Expect(lomEmpty.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			lomLocal := &cluster.LOM{T: tMock, Objname: testObject}
			err = lomLocal.Init(sameBucketName, cmn.AIS)
			Expect(err).NotTo(HaveOccurred())
			err = lomLocal.Load(false)
			Expect(cmn.IsNotObjExist(err)).To(BeTrue())
			Expect(lomLocal.FQN).To(Equal(desiredLocalFQN))
			Expect(lomLocal.Uname()).To(Equal(cluster.Bo2Uname(sameBucketName, testObject)))
			Expect(lomLocal.IsAIS()).To(BeTrue())
			Expect(lomLocal.ParsedFQN.Provider).To(BeEquivalentTo(cmn.AIS))
			Expect(lomLocal.ParsedFQN.MpathInfo.Path).To(Equal(mpaths[0]))
			Expect(lomLocal.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomLocal.ParsedFQN.ObjName).To(Equal(testObject))
			Expect(lomLocal.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			lomCloud := &cluster.LOM{T: tMock, Objname: testObject}
			err = lomCloud.Init(sameBucketName, cmn.Cloud)
			Expect(err).NotTo(HaveOccurred())
			err = lomCloud.Load(false)
			Expect(cmn.IsNotObjExist(err)).To(BeTrue())
			Expect(lomCloud.FQN).To(Equal(desiredCloudFQN))
			Expect(lomCloud.Uname()).To(Equal(cluster.Bo2Uname(sameBucketName, testObject)))
			Expect(lomCloud.IsAIS()).To(BeFalse())
			Expect(lomCloud.ParsedFQN.Provider).To(BeEquivalentTo(cmn.Cloud))
			Expect(lomCloud.ParsedFQN.MpathInfo.Path).To(Equal(mpaths[0]))
			Expect(lomCloud.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomCloud.ParsedFQN.ObjName).To(Equal(testObject))
			Expect(lomCloud.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			fs.Mountpaths.Enable(mpaths[1])
		})
	})
})

//
// HELPERS
//

// needs to be called inside of gomega scope like Describe/It
func NewBasicLom(fqn string, t cluster.Target) *cluster.LOM {
	lom := &cluster.LOM{T: t, FQN: fqn}
	err := lom.Init("", "")
	Expect(err).NotTo(HaveOccurred())
	return lom
}

func filePut(fqn string, size int, t cluster.Target) *cluster.LOM {
	createTestFile(fqn, size)
	lom := NewBasicLom(fqn, t)
	lom.SetSize(int64(size))
	lom.IncVersion()
	Expect(lom.Persist()).NotTo(HaveOccurred())
	lom.Uncache()
	return lom
}

func createTestFile(fqn string, size int) {
	_ = os.Remove(fqn)
	testFile, err := cmn.CreateFile(fqn)
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
	hashReader, _ := os.Open(fqn)
	var err error
	hash, err = cmn.ComputeXXHash(hashReader, nil)
	Expect(err).NotTo(HaveOccurred())
	return
}
