/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster_test

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

const (
	mpath  = "/tmp/lomtest_mpath/1"
	mpath2 = "/tmp/lomtest_mpath/2"

	bucketLocalA = "LOM_TEST_Local_1"
	bucketLocalB = "LOM_TEST_Local_2"

	bucketCloudA = "LOM_TEST_Cloud_1"
	bucketCloudB = "LOM_TEST_Cloud_2"

	sameBucketName = "LOM_TEST_Local_and_Cloud"
)

var _ = Describe("LOM", func() {
	oldCloudProvider := cmn.GCO.Get().CloudProvider

	cmn.CreateDir(mpath)
	cmn.CreateDir(mpath2)

	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.Add(mpath)
	fs.Mountpaths.DisableFsIDCheck()
	fs.Mountpaths.Add(mpath2)
	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	tMock := cluster.NewTargetMock(cluster.BownerMock{BMD: cluster.BMD{
		LBmap: map[string]*cmn.BucketProps{
			// Map local buckets here referenced by test lom
			bucketLocalA: &cmn.BucketProps{
				Cksum: cmn.CksumConf{Type: cmn.ChecksumNone},
			},
			bucketLocalB: &cmn.BucketProps{
				Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash},
				LRU:   cmn.LRUConf{Enabled: true},
			},
			sameBucketName: &cmn.BucketProps{},
		},
		CBmap: map[string]*cmn.BucketProps{
			// Map cloud buckets here referenced by test lom
			bucketCloudA:   &cmn.BucketProps{},
			bucketCloudB:   &cmn.BucketProps{},
			sameBucketName: &cmn.BucketProps{},
		},
		Version: 1,
	}})

	BeforeEach(func() {
		//dummy cloud provider for tests involving cloud buckets
		cmn.GCO.Get().CloudProvider = cmn.ProviderAmazon

		os.RemoveAll(mpath)
		os.RemoveAll(mpath2)
		cmn.CreateDir(mpath)
		cmn.CreateDir(mpath2)
	})
	AfterEach(func() {
		os.RemoveAll(mpath)
		os.RemoveAll(mpath2)

		cmn.GCO.Get().CloudProvider = oldCloudProvider
	})

	Describe("FQN Resolution", func() {
		testObject := "foldr/test-obj.ext"
		desiredLocalFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

		When("run for a local bucket", func() {
			It("Should populate fields from Bucket and Objname", func() {
				fs.Mountpaths.Disable(mpath2) // Ensure that it matches desiredLocalFQN

				lom := &cluster.LOM{T: tMock, Bucket: bucketLocalA, Objname: testObject}
				Expect(lom.Fill("", 0)).To(BeEmpty())
				Expect(lom.FQN).To(BeEquivalentTo(desiredLocalFQN))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketLocalA, testObject)))
				Expect(lom.BckIsLocal).To(BeTrue())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeTrue())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpath2)
			})

			It("Should populate fields from a FQN", func() {

				lom := &cluster.LOM{T: tMock, FQN: desiredLocalFQN}
				Expect(lom.Fill("", 0)).To(BeEmpty())
				Expect(lom.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketLocalA, testObject)))
				Expect(lom.BckIsLocal).To(BeTrue())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeTrue())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))
			})

			It("Should resolve work files", func() {
				testPid := strconv.FormatInt(9876, 16)
				testTieIndex := strconv.FormatInt(1355314332000000, 16)[5:]
				workObject := "foldr/get.test-obj.ext" + "." + testTieIndex + "." + testPid
				localFQN := filepath.Join(mpath, fs.WorkfileType, cmn.LocalBs, bucketLocalA, workObject)

				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(lom.Fill("", 0)).To(BeEmpty())
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.WorkfileType))
			})
		})

		When("run for a cloud bucket", func() {
			testObject := "foldr/test-obj.ext"
			desiredCloudFQN := filepath.Join(mpath, fs.ObjectType, cmn.CloudBs, bucketCloudA, testObject)

			It("Should populate fields from Bucket and Objname", func() {
				fs.Mountpaths.Disable(mpath2) // Ensure that it matches desiredCloudFQN

				lom := &cluster.LOM{T: tMock, Bucket: bucketCloudA, Objname: testObject}
				Expect(lom.Fill("", 0)).To(BeEmpty())
				Expect(lom.FQN).To(BeEquivalentTo(desiredCloudFQN))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketCloudA, testObject)))
				Expect(lom.BckIsLocal).To(BeFalse())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeFalse())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpath2)
			})

			It("Should populate fields from a FQN", func() {
				lom := &cluster.LOM{T: tMock, FQN: desiredCloudFQN}
				Expect(lom.Fill("", 0)).To(BeEmpty())
				Expect(lom.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketCloudA, testObject)))
				Expect(lom.BckIsLocal).To(BeFalse())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeFalse())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))
			})
		})

		When("run for invalid FQN", func() {
			DescribeTable("should return error",
				func(fqn string) {
					mlom := &cluster.LOM{T: tMock, FQN: fqn}
					Expect(mlom.Fill("", 0)).ToNot(BeEmpty())
				},
				Entry(
					"invalid object name",
					filepath.Join("blah", fs.ObjectType, cmn.LocalBs, bucketLocalA, " ??? "),
				),
				Entry(
					"invalid fqn",
					"?/.,",
				),
				Entry(
					"missing content type",
					filepath.Join(mpath),
				),
				Entry(
					"missing bucket type",
					filepath.Join(mpath, fs.ObjectType),
				),
				Entry(
					"missing bucket",
					filepath.Join(mpath, fs.ObjectType, cmn.LocalBs),
				),
				Entry(
					"missing object",
					filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA),
				),
				Entry(
					"non-existent mountpath",
					filepath.Join("blah", fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject),
				),
				Entry(
					"non-existent bucket",
					filepath.Join(mpath, "blah", cmn.LocalBs, bucketCloudA, testObject),
				),
				Entry(
					"non-existent bucket type",
					filepath.Join(mpath, fs.ObjectType, "blah", bucketLocalA, testObject),
				),
				Entry(
					"non-existent bucket",
					filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, "blah", testObject),
				),
				Entry(
					"mismatched bucket type 2",
					filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketCloudA, testObject),
				),
			)
		})
	})

	Describe("Fill", func() {
		Describe("Fstat", func() {
			testFileSize := 123
			testObjectName := "fstat-foldr/test-obj.ext"
			localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObjectName)

			It("should be able to mark file as Non-existent", func() {
				os.Remove(localFQN)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(lom.Fill("", cluster.LomFstat)).To(BeEmpty())

				Expect(lom.Exists()).To(BeFalse())
			})

			It("should be able to mark file as Existent", func() {
				createTestFile(localFQN, testFileSize)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(lom.Fill("", cluster.LomFstat)).To(BeEmpty())

				Expect(lom.Exists()).To(BeTrue())
				Expect(lom.Size).To(BeEquivalentTo(testFileSize))
			})
		})

		Describe("Atime", func() {
			desiredAtime := time.Unix(1500000000, 0)
			testObjectName := "foldr/test-obj.ext"

			It("should fetch atime for bucket with LRU disabled", func() {
				localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(lom.Fill("", cluster.LomAtime)).To(BeEmpty())

				Expect(lom.Atime).To(BeEquivalentTo(desiredAtime))
				Expect(lom.Atimestr()).To(BeEquivalentTo(desiredAtime.Format(cmn.RFC822)))
			})
			It("should fetch atime for bucket with LRU enabled", func() {
				setupMockRunner(tMock)
				defer teardownMockRunner(tMock)

				localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
				createTestFile(localFQN, 0)
				Expect(os.Chtimes(localFQN, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())
				lom := &cluster.LOM{T: tMock, FQN: localFQN}

				Expect(lom.Fill("", cluster.LomAtime)).To(BeEmpty())

				Expect(lom.Atime).To(BeEquivalentTo(desiredAtime))
				Expect(lom.Atimestr()).To(BeEquivalentTo(desiredAtime.Format(cmn.RFC822)))
			})
		})

		Describe("checksum", func() {
			testFileSize := 456
			testObjectName := "cksum-foldr/test-obj.ext"
			//Bucket needs to have checksum enabled
			localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)

			It("should ignore if bucket checksum is none", func() {
				testObject := "foldr/test-obj.ext"
				noneFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

				lom := &cluster.LOM{T: tMock, FQN: noneFQN}
				Expect(lom.Fill("", cluster.LomCksum|cluster.LomCksumMissingRecomp)).To(BeEmpty())

				Expect(lom.Cksum).To(BeNil())
			})

			It("should not retrieve cksum when it was not previously computed", func() {
				createTestFile(localFQN, testFileSize)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}

				Expect(lom.Fill("", cluster.LomCksum)).To(BeEmpty())
				Expect(lom.Cksum).To(BeNil())
			})

			It("should be able to verify checksum", func() {
				createTestFile(localFQN, testFileSize)
				expectedChecksum := getTestFileHash(localFQN)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(expectedChecksum))).To(BeEmpty())

				Expect(lom.Fill("", cluster.LomCksum|cluster.LomCksumPresentRecomp)).To(BeEmpty())

				_, val := lom.Cksum.Get()
				Expect(val).To(BeEquivalentTo(expectedChecksum))
				Expect(lom.BadCksum).To(BeFalse())
			})

			It("should be able to detect bad checksum", func() {
				createTestFile(localFQN, testFileSize)
				badChecksum := "EA5EACE"
				lom := &cluster.LOM{T: tMock, FQN: localFQN}
				Expect(fs.SetXattr(lom.FQN, cmn.XattrXXHash, []byte(badChecksum))).To(BeEmpty())

				Expect(lom.Fill("", cluster.LomCksum|cluster.LomCksumPresentRecomp)).ToNot(BeEmpty())

				_, cksumValue := lom.Cksum.Get()
				Expect(cksumValue).To(BeEquivalentTo(badChecksum))
				Expect(lom.BadCksum).To(BeTrue())
			})

			It("should be able to calculate and store checksum", func() {
				createTestFile(localFQN, testFileSize)
				expectedHash := getTestFileHash(localFQN)
				lom := &cluster.LOM{T: tMock, FQN: localFQN}

				Expect(lom.Fill("", cluster.LomCksum|cluster.LomCksumMissingRecomp)).To(BeEmpty())

				_, cksumValue := lom.Cksum.Get()
				Expect(cksumValue).To(BeEquivalentTo(expectedHash))
				xattrVal, _ := fs.GetXattr(lom.FQN, cmn.XattrXXHash)
				Expect(xattrVal).To(BeEquivalentTo(expectedHash))
			})
		})

		Describe("Version", func() {
			testObject := "foldr/test-obj.ext"
			desiredVersion := "9001"
			localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

			It("should be able to get version", func() {
				createTestFile(localFQN, 0)
				Expect(fs.SetXattr(localFQN, cmn.XattrVersion, []byte(desiredVersion))).To(BeEmpty())

				lom := &cluster.LOM{T: tMock, FQN: localFQN}

				Expect(lom.Fill("", cluster.LomVersion)).To(BeEmpty())
				Expect(lom.Version).To(BeEquivalentTo(desiredVersion))
			})
		})

		Describe("LomCopy", func() {
			testFileSize := 123
			testObject := "foldr/test-obj.ext"
			localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)
			copyFQN := filepath.Join(mpath2, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

			It("should be able to get copy", func() {
				createTestFile(localFQN, testFileSize)
				Expect(fs.SetXattr(localFQN, cmn.XattrCopies, []byte(copyFQN))).To(BeEmpty())

				lom := &cluster.LOM{T: tMock, FQN: localFQN}

				Expect(lom.Fill("", cluster.LomCopy)).To(BeEmpty())
				Expect(lom.CopyFQN[0]).To(BeEquivalentTo(copyFQN))
			})
		})
	})

	Describe("cop object methods", func() {
		testObjectName := "foldr/test-obj.ext"
		localFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
		copyFQN := filepath.Join(mpath2, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
		testFileSize := 101
		desiredVersion := "9002"

		if _, hrw, _ := cluster.ResolveFQN(localFQN, tMock.GetBowner()); hrw != localFQN {
			localFQN, copyFQN = copyFQN, localFQN
		}

		prepareLomWithCopy := func(setXCopy bool) (lom *cluster.LOM) {
			//Prepares a basic lom with a copy
			createTestFile(localFQN, testFileSize)
			Expect(fs.SetXattr(localFQN, cmn.XattrVersion, []byte(desiredVersion))).To(BeEmpty())
			lom = &cluster.LOM{T: tMock, FQN: localFQN}
			Expect(lom.Fill("", cluster.LomVersion|cluster.LomCksum|cluster.LomCksumMissingRecomp)).To(BeEmpty())

			Expect(lom.CopyObject(copyFQN, make([]byte, testFileSize))).ShouldNot(HaveOccurred())
			_, err := os.Stat(copyFQN)
			Expect(os.IsNotExist(err)).To(BeFalse())

			if setXCopy {
				Expect(lom.SetXcopy(copyFQN)).Should(BeEmpty())
			}
			return
		}

		Describe("CopyObject", func() {
			It("Should successfully copy the object", func() {
				prepareLomWithCopy(false)
				expectedHash := getTestFileHash(localFQN)

				//Check copy created
				var xattr []byte
				xattr, _ = fs.GetXattr(copyFQN, cmn.XattrVersion)
				Expect(string(xattr)).To(BeEquivalentTo(desiredVersion))
				xattr, _ = fs.GetXattr(copyFQN, cmn.XattrXXHash)
				Expect(string(xattr)).To(BeEquivalentTo(expectedHash))

				//Check copy contents are corrrect
				Expect(getTestFileHash(copyFQN)).To(BeEquivalentTo(expectedHash))
			})
		})

		Describe("SetXcopy", func() {
			It("Should corectly set Xattributes", func() {
				lom := prepareLomWithCopy(true)

				// Check copy set
				var xattr []byte
				xattr, _ = fs.GetXattr(copyFQN, cmn.XattrCopies)
				Expect(string(xattr)).To(BeEquivalentTo(localFQN))
				xattr, _ = fs.GetXattr(localFQN, cmn.XattrCopies)
				Expect(string(xattr)).To(BeEquivalentTo(copyFQN))

				Expect(lom.CopyFQN[0]).To(BeEquivalentTo(copyFQN))

				// Check msic copy data
				lomCopy := &cluster.LOM{T: tMock, FQN: copyFQN}
				Expect(lomCopy.Fill("", cluster.LomVersion|cluster.LomCksum|cluster.LomCksumMissingRecomp|cluster.LomCopy)).To(BeEmpty())

				Expect(lomCopy.HrwFQN).To(BeEquivalentTo(lom.HrwFQN))
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopies()).To(BeTrue())
				Expect(lomCopy.IsCopy()).To(BeTrue())
				Expect(lomCopy.HasCopies()).To(BeFalse())

			})
		})

		Describe("DelAllCopies", func() {
			It("Should be able to delete all copies", func() {
				lom := prepareLomWithCopy(true)

				Expect(lom.DelAllCopies()).To(BeEmpty())
				_, err := os.Stat(copyFQN)
				Expect(os.IsNotExist(err)).To(BeTrue())
			})
		})
	})

	Describe("copy LOM struct", func() {
		BeforeEach(func() {
			fs.Mountpaths.Disable(mpath2) // Ensure that it matches desiredCloudFQN
		})

		AfterEach(func() {
			fs.Mountpaths.Enable(mpath2)
		})

		It("should shallow copy LOM struct", func() {
			testObject := "foldr/test-obj.ext"

			lomEmpty := &cluster.LOM{T: tMock, Bucket: sameBucketName, Objname: testObject}
			Expect(lomEmpty.Fill("", 0)).To(BeEmpty())

			copiedLOM := lomEmpty.CloneAndSet(cluster.LOMCopyProps{})
			Expect(copiedLOM).To(Equal(lomEmpty))
		})

		It("should copy LOM struct and fill with props", func() {
			testObject := "foldr/test-obj.ext"
			desiredLocalFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, sameBucketName, testObject)
			desiredCloudFQN := filepath.Join(mpath, fs.ObjectType, cmn.CloudBs, sameBucketName, testObject)

			lomEmpty := &cluster.LOM{T: tMock, Bucket: sameBucketName, Objname: testObject}
			Expect(lomEmpty.Fill("", 0)).To(BeEmpty())
			Expect(lomEmpty.FQN).To(Equal(desiredLocalFQN))
			Expect(lomEmpty.Cksum).To(BeNil())
			Expect(lomEmpty.Version).To(BeEmpty())

			cksum := cmn.NewCksum(cmn.ChecksumXXHash, "something")
			version := "102"
			copiedLOM := lomEmpty.CloneAndSet(cluster.LOMCopyProps{
				FQN:     desiredCloudFQN,
				Cksum:   cksum,
				Version: version,
			})
			Expect(copiedLOM.FQN).To(Equal(desiredCloudFQN))
			Expect(copiedLOM.Uname).To(Equal(cluster.Uname(sameBucketName, testObject)))
			Expect(copiedLOM.BckIsLocal).To(BeFalse())
			Expect(copiedLOM.ParsedFQN.IsLocal).To(BeFalse())
			Expect(copiedLOM.ParsedFQN.MpathInfo.Path).To(Equal(mpath))
			Expect(copiedLOM.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(copiedLOM.ParsedFQN.Objname).To(Equal(testObject))
			Expect(copiedLOM.ParsedFQN.ContentType).To(Equal(fs.ObjectType))
			Expect(copiedLOM.Cksum).To(Equal(cksum))
			Expect(copiedLOM.Version).To(Equal(version))
		})
	})

	Describe("local and cloud bucket with same name", func() {
		It("should have different fqn", func() {
			testObject := "foldr/test-obj.ext"
			desiredLocalFQN := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, sameBucketName, testObject)
			desiredCloudFQN := filepath.Join(mpath, fs.ObjectType, cmn.CloudBs, sameBucketName, testObject)

			fs.Mountpaths.Disable(mpath2) // Ensure that it matches desiredCloudFQN

			lomEmpty := &cluster.LOM{T: tMock, Bucket: sameBucketName, Objname: testObject}
			Expect(lomEmpty.Fill("", 0)).To(BeEmpty())
			Expect(lomEmpty.FQN).To(Equal(desiredLocalFQN))
			Expect(lomEmpty.Uname).To(Equal(cluster.Uname(sameBucketName, testObject)))
			Expect(lomEmpty.BckIsLocal).To(BeTrue())
			Expect(lomEmpty.ParsedFQN.IsLocal).To(BeTrue())
			Expect(lomEmpty.ParsedFQN.MpathInfo.Path).To(Equal(mpath))
			Expect(lomEmpty.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomEmpty.ParsedFQN.Objname).To(Equal(testObject))
			Expect(lomEmpty.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			lomLocal := &cluster.LOM{T: tMock, Bucket: sameBucketName, Objname: testObject}
			Expect(lomLocal.Fill(cmn.LocalBs, 0)).To(BeEmpty())
			Expect(lomLocal.FQN).To(Equal(desiredLocalFQN))
			Expect(lomLocal.Uname).To(Equal(cluster.Uname(sameBucketName, testObject)))
			Expect(lomLocal.BckIsLocal).To(BeTrue())
			Expect(lomLocal.ParsedFQN.IsLocal).To(BeTrue())
			Expect(lomLocal.ParsedFQN.MpathInfo.Path).To(Equal(mpath))
			Expect(lomLocal.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomLocal.ParsedFQN.Objname).To(Equal(testObject))
			Expect(lomLocal.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			lomCloud := &cluster.LOM{T: tMock, Bucket: sameBucketName, Objname: testObject}
			Expect(lomCloud.Fill(cmn.CloudBs, 0)).To(BeEmpty())
			Expect(lomCloud.FQN).To(Equal(desiredCloudFQN))
			Expect(lomCloud.Uname).To(Equal(cluster.Uname(sameBucketName, testObject)))
			Expect(lomCloud.BckIsLocal).To(BeFalse())
			Expect(lomCloud.ParsedFQN.IsLocal).To(BeFalse())
			Expect(lomCloud.ParsedFQN.MpathInfo.Path).To(Equal(mpath))
			Expect(lomCloud.ParsedFQN.Bucket).To(Equal(sameBucketName))
			Expect(lomCloud.ParsedFQN.Objname).To(Equal(testObject))
			Expect(lomCloud.ParsedFQN.ContentType).To(Equal(fs.ObjectType))

			fs.Mountpaths.Enable(mpath2)
		})
	})
})

//
// HELPERS
//

func setupMockRunner(t *cluster.TargetMock) {
	t.Atime = atime.NewRunner(fs.Mountpaths, ios.NewIostatRunner())
	go t.Atime.Run()
}

func teardownMockRunner(t *cluster.TargetMock) {
	t.Atime.Stop(errors.New(""))
	t.Atime = nil
}

func createTestFile(fqn string, size int) {
	os.Remove(fqn)
	testFile, err := cmn.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		buff := make([]byte, size)
		rand.Read(buff)
		_, err := testFile.Write(buff)
		testFile.Close()

		Expect(err).ShouldNot(HaveOccurred())
	}
}

func getTestFileHash(fqn string) (hash string) {
	hashReader, _ := os.Open(fqn)
	var errstr string
	hash, errstr = cmn.ComputeXXHash(hashReader, nil)
	Expect(errstr).To(BeEmpty())
	return
}
