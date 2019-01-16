/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
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
	"github.com/NVIDIA/aistore/memsys"

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
)

var (
	tMock targetMock
)

var _ = Describe("LOM", func() {
	cmn.CreateDir(mpath)
	cmn.CreateDir(mpath2)

	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.Add(mpath)
	fs.Mountpaths.DisableFsIDCheck()
	fs.Mountpaths.Add(mpath2)
	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	tMock = targetMock{
		b: bownerMock{BMD: cluster.BMD{
			LBmap: map[string]*cmn.BucketProps{
				//Map local buckets here refrenced by test lom
				bucketLocalA: &cmn.BucketProps{
					CksumConf: cmn.CksumConf{Checksum: cmn.ChecksumNone},
				},
				bucketLocalB: &cmn.BucketProps{
					LRUConf:   cmn.LRUConf{LRUEnabled: true},
					CksumConf: cmn.CksumConf{Checksum: cmn.ChecksumXXHash},
				},
			},
			CBmap: map[string]*cmn.BucketProps{
				//Map cloud buckets here refrenced by test lom
				bucketCloudA: &cmn.BucketProps{},
				bucketCloudB: &cmn.BucketProps{},
			},
			Version: 1,
		}},
		//test cases are responsible for preparing this as needed
		r: nil,
	}

	BeforeEach(func() {
		os.RemoveAll(mpath)
		os.RemoveAll(mpath2)
		cmn.CreateDir(mpath)
		cmn.CreateDir(mpath2)
	})
	AfterEach(func() {
		os.RemoveAll(mpath)
		os.RemoveAll(mpath2)
	})

	Describe("FQN Resolution", func() {
		testObject := "foldr/test-obj.ext"
		desiredLocalFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

		When("run for a local bucket", func() {
			It("Should populate fields from Bucket and Objname", func() {
				fs.Mountpaths.Disable(mpath2) //Ensure that it matches desiredLocalFqn

				lom := &cluster.LOM{T: tMock, Bucket: bucketLocalA, Objname: testObject}
				Expect(lom.Fill(0)).To(BeEmpty())
				Expect(lom.Fqn).To(BeEquivalentTo(desiredLocalFqn))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketLocalA, testObject)))
				Expect(lom.Bislocal).To(BeTrue())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeTrue())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpath2)
			})
			It("Should populate fields from a FQN", func() {

				lom := &cluster.LOM{T: tMock, Fqn: desiredLocalFqn}
				Expect(lom.Fill(0)).To(BeEmpty())
				Expect(lom.Bucket).To(BeEquivalentTo(bucketLocalA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketLocalA, testObject)))
				Expect(lom.Bislocal).To(BeTrue())

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
				localFqn := filepath.Join(mpath, fs.WorkfileType, cmn.LocalBs, bucketLocalA, workObject)

				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(lom.Fill(0)).To(BeEmpty())
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.WorkfileType))
			})
		})

		When("run for a cloud bucket", func() {
			testObject := "foldr/test-obj.ext"
			desiredCloudFqn := filepath.Join(mpath, fs.ObjectType, cmn.CloudBs, bucketCloudA, testObject)

			It("Should populate fields from Bucket and Objname", func() {
				fs.Mountpaths.Disable(mpath2) //Ensure that it matches desiredCloudFqn

				lom := &cluster.LOM{T: tMock, Bucket: bucketCloudA, Objname: testObject}
				Expect(lom.Fill(0)).To(BeEmpty())
				Expect(lom.Fqn).To(BeEquivalentTo(desiredCloudFqn))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketCloudA, testObject)))
				Expect(lom.Bislocal).To(BeFalse())

				// from lom.go: redundant in-part; tradeoff to speed-up workfile name gen, etc.
				Expect(lom.ParsedFQN.IsLocal).To(BeFalse())
				Expect(lom.ParsedFQN.MpathInfo.Path).To(BeEquivalentTo(mpath))
				Expect(lom.ParsedFQN.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.ParsedFQN.Objname).To(BeEquivalentTo(testObject))
				Expect(lom.ParsedFQN.ContentType).To(BeEquivalentTo(fs.ObjectType))

				fs.Mountpaths.Enable(mpath2)
			})

			It("Should populate fields from a FQN", func() {
				lom := &cluster.LOM{T: tMock, Fqn: desiredCloudFqn}
				Expect(lom.Fill(0)).To(BeEmpty())
				Expect(lom.Bucket).To(BeEquivalentTo(bucketCloudA))
				Expect(lom.Objname).To(BeEquivalentTo(testObject))

				Expect(lom.Uname).To(BeEquivalentTo(cluster.Uname(bucketCloudA, testObject)))
				Expect(lom.Bislocal).To(BeFalse())

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
					mlom := &cluster.LOM{T: tMock, Fqn: fqn}
					Expect(mlom.Fill(0)).ToNot(BeEmpty())
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
					"mismatched bucket type 1",
					filepath.Join(mpath, fs.ObjectType, cmn.CloudBs, bucketLocalA, testObject),
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
			localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObjectName)

			It("should be able to mark file as Non-existent", func() {
				os.Remove(localFqn)
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(lom.Fill(cluster.LomFstat)).To(BeEmpty())

				//TODO: Keep in mind that having both is redundent and should be updated when one is removed
				Expect(lom.DoesNotExist).To(BeTrue())
				Expect(lom.Exists()).To(BeFalse())
			})

			It("should be able to mark file as Existent", func() {
				createTestFile(localFqn, testFileSize)
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(lom.Fill(cluster.LomFstat)).To(BeEmpty())

				Expect(lom.DoesNotExist).To(BeFalse())
				Expect(lom.Exists()).To(BeTrue())
				Expect(lom.Size).To(BeEquivalentTo(testFileSize))
			})
		})

		Describe("Atime", func() {
			desiredAtime := time.Unix(1500000000, 0)
			testObjectName := "foldr/test-obj.ext"

			It("should fetch atime for bucket with LRU disabled", func() {
				localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObjectName)
				createTestFile(localFqn, 0)
				Expect(os.Chtimes(localFqn, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())

				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(lom.Fill(cluster.LomAtime)).To(BeEmpty())

				Expect(lom.Atime).To(BeEquivalentTo(desiredAtime))
				Expect(lom.Atimestr).To(BeEquivalentTo(desiredAtime.Format(cmn.RFC822)))
			})
			It("should fetch atime for bucket with LRU enabled", func() {
				setupMockRunner()
				defer teardownMockRunner()

				localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
				createTestFile(localFqn, 0)
				Expect(os.Chtimes(localFqn, desiredAtime, desiredAtime)).ShouldNot(HaveOccurred())
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}

				Expect(lom.Fill(cluster.LomAtime)).To(BeEmpty())

				Expect(lom.Atime).To(BeEquivalentTo(desiredAtime))
				Expect(lom.Atimestr).To(BeEquivalentTo(desiredAtime.Format(cmn.RFC822)))
			})
		})

		Describe("CKSum", func() {
			testFileSize := 456
			testObjectName := "cksum-foldr/test-obj.ext"
			//Bucket needs to have checksum enabled
			localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)

			It("should ignore if bucket checksum is none", func() {
				testObject := "foldr/test-obj.ext"
				noneFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

				lom := &cluster.LOM{T: tMock, Fqn: noneFqn}
				Expect(lom.Fill(cluster.LomCksum | cluster.LomCksumMissingRecomp)).To(BeEmpty())

				Expect(lom.Nhobj).To(BeNil())
			})

			It("should not retrieve cksum when it was not previously computed", func() {
				createTestFile(localFqn, testFileSize)
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}

				Expect(lom.Fill(cluster.LomCksum)).To(BeEmpty())
				Expect(lom.Nhobj).To(BeNil())
			})

			It("should be able to verify checksum", func() {
				createTestFile(localFqn, testFileSize)
				expectedChecksum := getTestFileHash(localFqn)
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(fs.SetXattr(lom.Fqn, cmn.XattrXXHash, []byte(expectedChecksum))).To(BeEmpty())

				Expect(lom.Fill(cluster.LomCksum | cluster.LomCksumPresentRecomp)).To(BeEmpty())

				_, val := lom.Nhobj.Get()
				Expect(val).To(BeEquivalentTo(expectedChecksum))
				Expect(lom.Badchecksum).To(BeFalse())
			})

			It("should be able to detect bad checksum", func() {
				createTestFile(localFqn, testFileSize)
				badChecksum := "EA5EACE"
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}
				Expect(fs.SetXattr(lom.Fqn, cmn.XattrXXHash, []byte(badChecksum))).To(BeEmpty())

				Expect(lom.Fill(cluster.LomCksum | cluster.LomCksumPresentRecomp)).ToNot(BeEmpty())

				_, val := lom.Nhobj.Get()
				Expect(val).To(BeEquivalentTo(badChecksum))
				Expect(lom.Badchecksum).To(BeTrue())
			})

			It("should be able to calculate and store checksum", func() {
				createTestFile(localFqn, testFileSize)
				expectedHash := getTestFileHash(localFqn)
				lom := &cluster.LOM{T: tMock, Fqn: localFqn}

				Expect(lom.Fill(cluster.LomCksum | cluster.LomCksumMissingRecomp)).To(BeEmpty())

				_, lomVal := lom.Nhobj.Get()
				Expect(lomVal).To(BeEquivalentTo(expectedHash))
				xattrVal, _ := fs.GetXattr(lom.Fqn, cmn.XattrXXHash)
				Expect(xattrVal).To(BeEquivalentTo(expectedHash))
			})
		})

		Describe("Version", func() {
			testObject := "foldr/test-obj.ext"
			desiredVersion := "9001"
			localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

			It("should be able to get version", func() {
				createTestFile(localFqn, 0)
				Expect(fs.SetXattr(localFqn, cmn.XattrVersion, []byte(desiredVersion))).To(BeEmpty())

				lom := &cluster.LOM{T: tMock, Fqn: localFqn}

				Expect(lom.Fill(cluster.LomVersion)).To(BeEmpty())
				Expect(lom.Version).To(BeEquivalentTo(desiredVersion))
			})
		})

		Describe("LomCopy", func() {

			testFileSize := 123
			testObject := "foldr/test-obj.ext"
			localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)
			copyFqn := filepath.Join(mpath2, fs.ObjectType, cmn.LocalBs, bucketLocalA, testObject)

			It("should be able to get copy", func() {
				createTestFile(localFqn, testFileSize)
				Expect(fs.SetXattr(localFqn, cmn.XattrCopies, []byte(copyFqn))).To(BeEmpty())

				lom := &cluster.LOM{T: tMock, Fqn: localFqn}

				Expect(lom.Fill(cluster.LomCopy)).To(BeEmpty())
				Expect(lom.CopyFQN).To(BeEquivalentTo(copyFqn))
			})

		})

	})

	Describe("Copy Functions", func() {
		testObjectName := "foldr/test-obj.ext"
		localFqn := filepath.Join(mpath, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
		copyFqn := filepath.Join(mpath2, fs.ObjectType, cmn.LocalBs, bucketLocalB, testObjectName)
		testFileSize := 101
		desiredVersion := "9002"

		if _, hrw, _ := cluster.ResolveFQN(localFqn, tMock.GetBowner()); hrw != localFqn {
			localFqn, copyFqn = copyFqn, localFqn
		}

		prepareLomWithCopy := func(setXCopy bool) (lom *cluster.LOM) {
			//Prepares a basic lom with a copy
			createTestFile(localFqn, testFileSize)
			Expect(fs.SetXattr(localFqn, cmn.XattrVersion, []byte(desiredVersion))).To(BeEmpty())
			lom = &cluster.LOM{T: tMock, Fqn: localFqn}
			Expect(lom.Fill(cluster.LomVersion | cluster.LomCksum | cluster.LomCksumMissingRecomp)).To(BeEmpty())

			Expect(lom.CopyObject(copyFqn, make([]byte, testFileSize))).ShouldNot(HaveOccurred())
			_, err := os.Stat(copyFqn)
			Expect(os.IsNotExist(err)).To(BeFalse())

			if setXCopy {
				Expect(lom.SetXcopy(copyFqn)).Should(BeEmpty())
			}
			return
		}

		Describe("CopyObject", func() {
			It("Should successfully copy the object", func() {
				prepareLomWithCopy(false)
				expectedHash := getTestFileHash(localFqn)

				//Check copy created
				var xattr []byte
				xattr, _ = fs.GetXattr(copyFqn, cmn.XattrVersion)
				Expect(string(xattr)).To(BeEquivalentTo(desiredVersion))
				xattr, _ = fs.GetXattr(copyFqn, cmn.XattrXXHash)
				Expect(string(xattr)).To(BeEquivalentTo(expectedHash))

				//Check copy contents are corrrect
				Expect(getTestFileHash(copyFqn)).To(BeEquivalentTo(expectedHash))
			})
		})

		Describe("SetXcopy", func() {
			It("Should corectly set Xattributes", func() {
				lom := prepareLomWithCopy(true)

				//Check copy set
				var xattr []byte
				xattr, _ = fs.GetXattr(copyFqn, cmn.XattrCopies)
				Expect(string(xattr)).To(BeEquivalentTo(localFqn))
				xattr, _ = fs.GetXattr(localFqn, cmn.XattrCopies)
				Expect(string(xattr)).To(BeEquivalentTo(copyFqn))

				Expect(lom.CopyFQN).To(BeEquivalentTo(copyFqn))

				//Check msic copy data
				lomCopy := &cluster.LOM{T: tMock, Fqn: copyFqn}
				Expect(lomCopy.Fill(cluster.LomVersion | cluster.LomCksum | cluster.LomCksumMissingRecomp | cluster.LomCopy)).To(BeEmpty())

				Expect(lomCopy.HrwFQN).To(BeEquivalentTo(lom.HrwFQN))
				Expect(lom.IsCopy()).To(BeFalse())
				Expect(lom.HasCopy()).To(BeTrue())
				Expect(lomCopy.IsCopy()).To(BeTrue())
				Expect(lomCopy.HasCopy()).To(BeFalse())

			})
		})

		Describe("DelCopy", func() {
			It("Should be able to delete the copy", func() {
				lom := prepareLomWithCopy(true)

				Expect(lom.DelCopy()).To(BeEmpty())
				_, err := os.Stat(copyFqn)
				Expect(os.IsNotExist(err)).To(BeTrue())
			})
		})
	})

})

//
// HELPERS
//
func setupMockRunner() {
	tMock.r = atime.NewRunner(fs.Mountpaths, ios.NewIostatRunner())
	go tMock.r.Run()
}
func teardownMockRunner() {
	tMock.r.Stop(errors.New(""))
	tMock.r = nil
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

//
// MOCKS
//
type bownerMock struct{ cluster.BMD }

func (r bownerMock) Get() *cluster.BMD { return &r.BMD }

type targetMock struct {
	b bownerMock
	r *atime.Runner
}

func (n targetMock) IsRebalancing() bool           { return false }
func (n targetMock) RunLRU()                       {}
func (n targetMock) PrefetchQueueLen() int         { return 0 }
func (n targetMock) Prefetch()                     {}
func (n targetMock) GetBowner() cluster.Bowner     { return n.b }
func (n targetMock) FSHC(err error, path string)   {}
func (n targetMock) GetAtimeRunner() *atime.Runner { return n.r }
func (n targetMock) GetMem2() *memsys.Mem2         { return memsys.Init() }
