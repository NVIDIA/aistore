// Package space_test is a unit test for the package.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package space_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/space"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact/xreg"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	initialDiskUsagePct  = 0.9
	hwm                  = 80
	lwm                  = 50
	numberOfCreatedFiles = 45
	fileSize             = 10 * cos.MiB
	blockSize            = cos.KiB
	basePath             = "/tmp/space-tests"
	bucketName           = "space-bck"
	bucketNameAnother    = bucketName + "-another"
)

type fileMetadata struct {
	name string
	size int64
}

var gT *testing.T

func TestEvictCleanup(t *testing.T) {
	xreg.Init()
	hk.TestInit()
	cos.InitShortID(0)

	RegisterFailHandler(Fail)
	gT = t
	RunSpecs(t, t.Name())
}

var _ = Describe("space evict/cleanup tests", func() {
	Describe("Run", func() {
		var (
			filesPath  string
			fpAnother  string
			bckAnother cmn.Bck
		)

		BeforeEach(func() {
			initConfig()
			createAndAddMountpath(basePath)
			core.T = newTargetLRUMock()
			availablePaths := fs.GetAvail()
			bck := cmn.Bck{Name: bucketName, Provider: apc.AIS, Ns: cmn.NsGlobal}
			bckAnother = cmn.Bck{Name: bucketNameAnother, Provider: apc.AIS, Ns: cmn.NsGlobal}
			filesPath = availablePaths[basePath].MakePathCT(&bck, fs.ObjectType)
			fpAnother = availablePaths[basePath].MakePathCT(&bckAnother, fs.ObjectType)
			cos.CreateDir(filesPath)
			cos.CreateDir(fpAnother)
		})

		AfterEach(func() {
			os.RemoveAll(basePath)
		})

		Describe("evict files", func() {
			var ini *space.IniLRU
			BeforeEach(func() {
				ini = newIniLRU()
			})
			It("should not fail when there are no files", func() {
				space.RunLRU(ini)
			})

			It("should evict correct number of files", func() {
				if testing.Short() {
					Skip("skipping in short mode")
				}
				saveRandomFiles(filesPath, numberOfCreatedFiles)

				space.RunLRU(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				numberOfFilesLeft := len(files)

				// too few files evicted
				Expect(float64(numberOfFilesLeft) / numberOfCreatedFiles * initialDiskUsagePct).To(BeNumerically("<=", 0.01*lwm))
				// to many files evicted
				Expect(float64(numberOfFilesLeft+1) / numberOfCreatedFiles * initialDiskUsagePct).To(BeNumerically(">", 0.01*lwm))
			})

			It("should evict the oldest files", func() {
				const numberOfFiles = 6

				ini.GetFSStats = getMockGetFSStats(numberOfFiles)

				oldFiles := []fileMetadata{
					{getRandomFileName(3), fileSize},
					{getRandomFileName(4), fileSize},
					{getRandomFileName(5), fileSize},
				}
				saveRandomFilesWithMetadata(filesPath, oldFiles)
				time.Sleep(1 * time.Second)
				saveRandomFiles(filesPath, 3)

				space.RunLRU(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(3))

				oldFilesNames := namesFromFilesMetadatas(oldFiles)
				for _, name := range files {
					Expect(cos.StringInSlice(name.Name(), oldFilesNames)).To(BeFalse())
				}
			})

			It("should evict files of different sizes", func() {
				const totalSize = 32 * cos.MiB
				if testing.Short() {
					Skip("skipping in short mode")
				}

				ini.GetFSStats = func(string) (blocks, bavail uint64, bsize int64, err error) {
					bsize = blockSize
					btaken := uint64(totalSize / blockSize)
					blocks = uint64(float64(btaken) / initialDiskUsagePct)
					bavail = blocks - btaken
					return
				}

				// files sum up to 32Mb
				files := []fileMetadata{
					{getRandomFileName(0), int64(4 * cos.MiB)},
					{getRandomFileName(1), int64(16 * cos.MiB)},
					{getRandomFileName(2), int64(4 * cos.MiB)},
					{getRandomFileName(3), int64(8 * cos.MiB)},
				}
				saveRandomFilesWithMetadata(filesPath, files)

				// To go under lwm (50%), LRU should evict the oldest files until <=50% reached
				// Those files are a 4MB file and a 16MB file
				space.RunLRU(ini)

				filesLeft, err := os.ReadDir(filesPath)
				Expect(len(filesLeft)).To(Equal(2))
				Expect(err).NotTo(HaveOccurred())

				correctFilenamesLeft := namesFromFilesMetadatas(files[2:])
				for _, name := range filesLeft {
					Expect(cos.StringInSlice(name.Name(), correctFilenamesLeft)).To(BeTrue())
				}
			})

			It("should evict only files from requested bucket [ignores LRU prop]", func() {
				if testing.Short() {
					Skip("skipping in short mode")
				}
				saveRandomFiles(fpAnother, numberOfCreatedFiles)
				saveRandomFiles(filesPath, numberOfCreatedFiles)

				ini.Buckets = []cmn.Bck{bckAnother}
				ini.Force = true // Ignore LRU enabled
				space.RunLRU(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				filesAnother, err := os.ReadDir(fpAnother)
				Expect(err).NotTo(HaveOccurred())

				numFilesLeft := len(files)
				numFilesLeftAnother := len(filesAnother)

				// files not evicted from bucket
				Expect(numFilesLeft).To(BeNumerically("==", numberOfCreatedFiles))

				// too few files evicted
				Expect(float64(numFilesLeftAnother) / numberOfCreatedFiles * initialDiskUsagePct).To(BeNumerically("<=", 0.01*lwm))
				// to many files evicted
				Expect(float64(numFilesLeftAnother+1) / numberOfCreatedFiles * initialDiskUsagePct).To(BeNumerically(">", 0.01*lwm))
			})
		})

		Describe("not evict files", func() {
			var ini *space.IniLRU
			BeforeEach(func() {
				ini = newIniLRU()
			})
			It("should do nothing when disk usage is below hwm", func() {
				const numberOfFiles = 4
				config := cmn.GCO.BeginUpdate()
				config.Space.HighWM = 95
				config.Space.LowWM = 40
				cmn.GCO.CommitUpdate(config)

				ini.GetFSStats = getMockGetFSStats(numberOfFiles)

				saveRandomFiles(filesPath, numberOfFiles)

				space.RunLRU(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})

			It("should do nothing if dontevict time was not reached", func() {
				const numberOfFiles = 6
				config := cmn.GCO.BeginUpdate()
				config.LRU.DontEvictTime = cos.Duration(5 * time.Minute)
				cmn.GCO.CommitUpdate(config)

				ini.GetFSStats = getMockGetFSStats(numberOfFiles)

				saveRandomFiles(filesPath, numberOfFiles)

				space.RunLRU(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})

			It("should not evict if LRU disabled and force is false", func() {
				saveRandomFiles(fpAnother, numberOfCreatedFiles)

				ini.Buckets = []cmn.Bck{bckAnother} // bckAnother has LRU disabled
				space.RunLRU(ini)

				filesAnother, err := os.ReadDir(fpAnother)
				Expect(err).NotTo(HaveOccurred())

				numFilesLeft := len(filesAnother)
				Expect(numFilesLeft).To(BeNumerically("==", numberOfCreatedFiles))
			})
		})

		Describe("cleanup 'deleted'", func() {
			var ini *space.IniCln
			BeforeEach(func() {
				ini = newInitStoreCln()
			})
			It("should remove all deleted items", func() {
				var (
					availablePaths = fs.GetAvail()
					mi             = availablePaths[basePath]
				)

				saveRandomFiles(filesPath, 10)
				Expect(filesPath).To(BeADirectory())

				err := mi.MoveToDeleted(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(filesPath).NotTo(BeADirectory())

				files, err := os.ReadDir(mi.DeletedRoot())
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(1))

				space.RunCleanup(ini)

				files, err = os.ReadDir(mi.DeletedRoot())
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(0))
			})
		})
	})
})

//
// test helpers & utilities
//

func namesFromFilesMetadatas(fileMetadata []fileMetadata) []string {
	result := make([]string, len(fileMetadata))
	for i, file := range fileMetadata {
		result[i] = file.name
	}
	return result
}

func mockGetFSUsedPercentage(string) (usedPrecentage int64, _ bool) {
	return int64(initialDiskUsagePct * 100), true
}

func getMockGetFSStats(currentFilesNum int) func(string) (uint64, uint64, int64, error) {
	currDiskUsage := initialDiskUsagePct
	return func(string) (blocks, bavail uint64, bsize int64, err error) {
		bsize = blockSize
		btaken := uint64(currentFilesNum * fileSize / blockSize)
		blocks = uint64(float64(btaken) / currDiskUsage) // gives around currDiskUsage of virtual disk usage
		bavail = blocks - btaken
		return
	}
}

func newTargetLRUMock() *mock.TargetMock {
	// Bucket owner mock, required for LOM
	var (
		bmdMock = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketName, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
					LRU:    cmn.LRUConf{Enabled: true},
					Access: apc.AccessAll,
					BID:    0xa7b8c1d2,
				},
			),
			meta.NewBck(
				bucketNameAnother, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
					LRU:    cmn.LRUConf{Enabled: false},
					Access: apc.AccessAll,
					BID:    0xf4e3d2c1,
				},
			),
		)
		tMock = mock.NewTarget(bmdMock)
	)
	return tMock
}

func newIniLRU() *space.IniLRU {
	xlru := &space.XactLRU{}
	xlru.InitBase(cos.GenUUID(), apc.ActLRU, nil)
	return &space.IniLRU{
		Xaction:             xlru,
		Config:              cmn.GCO.Get(),
		StatsT:              mock.NewStatsTracker(),
		GetFSUsedPercentage: mockGetFSUsedPercentage,
		GetFSStats:          getMockGetFSStats(numberOfCreatedFiles),
	}
}

func newInitStoreCln() *space.IniCln {
	xcln := &space.XactCln{}
	xcln.InitBase(cos.GenUUID(), apc.ActStoreCleanup, nil)
	return &space.IniCln{
		Xaction: xcln,
		Config:  cmn.GCO.Get(),
		StatsT:  mock.NewStatsTracker(),
	}
}

func initConfig() {
	config := cmn.GCO.BeginUpdate()
	config.LRU.DontEvictTime = 0
	config.Space.HighWM = hwm
	config.Space.LowWM = lwm
	config.LRU.Enabled = true
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)
}

func createAndAddMountpath(path string) {
	cos.CreateDir(path)
	fs.TestNew(nil)
	fs.Add(path, "daeID")

	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}, true)
}

func getRandomFileName(fileCounter int) string {
	return fmt.Sprintf("%v-%v.txt", trand.String(13), fileCounter)
}

func saveRandomFile(filename string, size int64) {
	buff := make([]byte, size)
	_, err := cos.SaveReader(filename, rand.Reader, buff, cos.ChecksumNone, size)
	Expect(err).NotTo(HaveOccurred())
	lom := &core.LOM{}
	err = lom.InitFQN(filename, nil)
	Expect(err).NotTo(HaveOccurred())
	lom.SetSize(size)
	lom.IncVersion()
	lom.SetAtimeUnix(time.Now().UnixNano())
	Expect(lom.Persist()).NotTo(HaveOccurred())
}

func saveRandomFilesWithMetadata(filesPath string, files []fileMetadata) {
	for _, file := range files {
		saveRandomFile(path.Join(filesPath, file.name), file.size)
	}
}

// Saves random bytes to a file with random name.
// timestamps and names are not increasing in the same manner
func saveRandomFiles(filesPath string, filesNumber int) {
	for i := 0; i < filesNumber; i++ {
		saveRandomFile(path.Join(filesPath, getRandomFileName(i)), fileSize)
	}
}
