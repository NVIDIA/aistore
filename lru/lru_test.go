// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orphaned workfiles.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package lru_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLRUMain(t *testing.T) {
	RegisterFailHandler(Fail)
	go hk.DefaultHK.Run()
	RunSpecs(t, "LRU Suite")
}

const (
	initialDiskUsagePct  = 0.9
	hwm                  = 80
	lwm                  = 50
	numberOfCreatedFiles = 45
	fileSize             = 10 * cos.MiB
	blockSize            = cos.KiB
	basePath             = "/tmp/lru-tests"
	bucketName           = "lru-bck"
	bucketNameAnother    = bucketName + "-another"
)

type fileMetadata struct {
	name string
	size int64
}

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

func newTargetLRUMock() *cluster.TargetMock {
	// Bucket owner mock, required for LOM
	var (
		bmdMock = cluster.NewBaseBownerMock(
			cluster.NewBck(
				bucketName, cmn.ProviderAIS, cmn.NsGlobal,
				&cmn.BucketProps{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
					LRU:    cmn.LRUConf{Enabled: true},
					Access: cmn.AccessAll,
					BID:    0xa7b8c1d2,
				},
			),
			cluster.NewBck(
				bucketNameAnother, cmn.ProviderAIS, cmn.NsGlobal,
				&cmn.BucketProps{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
					LRU:    cmn.LRUConf{Enabled: false},
					Access: cmn.AccessAll,
					BID:    0xf4e3d2c1,
				},
			),
		)
		tMock = cluster.NewTargetMock(bmdMock)
	)
	return tMock
}

func newInitLRU(t cluster.Target) *lru.InitLRU {
	xlru := &lru.Xaction{
		XactDemandBase: *xaction.NewXDB(cos.GenUUID(), cmn.ActLRU, nil, 2*time.Second, time.Second),
		Renewed:        make(chan struct{}, 8),
	}
	xlru.InitIdle()
	return &lru.InitLRU{
		Xaction:             xlru,
		StatsT:              stats.NewTrackerMock(),
		T:                   t,
		GetFSUsedPercentage: mockGetFSUsedPercentage,
		GetFSStats:          getMockGetFSStats(numberOfCreatedFiles),
	}
}

func initConfig() {
	config := cmn.GCO.BeginUpdate()
	config.LRU.DontEvictTime = 0
	config.LRU.HighWM = hwm
	config.LRU.LowWM = lwm
	config.LRU.Enabled = true
	cmn.GCO.CommitUpdate(config)
}

func createAndAddMountpath(path string) {
	cos.CreateDir(path)
	fs.Init()
	fs.Add(path, "daeID")

	fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})
}

func getRandomFileName(fileCounter int) string {
	return fmt.Sprintf("%v-%v.txt", cos.RandString(13), fileCounter)
}

func saveRandomFile(filename string, size int64) {
	buff := make([]byte, size)
	_, err := cos.SaveReader(filename, rand.Reader, buff, cos.ChecksumNone, size, "")
	Expect(err).NotTo(HaveOccurred())
	lom := &cluster.LOM{FQN: filename}
	err = lom.Init(cmn.Bck{})
	Expect(err).NotTo(HaveOccurred())
	lom.SetSize(size)
	lom.IncVersion()
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

var _ = Describe("LRU tests", func() {
	cos.InitShortID(0)
	Describe("Run", func() {
		var (
			t   *cluster.TargetMock
			ini *lru.InitLRU

			filesPath  string
			fpAnother  string
			bckAnother cmn.Bck
		)

		BeforeEach(func() {
			initConfig()
			createAndAddMountpath(basePath)
			t = newTargetLRUMock()
			ini = newInitLRU(t)

			mpaths, _ := fs.Get()
			bck := cmn.Bck{Name: bucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
			bckAnother = cmn.Bck{Name: bucketNameAnother, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal}
			filesPath = mpaths[basePath].MakePathCT(bck, fs.ObjectType)
			fpAnother = mpaths[basePath].MakePathCT(bckAnother, fs.ObjectType)
			cos.CreateDir(filesPath)
			cos.CreateDir(fpAnother)
		})

		AfterEach(func() {
			os.RemoveAll(basePath)
		})

		Describe("evict files", func() {
			It("should not fail when there are no files", func() {
				lru.Run(ini)
			})

			It("should evict correct number of files", func() {
				saveRandomFiles(filesPath, numberOfCreatedFiles)

				lru.Run(ini)

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

				lru.Run(ini)

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
				// Those files are 4Mb file and 16Mb file
				lru.Run(ini)

				filesLeft, err := os.ReadDir(filesPath)
				Expect(len(filesLeft)).To(Equal(2))
				Expect(err).NotTo(HaveOccurred())

				correctFilenamesLeft := namesFromFilesMetadatas(files[2:])
				for _, name := range filesLeft {
					Expect(cos.StringInSlice(name.Name(), correctFilenamesLeft)).To(BeTrue())
				}
			})

			It("should evict only files from requested bucket [ignores LRU prop]", func() {
				saveRandomFiles(fpAnother, numberOfCreatedFiles)
				saveRandomFiles(filesPath, numberOfCreatedFiles)

				ini.Buckets = []cmn.Bck{bckAnother}
				ini.Force = true // Ignore LRU enabled
				lru.Run(ini)

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
			It("should do nothing when disk usage is below hwm", func() {
				const numberOfFiles = 4
				config := cmn.GCO.BeginUpdate()
				config.LRU.HighWM = 95
				config.LRU.LowWM = 40
				cmn.GCO.CommitUpdate(config)

				ini.GetFSStats = getMockGetFSStats(numberOfFiles)

				saveRandomFiles(filesPath, numberOfFiles)

				lru.Run(ini)

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

				lru.Run(ini)

				files, err := os.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})

			It("should not evict if LRU disabled and force is false", func() {
				saveRandomFiles(fpAnother, numberOfCreatedFiles)

				ini.Buckets = []cmn.Bck{bckAnother} // bckAnother has LRU disabled
				lru.Run(ini)

				filesAnother, err := os.ReadDir(fpAnother)
				Expect(err).NotTo(HaveOccurred())

				numFilesLeft := len(filesAnother)
				Expect(numFilesLeft).To(BeNumerically("==", numberOfCreatedFiles))
			})
		})

		Describe("evict trash directory", func() {
			It("should totally evict trash directory", func() {
				var (
					mpaths, _ = fs.Get()
					mpath     = mpaths[basePath]
				)

				saveRandomFiles(filesPath, 10)
				Expect(filesPath).To(BeADirectory())

				err := mpath.MoveToTrash(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(filesPath).NotTo(BeADirectory())

				files, err := os.ReadDir(mpath.MakePathTrash())
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(1))

				lru.Run(ini)

				files, err = os.ReadDir(mpath.MakePathTrash())
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(0))
			})
		})
	})
})
