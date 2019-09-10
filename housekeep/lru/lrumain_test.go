// Package lru provides least recently used cache replacement policy for stored objects
// and serves as a generic garbage-collection mechanism for orhaned workfiles.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package lru

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLRUMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LRU Suite")
}

const (
	initialDiskUsagePct  = 0.9
	hwm                  = 80
	lwm                  = 50
	numberOfCreatedFiles = 45
	fileSize             = 10 * cmn.MiB
	blockSize            = cmn.KiB
	basePath             = "/tmp/lru-tests/"
	bucketName           = "lru-bck"
	filesPath            = basePath + fs.ObjectType + "/local/" + bucketName
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

func mockGetFSUsedPercentage(path string) (usedPrecentage int64, ok bool) {
	return int64(initialDiskUsagePct * 100), true
}

func getMockGetFSStats(currentFilesNum int) func(string) (uint64, uint64, int64, error) {
	currDiskUsage := initialDiskUsagePct
	return func(string) (blocks uint64, bavail uint64, bsize int64, err error) {
		bsize = blockSize
		btaken := uint64(currentFilesNum * fileSize / blockSize)
		blocks = uint64(float64(btaken) / currDiskUsage) // gives around currDiskUsage of virtual disk usage
		bavail = blocks - btaken
		return
	}
}

func newTargetLRUMock() *cluster.TargetMock {
	// Bucket owner mock, required for LOM
	bo := cluster.BownerMock{BMD: cluster.BMD{
		LBmap: map[string]*cmn.BucketProps{
			bucketName: {
				Cksum: cmn.CksumConf{Type: cmn.ChecksumNone},
				LRU:   cmn.LRUConf{Enabled: true},
			},
		},
	}}

	target := cluster.NewTargetMock(bo)
	return target
}

func newInitLRU(t cluster.Target) *InitLRU {
	xlru := &Xaction{}
	return &InitLRU{
		Xlru:                xlru,
		Statsif:             &statsLRUMock{},
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
	config.LRU.EvictAISBuckets = true
	cmn.GCO.CommitUpdate(config)
}

func createAndAddMountpath(path string) {
	cmn.CreateDir(path)
	fs.InitMountedFS()
	fs.Mountpaths.Add(path)

	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})
}

func getRandomFileName(fileCounter int) string {
	randomGen := mrand.New(mrand.NewSource(time.Now().UTC().UnixNano()))
	return fmt.Sprintf("%v-%v.txt", tutils.FastRandomFilename(randomGen, 13), fileCounter)
}

func saveRandomFile(t cluster.Target, filename string, size int64) {
	buff := make([]byte, size)
	_, err := cmn.SaveReader(filename, rand.Reader, buff, false, size)
	Expect(err).NotTo(HaveOccurred())
	lom, errstr := cluster.LOM{T: t, FQN: filename}.Init("")
	Expect(errstr).NotTo(HaveOccurred())
	lom.SetSize(size)
	lom.IncObjectVersion()
	Expect(lom.Persist()).NotTo(HaveOccurred())
}

func saveRandomFilesWithMetadata(t cluster.Target, files []fileMetadata) {
	for _, file := range files {
		saveRandomFile(t, path.Join(filesPath, file.name), file.size)
	}
}

// Saves random bytes to a file with random name.
// timestamps and names are not increasing in the same manner
func saveRandomFiles(t cluster.Target, filesNumber int) {
	for i := 0; i < filesNumber; i++ {
		saveRandomFile(t, path.Join(filesPath, getRandomFileName(i)), fileSize)
	}
}

var _ = Describe("LRU tests", func() {
	Describe("InitAndRun", func() {

		var t *cluster.TargetMock
		var ini *InitLRU

		BeforeEach(func() {
			initConfig()
			createAndAddMountpath(basePath)
			cmn.CreateDir(filesPath)
			t = newTargetLRUMock()
			ini = newInitLRU(t)
		})

		AfterEach(func() {
			os.RemoveAll(basePath)
		})

		Describe("evict files", func() {
			It("should not fail when there are no files", func() {
				InitAndRun(ini)
			})

			It("should evict correct number of files", func() {
				saveRandomFiles(t, numberOfCreatedFiles)

				InitAndRun(ini)

				files, err := ioutil.ReadDir(filesPath)
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
				saveRandomFilesWithMetadata(t, oldFiles)
				time.Sleep(1 * time.Second)
				saveRandomFiles(t, 3)

				InitAndRun(ini)

				files, err := ioutil.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(3))

				oldFilesNames := namesFromFilesMetadatas(oldFiles)
				for _, name := range files {
					Expect(cmn.StringInSlice(name.Name(), oldFilesNames)).To(BeFalse())
				}
			})

			It("should evict files of different sizes", func() {
				const totalSize = 32 * cmn.MiB

				ini.GetFSStats = func(string) (blocks uint64, bavail uint64, bsize int64, err error) {
					bsize = blockSize
					btaken := uint64(totalSize / blockSize)
					blocks = uint64(float64(btaken) / initialDiskUsagePct)
					bavail = blocks - btaken
					return
				}

				// files sum up to 32Mb
				files := []fileMetadata{
					{getRandomFileName(0), int64(4 * cmn.MiB)},
					{getRandomFileName(1), int64(16 * cmn.MiB)},
					{getRandomFileName(2), int64(4 * cmn.MiB)},
					{getRandomFileName(3), int64(8 * cmn.MiB)},
				}
				saveRandomFilesWithMetadata(t, files)

				// To go under lwm (50%), LRU should evict the oldest files until <=50% reached
				// Those files are 4Mb file and 16Mb file
				InitAndRun(ini)

				filesLeft, err := ioutil.ReadDir(filesPath)
				Expect(len(filesLeft)).To(Equal(2))
				Expect(err).NotTo(HaveOccurred())

				correctFilenamesLeft := namesFromFilesMetadatas(files[2:])
				for _, name := range filesLeft {
					Expect(cmn.StringInSlice(name.Name(), correctFilenamesLeft)).To(BeTrue())
				}
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

				saveRandomFiles(t, numberOfFiles)

				InitAndRun(ini)

				files, err := ioutil.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})

			It("should do nothing if dontevict time was not reached", func() {
				const numberOfFiles = 6
				config := cmn.GCO.BeginUpdate()
				config.LRU.DontEvictTime = 5 * time.Minute
				cmn.GCO.CommitUpdate(config)

				ini.GetFSStats = getMockGetFSStats(numberOfFiles)

				saveRandomFiles(t, numberOfFiles)

				InitAndRun(ini)

				files, err := ioutil.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})
		})
	})
})

// MOCK TYPES
// TODO: Maybe this should be in stats_mock.go

type statsLRUMock struct{}

func (s *statsLRUMock) StartedUp() bool                        { return true }
func (s *statsLRUMock) Add(name string, val int64)             {}
func (s *statsLRUMock) Get(name string) int64                  { return 0 }
func (s *statsLRUMock) AddErrorHTTP(method string, val int64)  {}
func (s *statsLRUMock) AddMany(namedVal64 ...stats.NamedVal64) {}
func (s *statsLRUMock) Register(name string, kind string)      {}
