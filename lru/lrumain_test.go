// Package lru provides atime-based least recently used cache replacement policy for stored objects
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

	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
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

func getMockGetFSStats(currentFilesNum int, currDiskUsage float64) func(string) (uint64, uint64, int64, error) {
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
			bucketName: &cmn.BucketProps{
				Cksum: cmn.CksumConf{Type: cmn.ChecksumNone},
				LRU:   cmn.LRUConf{Enabled: true},
			},
		},
	}}

	runner := atime.NewRunner(fs.Mountpaths, ios.NewIostatRunner())
	go runner.Run()

	target := cluster.NewTargetMock(bo)
	target.Atime = runner

	return target
}

func newInitLRU(t *cluster.TargetMock) *InitLRU {
	xlru := &xactMock{}
	return &InitLRU{
		Xlru:                xlru,
		Namelocker:          &nameLockerMock{},
		Statsif:             &statsLRUMock{},
		T:                   t,
		GetFSUsedPercentage: mockGetFSUsedPercentage,
		GetFSStats:          getMockGetFSStats(numberOfCreatedFiles, initialDiskUsagePct),
	}
}

func initConfig() {
	config := cmn.GCO.BeginUpdate()
	config.LRU.DontEvictTime = 0
	config.LRU.HighWM = hwm
	config.LRU.LowWM = lwm
	config.LRU.Enabled = true
	config.LRU.LocalBuckets = true
	cmn.GCO.CommitUpdate(config)
}

func createAndAddMountpath(path string) {
	cmn.CreateDir(path)
	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.Add(path)

	fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})
}

func getRandomFileName(fileCounter int) string {
	randomGen := mrand.New(mrand.NewSource(time.Now().UTC().UnixNano()))
	return fmt.Sprintf("%v-%v.txt", tutils.FastRandomFilename(randomGen, 13), fileCounter)
}

func saveRandomFile(filename string, size int64) {
	buff := make([]byte, size)
	err := cmn.SaveReader(filename, rand.Reader, buff, size)
	Expect(err).NotTo(HaveOccurred())
}

func saveRandomFilesWithMetadata(dirname string, files []fileMetadata) {
	for _, file := range files {
		saveRandomFile(path.Join(dirname, file.name), file.size)
	}
}

// Saves random bytes to a file with random name.
// timestamps and names are not increasing in the same manner
func saveRandomFiles(dirname string, filesNumber int, size int64) {
	for i := 0; i < filesNumber; i++ {
		saveRandomFile(path.Join(dirname, getRandomFileName(i)), size)
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
			t.Atime.Stop(nil)
		})

		Describe("evict files", func() {
			It("should not fail when there are no files", func() {
				InitAndRun(ini)
			})

			It("should evict correct number of files", func() {
				saveRandomFiles(filesPath, numberOfCreatedFiles, fileSize)

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

				ini.GetFSStats = getMockGetFSStats(numberOfFiles, initialDiskUsagePct)

				oldFiles := []fileMetadata{
					fileMetadata{getRandomFileName(3), fileSize},
					fileMetadata{getRandomFileName(4), fileSize},
					fileMetadata{getRandomFileName(5), fileSize},
				}
				saveRandomFilesWithMetadata(filesPath, oldFiles)
				time.Sleep(1 * time.Second)
				saveRandomFiles(filesPath, 3, fileSize)

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

				// files sum up to 16Mb
				files := []fileMetadata{
					{getRandomFileName(0), int64(4 * cmn.MiB)},
					{getRandomFileName(1), int64(16 * cmn.MiB)},
					{getRandomFileName(2), int64(4 * cmn.MiB)},
					{getRandomFileName(3), int64(8 * cmn.MiB)},
				}
				saveRandomFilesWithMetadata(filesPath, files)

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

				ini.GetFSStats = getMockGetFSStats(numberOfFiles, initialDiskUsagePct)

				saveRandomFiles(filesPath, numberOfFiles, fileSize)

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

				ini.GetFSStats = getMockGetFSStats(numberOfFiles, initialDiskUsagePct)

				saveRandomFiles(filesPath, numberOfFiles, fileSize)

				InitAndRun(ini)

				files, err := ioutil.ReadDir(filesPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(numberOfFiles))
			})
		})
	})
})

// MOCK TYPES

type nameLockerMock struct{}

func (n nameLockerMock) TryLock(uname string, exclusive bool) bool { return true }
func (n nameLockerMock) Lock(uname string, exclusive bool)         {}
func (n nameLockerMock) DowngradeLock(uname string)                {}
func (n nameLockerMock) Unlock(uname string, exclusive bool)       {}

// TODO: Maybe this should be in stats_mock.go

type statsLRUMock struct{}

func (s *statsLRUMock) Add(name string, val int64)             {}
func (s *statsLRUMock) AddErrorHTTP(method string, val int64)  {}
func (s *statsLRUMock) AddMany(namedVal64 ...stats.NamedVal64) {}
func (s *statsLRUMock) Register(name string, kind string)      {}

type xactMock struct{}

func (x *xactMock) ID() int64                          { return 0 }
func (x *xactMock) Kind() string                       { return "" }
func (x *xactMock) Bucket() string                     { return "" }
func (x *xactMock) StartTime(s ...time.Time) time.Time { return time.Now() }
func (x *xactMock) EndTime(e ...time.Time) time.Time   { return time.Now() }
func (x *xactMock) String() string                     { return "" }
func (x *xactMock) Abort()                             {}
func (x *xactMock) ChanAbort() <-chan struct{}         { return nil }
func (x *xactMock) Finished() bool                     { return false }
