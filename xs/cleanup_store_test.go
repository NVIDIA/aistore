// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xreg"
	"github.com/NVIDIA/aistore/xs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	xreg.Init()
	hk.TestInit()
}

func TestStoreCleanupMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Cleanup Suite")
}

const (
	fileSize          = 10 * cos.MiB
	basePath          = "/tmp/stgclean-tests"
	bucketName        = "stgclean-bck"
	bucketNameAnother = bucketName + "-another"
)

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

func newInitStoreCln(t cluster.Target) *xs.InitStoreCln {
	xstg := &xs.StoreClnXaction{}
	xstg.InitBase(cos.GenUUID(), cmn.ActLRU, nil)
	return &xs.InitStoreCln{
		Xaction: xstg,
		StatsT:  stats.NewTrackerMock(),
		T:       t,
	}
}

func initConfig() {
	config := cmn.GCO.BeginUpdate()
	config.LRU.DontEvictTime = 0
	cmn.GCO.CommitUpdate(config)
}

func createAndAddMountpath(path string) {
	cos.CreateDir(path)
	fs.New()
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

// Saves random bytes to a file with random name.
// timestamps and names are not increasing in the same manner
func saveRandomFiles(filesPath string, filesNumber int) {
	for i := 0; i < filesNumber; i++ {
		saveRandomFile(path.Join(filesPath, getRandomFileName(i)), fileSize)
	}
}

var _ = Describe("Storage cleanup tests", func() {
	cos.InitShortID(0)
	Describe("Run", func() {
		var (
			t   *cluster.TargetMock
			ini *xs.InitStoreCln

			filesPath  string
			fpAnother  string
			bckAnother cmn.Bck
		)

		BeforeEach(func() {
			initConfig()
			createAndAddMountpath(basePath)
			t = newTargetLRUMock()
			ini = newInitStoreCln(t)

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

				xs.RunStoreClean(ini)

				files, err = os.ReadDir(mpath.MakePathTrash())
				Expect(err).NotTo(HaveOccurred())
				Expect(len(files)).To(Equal(0))
			})
		})
	})
})
