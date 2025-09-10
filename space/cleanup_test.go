// Package space_test is a unit test for the package.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package space_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	tmpDir     = "/tmp/cleanup-test"
	numMpaths  = 3
	bucketName = "test-bucket"
)

func TestEvictCleanup(t *testing.T) {
	xreg.Init()
	hk.Init(false)
	cos.InitShortID(0)

	RegisterFailHandler(Fail)
	RunSpecs(t, t.Name())
}

var _ = Describe("AIStore content cleanup tests", func() {
	var (
		mpaths []string
		bck    cmn.Bck
		ini    *space.IniCln
	)

	BeforeEach(func() {
		// mountpaths
		mpaths = make([]string, 0, numMpaths)
		for range numMpaths {
			mpath := filepath.Join(tmpDir, "mpath", cos.GenTie())
			mpaths = append(mpaths, mpath)
			cos.CreateDir(mpath)
		}

		// init fs package
		fs.TestNew(mock.NewIOS())
		for _, mpath := range mpaths {
			_, err := fs.Add(mpath, "daeID")
			Expect(err).NotTo(HaveOccurred())
		}

		// register all content types
		fs.CSM.Reg(fs.ObjCT, &fs.ObjectContentRes{}, true)
		fs.CSM.Reg(fs.WorkCT, &fs.WorkContentRes{}, true)
		fs.CSM.Reg(fs.ChunkCT, &fs.ObjChunkContentRes{}, true)
		fs.CSM.Reg(fs.ChunkMetaCT, &fs.ChunkMetaContentRes{}, true)

		// bucket
		bck = cmn.Bck{Name: bucketName, Provider: apc.AIS, Ns: cmn.NsGlobal}

		// mock BMD, mock target
		bmd := mock.NewBaseBownerMock(
			meta.NewBck(
				bucketName, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{
					Cksum:  cmn.CksumConf{Type: cos.ChecksumNone},
					LRU:    cmn.LRUConf{Enabled: true},
					Access: apc.AccessAll,
					BID:    0xa7b8c1d2,
				},
			),
		)
		core.T = mock.NewTarget(bmd)

		// cluster config
		config := cmn.GCO.BeginUpdate()
		config.Space.DontCleanupTime = cos.Duration(2 * time.Hour)
		config.Space.CleanupWM = 65
		config.Space.BatchSize = cmn.GCBatchSizeMin // Use proper minimum batch size
		config.Log.Level = "3"
		cmn.GCO.CommitUpdate(config)

		// x-cleanup
		xcln := &space.XactCln{}
		xcln.InitBase(cos.GenUUID(), apc.ActStoreCleanup, "" /*ctlmsg*/, nil)
		ini = &space.IniCln{
			Xaction: xcln,
			StatsT:  mock.NewStatsTracker(),
			Args:    &xact.ArgsMsg{},
		}
	})

	AfterEach(func() {
		os.RemoveAll(tmpDir)
	})

	Describe("Misplaced object cleanup", func() {
		It("should remove old misplaced objects", func() {
			objectName := "misplaced-object.txt"

			// Create LOM for the object to find correct placement
			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			wrongMpath := findOtherMpath(lom.Mountpath())

			// Find wrong mountpath for misplacement

			// Create misplaced object
			wrongFQN := wrongMpath.MakePathFQN(&bck, fs.ObjCT, objectName)
			oldTime := time.Now().Add(-3 * time.Hour)
			createTestLOM(wrongFQN, 1024, oldTime)

			// Backdate
			err = os.Chtimes(wrongFQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			Expect(wrongFQN).To(BeAnExistingFile())

			// Run cleanup
			space.RunCleanup(ini)

			// Verify misplaced object was removed
			Expect(wrongFQN).NotTo(BeAnExistingFile())
		})

		It("should respect --keep-misplaced flag", func() {
			// Create misplaced object (old enough for removal)
			objectName := "flagged-misplaced.txt"
			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			wrongMpath := findOtherMpath(lom.Mountpath())
			wrongFQN := wrongMpath.MakePathFQN(&bck, fs.ObjCT, objectName)
			oldTime := time.Now().Add(-3 * time.Hour)
			createTestLOM(wrongFQN, 1024, oldTime)

			// ask to keep it
			ini.Args.Flags |= xact.FlagKeepMisplaced

			space.RunCleanup(ini)

			// misplaced object should exist
			Expect(wrongFQN).To(BeAnExistingFile())
		})

		It("should remove zero-size objects when flag is set", func() {
			objectName := "zero-size-object.txt"
			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			oldTime := time.Now().Add(-3 * time.Hour)
			createTestLOM(lom.FQN, 0, oldTime) // zero size object

			err = os.Chtimes(lom.FQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			ini.Args.Flags |= xact.FlagZeroSize

			space.RunCleanup(ini)

			Expect(lom.FQN).NotTo(BeAnExistingFile())
		})

		It("should preserve recent misplaced objects", func() {
			avail := fs.GetAvail()
			objectName := "recent-misplaced.txt"

			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			correctMpath := lom.Mountpath()
			var wrongMpath *fs.Mountpath
			for _, mp := range avail {
				if mp.Path != correctMpath.Path {
					wrongMpath = mp
					break
				}
			}

			// Force create LOM on wrong mountpath
			wrongFQN := wrongMpath.MakePathFQN(&bck, fs.ObjCT, objectName)
			misplacedLom := newBasicLom(wrongFQN, 512)
			createTestFile(wrongFQN, 512)

			// Set metadata and persist
			misplacedLom.SetSize(512)
			misplacedLom.IncVersion()
			err = persist(misplacedLom)
			Expect(err).NotTo(HaveOccurred())

			// Keep recent timestamp
			recentTime := time.Now().Add(-30 * time.Minute)
			err = os.Chtimes(wrongFQN, recentTime, recentTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			// Recent object should be preserved
			Expect(wrongFQN).To(BeAnExistingFile())
		})
	})

	Describe("Workfile cleanup", func() {
		It("should remove old workfiles", func() {
			objectName := "test-object-for-workfile.txt"

			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			workTag := "test-work-tag"
			workFQN := fs.CSM.Gen(lom, fs.WorkCT, workTag)

			// force different PID
			i := strings.LastIndexByte(workFQN, '.')
			workFQN = workFQN[:i] + ".123456789"

			createTestFile(workFQN, 256)

			// backdate workfile
			oldTime := time.Now().Add(-3 * time.Hour)
			err = os.Chtimes(workFQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			Expect(workFQN).To(BeAnExistingFile())

			space.RunCleanup(ini)

			// Old workfile should be removed
			Expect(workFQN).NotTo(BeAnExistingFile())
		})

		It("should preserve recent workfiles", func() {
			objectName := "recent-object-for-workfile.txt"

			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			workTag := "recent-work-tag"
			workFQN := fs.CSM.Gen(lom, fs.WorkCT, workTag)

			createTestFile(workFQN, 256)

			// Keep recent timestamp
			recentTime := time.Now().Add(-30 * time.Minute)
			err = os.Chtimes(workFQN, recentTime, recentTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			// Recent workfile should be preserved
			Expect(workFQN).To(BeAnExistingFile())
		})

		It("should remove old objects missing metadata", func() {
			avail := fs.GetAvail()
			mi := avail[mpaths[0]]

			objFQN := filepath.Join(mi.MakePathCT(&bck, fs.ObjCT), "missing-lmeta.bin")
			createTestFile(objFQN, 1024)

			// Backdate file so it’s beyond dont_cleanup_time
			old := time.Now().Add(-3 * time.Hour)
			Expect(os.Chtimes(objFQN, old, old)).To(Succeed())
			Expect(objFQN).To(BeAnExistingFile())

			space.RunCleanup(ini)

			// Should be removed as damaged/stale
			Expect(objFQN).NotTo(BeAnExistingFile())
		})

		It("should remove old objects with size/metadata mismatch", func() {
			avail := fs.GetAvail()
			mi := avail[mpaths[0]]

			fqn := filepath.Join(mi.MakePathCT(&bck, fs.ObjCT), "size-mismatch.bin")

			// write 1 KiB file on disk
			createTestFile(fqn, 1024)

			// persist lmeta claiming 4 KiB
			lom := newBasicLom(fqn)
			lom.SetSize(4 * 1024) // wrong
			lom.IncVersion()
			Expect(persist(lom)).To(Succeed())

			// make it old
			old := time.Now().Add(-3 * time.Hour)
			lom.SetAtimeUnix(old.UnixNano())
			Expect(persist(lom)).To(Succeed())
			Expect(os.Chtimes(fqn, old, old)).To(Succeed())

			Expect(fqn).To(BeAnExistingFile())

			space.RunCleanup(ini)

			// old + inconsistent ⇒ removed
			Expect(fqn).NotTo(BeAnExistingFile())
		})

		It("should remove old objects with corrupted metadata", func() {
			const xattrLOM = "user.ais.lom"
			avail := fs.GetAvail()
			mi := avail[mpaths[0]]

			fqn := filepath.Join(mi.MakePathCT(&bck, fs.ObjCT), "corrupt-xattr.bin")
			old := time.Now().Add(-3 * time.Hour)
			createTestLOM(fqn, 2000, old)

			// Corrupt object metadata
			err := fs.SetXattr(fqn, xattrLOM, []byte{0x00, 0xFF, 0x13, 0x37})
			Expect(err).NotTo(HaveOccurred())

			err = os.Chtimes(fqn, old, old)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)
			Expect(fqn).NotTo(BeAnExistingFile())
		})

	})

	Describe("Chunk cleanup", func() {
		It("should remove old orphaned chunks", func() {
			avail := fs.GetAvail()
			mpath := avail[mpaths[0]]

			// Create chunk directory
			chunkDir := mpath.MakePathCT(&bck, fs.ChunkCT)
			err := cos.CreateDir(chunkDir)
			Expect(err).NotTo(HaveOccurred())

			// Create orphaned chunk with proper naming (object.uploadID.partNumber)
			chunkFileName := "largefile." + cos.GenTie() + ".0001"
			chunkFQN := filepath.Join(chunkDir, chunkFileName)
			createTestFile(chunkFQN, 1024)

			// Backdate chunk
			oldTime := time.Now().Add(-3 * time.Hour)
			err = os.Chtimes(chunkFQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			Expect(chunkFQN).To(BeAnExistingFile())

			space.RunCleanup(ini)

			// Old orphaned chunk should be removed
			Expect(chunkFQN).NotTo(BeAnExistingFile())
		})

		It("should preserve recent chunks", func() {
			avail := fs.GetAvail()
			mpath := avail[mpaths[0]]

			chunkDir := mpath.MakePathCT(&bck, fs.ChunkCT)
			err := cos.CreateDir(chunkDir)
			Expect(err).NotTo(HaveOccurred())

			// Create recent chunk
			chunkFileName := "activefile." + cos.GenTie() + ".0001"
			chunkFQN := filepath.Join(chunkDir, chunkFileName)
			createTestFile(chunkFQN, 1024)

			// Keep recent timestamp
			recentTime := time.Now().Add(-30 * time.Minute)
			err = os.Chtimes(chunkFQN, recentTime, recentTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			// Recent chunk should be preserved
			Expect(chunkFQN).To(BeAnExistingFile())
		})

		It("should keep finalized manifest", func() {
			avail := fs.GetAvail()
			mpath := avail[mpaths[1]]
			fqnObj := mpath.MakePathFQN(&bck, fs.ObjCT, "largefile")
			createTestLOM(fqnObj, 1234, time.Now().Add(-3*time.Hour))
			ufest := filepath.Join(mpath.MakePathCT(&bck, fs.ChunkMetaCT), "largefile")
			createTestFile(ufest, 900)
			os.Chtimes(ufest, time.Now().Add(-3*time.Hour), time.Now().Add(-3*time.Hour))

			space.RunCleanup(ini)
			Expect(ufest).To(BeAnExistingFile()) // finalized manifest must stay
		})
	})

	Describe("Deleted content cleanup", func() {
		It("should aggressively clean deleted content regardless of age", func() {
			avail := fs.GetAvail()
			mi := avail[mpaths[0]]

			// Create object and move to deleted
			objDir := mi.MakePathCT(&bck, fs.ObjCT)
			objFQN := filepath.Join(objDir, "to-be-deleted.txt")
			createTestLOM(objFQN, 512) // Default current time is fine for deleted content

			err := mi.MoveToDeleted(objFQN)
			Expect(err).NotTo(HaveOccurred())

			// Verify in deleted directory
			deletedFiles, err := os.ReadDir(mi.DeletedRoot())
			Expect(err).NotTo(HaveOccurred())
			Expect(len(deletedFiles)).To(Equal(1))

			// Even if we make it very recent, it should be cleaned up
			recentTime := time.Now().Add(-5 * time.Minute)
			deletedPath := filepath.Join(mi.DeletedRoot(), deletedFiles[0].Name())
			err = os.Chtimes(deletedPath, recentTime, recentTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			// Deleted content should be cleaned up regardless of age
			deletedFilesAfter, err := os.ReadDir(mi.DeletedRoot())
			Expect(err).NotTo(HaveOccurred())
			Expect(len(deletedFilesAfter)).To(Equal(0))
		})
	})
})

//
// HELPERS (compare w/ core/lom_test.go)
//

func newBasicLom(fqn string, size ...int64) *core.LOM {
	lom := &core.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())

	if len(size) > 0 {
		lom.SetSize(size[0])
	}
	return lom
}

func createTestLOM(fqn string, size int, atime ...time.Time) {
	createTestFile(fqn, size)
	lom := newBasicLom(fqn)
	lom.SetSize(int64(size))
	lom.IncVersion()

	// Set custom atime if provided
	if len(atime) > 0 {
		lom.SetAtimeUnix(atime[0].UnixNano())
	}

	Expect(persist(lom)).NotTo(HaveOccurred())
}

func createTestFile(fqn string, size int) {
	_ = os.Remove(fqn)
	err := cos.CreateDir(filepath.Dir(fqn))
	Expect(err).NotTo(HaveOccurred())

	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		reader, _ := readers.NewRand(int64(size), cos.ChecksumNone)
		_, err := io.Copy(testFile, reader)
		_ = testFile.Close()
		Expect(err).ShouldNot(HaveOccurred())
	} else {
		_ = testFile.Close()
	}
}

func persist(lom *core.LOM) error {
	if lom.AtimeUnix() == 0 {
		lom.SetAtimeUnix(time.Now().UnixNano())
	}
	return lom.Persist()
}

func findOtherMpath(mi *fs.Mountpath) *fs.Mountpath {
	avail := fs.GetAvail()
	Expect(len(avail)).To(BeNumerically(">=", 2))

	for _, other := range avail {
		if other.Path != mi.Path {
			return other
		}
	}
	Fail("No other mountpath found")
	return nil
}
