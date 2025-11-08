// Package space_test is a unit test for the package.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package space_test

import (
	"io"
	"math/rand/v2"
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
		now    time.Time
	)

	BeforeEach(func() {
		now = time.Now()
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
		xcln.InitBase(cos.GenUUID(), apc.ActStoreCleanup, nil)
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
			oldTime := now.Add(-3 * time.Hour)
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
			oldTime := now.Add(-3 * time.Hour)
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

			oldTime := now.Add(-3 * time.Hour)
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
			recentTime := now.Add(-30 * time.Minute)
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
			workFQN := lom.GenFQN(fs.WorkCT, workTag)

			// force different PID
			i := strings.LastIndexByte(workFQN, '.')
			workFQN = workFQN[:i] + ".123456789"

			createTestFile(workFQN, 256)

			// backdate workfile
			oldTime := now.Add(-3 * time.Hour)
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
			workFQN := lom.GenFQN(fs.WorkCT, workTag)

			createTestFile(workFQN, 256)

			// Keep recent timestamp
			recentTime := now.Add(-30 * time.Minute)
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
			old := now.Add(-3 * time.Hour)
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
			old := now.Add(-3 * time.Hour)
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
			old := now.Add(-3 * time.Hour)
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
			oldTime := now.Add(-3 * time.Hour)
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
			recentTime := now.Add(-30 * time.Minute)
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
			createTestLOM(fqnObj, 1234, now.Add(-3*time.Hour))
			ufest := filepath.Join(mpath.MakePathCT(&bck, fs.ChunkMetaCT), "largefile")
			createTestFile(ufest, 900)
			os.Chtimes(ufest, now.Add(-3*time.Hour), now.Add(-3*time.Hour))

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
			recentTime := now.Add(-5 * time.Minute)
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

	It("removes old orphan chunk with no partial or completed manifest", func() {
		objName := "cleanup/orphan-no-manifest.bin"
		lom := &core.LOM{ObjName: objName}
		err := lom.InitBck(&bck)
		Expect(err).NotTo(HaveOccurred())

		// start a valid mpu session (auto-generated id)
		u, err := core.NewUfest("", lom, false /*must-exist*/)
		Expect(err).NotTo(HaveOccurred())

		// create a real chunk
		chunk, err := u.NewChunk(1, lom)
		Expect(err).NotTo(HaveOccurred())
		createTestFile(chunk.Path(), 64*cos.KiB) // any non-zero size

		// age
		old := now.Add(-3 * time.Hour)
		Expect(os.Chtimes(chunk.Path(), old, old)).NotTo(HaveOccurred())

		// run cleanup
		space.RunCleanup(ini)

		// expect the orphan chunk to be removed
		Expect(chunk.Path()).NotTo(BeAnExistingFile())
	})

	It("removes stray chunk with mismatched uploadID while keeping completed manifest (HRW-correct)", func() {
		objName := "cleanup/completed-vs-stray.bin"
		lom := core.AllocLOM(objName)
		Expect(lom.InitBck(&bck)).NotTo(HaveOccurred())
		defer core.FreeLOM(lom)

		u2, err := core.NewUfest("", lom, false /*mustExist*/)
		Expect(err).NotTo(HaveOccurred())

		const (
			chunkSize = 64 * cos.KiB
			numChunks = 3
		)
		for part := 1; part <= numChunks; part++ {
			ch, err := u2.NewChunk(part, lom)
			Expect(err).NotTo(HaveOccurred())
			createTestChunk(ch.Path(), chunkSize, nil)
			Expect(u2.Add(ch, chunkSize, int64(part))).NotTo(HaveOccurred())
		}
		err = lom.CompleteUfest(u2, false)
		Expect(err).NotTo(HaveOccurred())

		uu, err := core.NewUfest("", lom, true /* must-exist */)
		Expect(err).NotTo(HaveOccurred())

		lom.Lock(false)
		err = uu.LoadCompleted(lom)
		lom.Unlock(false)
		Expect(err).NotTo(HaveOccurred())

		Expect(uu.ID()).To(Equal(u2.ID())) // the completed ID should equal the one we just wrote

		// create a stray chunk under a different uploadid (u1) ---
		u1, err := core.NewUfest("", lom, false)
		Expect(err).NotTo(HaveOccurred())

		Expect(u1.ID()).NotTo(Equal(u2.ID()))

		stray, err := u1.NewChunk(1, lom)
		Expect(err).NotTo(HaveOccurred())
		Expect(cos.CreateDir(filepath.Dir(stray.Path()))).NotTo(HaveOccurred())
		createTestChunk(stray.Path(), chunkSize, nil)

		// make the stray chunk legitimate/discoverable
		Expect(u1.Add(stray, chunkSize, 1)).NotTo(HaveOccurred())
		// persist a partial for u1; current cleanup removes it too
		err = u1.StorePartial(lom, true)
		Expect(err).NotTo(HaveOccurred())

		// partial exists pre-cleanup
		partialFQN := lom.GenFQN(fs.ChunkMetaCT, u1.ID())
		Expect(partialFQN).To(BeAnExistingFile())

		// age the stray chunk well beyond dont_cleanup_time
		old := now.Add(-3 * time.Hour)
		_ = os.Chtimes(stray.Path(), old, old)
		_ = os.Chtimes(partialFQN, old, old)

		// run cleanup
		// TODO: try to initialize stats // before := ini.StatsT.Get(stats.CleanupStoreCount)
		space.RunCleanup(ini)
		// after := ini.StatsT.Get(stats.CleanupStoreCount)
		// Expect(after - before).To(Equal(int64(2)))

		// assert: stray chunk is removed
		Expect(stray.Path()).NotTo(BeAnExistingFile())

		// assert: partial manifest is removed
		Expect(partialFQN).NotTo(BeAnExistingFile())

		// completed must exist
		uu2, err := core.NewUfest("", lom, true)
		Expect(err).NotTo(HaveOccurred())

		lom.Lock(false)
		err = uu2.LoadCompleted(lom)
		lom.Unlock(false)

		Expect(err).NotTo(HaveOccurred())
		Expect(uu2.ID()).To(Equal(u2.ID()))
	})

	It("should remove files with malformed FQNs", func() {
		mi := fs.GetAvail()[mpaths[0]]
		invalidFQN := filepath.Join(mi.MakePathCT(&bck, fs.WorkCT), "invalid..filename")
		createTestFile(invalidFQN, 512)

		old := now.Add(-3 * time.Hour)
		os.Chtimes(invalidFQN, old, old)

		space.RunCleanup(ini)
		Expect(invalidFQN).NotTo(BeAnExistingFile())
	})

	Describe("Empty directory cleanup", func() {
		It("should remove already-empty directories during walk", func() {
			lom := &core.LOM{ObjName: "dir1/dir2/temp-for-empty-dir.txt"}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			createTestLOM(lom.FQN, 512)
			err = os.Remove(lom.FQN)
			Expect(err).NotTo(HaveOccurred())

			dir2 := filepath.Dir(lom.FQN)
			Expect(dir2).To(BeADirectory())
			dir1 := filepath.Dir(dir2)
			Expect(dir1).To(BeADirectory())

			// one (nested dir) at a time
			space.RunCleanup(ini)
			Expect(dir2).NotTo(BeADirectory())

			space.RunCleanup(ini)
			Expect(dir1).NotTo(BeADirectory())
		})

		It("should preserve directories with content", func() {
			lom := &core.LOM{ObjName: "recent-object-in-subdir.txt"}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			subDir := filepath.Join(filepath.Dir(lom.FQN), "subdir-with-content")
			err = cos.CreateDir(subDir)
			Expect(err).NotTo(HaveOccurred())

			recentFQN := filepath.Join(subDir, "recent-object.txt")
			createTestLOM(recentFQN, 512, now.Add(-30*time.Minute))

			space.RunCleanup(ini)

			Expect(recentFQN).To(BeAnExistingFile())
			Expect(subDir).To(BeADirectory())
		})
	})

	Describe("Global recency guard edge cases", func() {
		It("should handle the case when object may disappear during cleanup walk", func() {
			lom := &core.LOM{ObjName: "test-disappearing-object.txt"}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			lom.SetAtimeUnix(now.UnixNano())
			createTestFile(lom.FQN, 512)

			lom.IncVersion()
			err = persist(lom)
			Expect(err).NotTo(HaveOccurred())

			go func() { // _may_
				time.Sleep(time.Duration(rand.IntN(8)+1) * time.Millisecond)
				err = cos.RemoveFile(lom.FQN)
				Expect(err).NotTo(HaveOccurred())
			}()

			space.RunCleanup(ini)

			Expect(ini.Xaction.IsFinished()).To(BeTrue())
		})

		It("should handle misplaced at threshold timing", func() {
			lom := &core.LOM{ObjName: "threshold-timing.txt"}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			// other
			mi := findOtherMpath(lom.Mountpath())
			objFQN := filepath.Join(mi.MakePathCT(&bck, fs.ObjCT), "threshold-timing.txt")

			config := cmn.GCO.Get()
			pastThresholdTime := now.Add(-config.Space.DontCleanupTime.D() - time.Millisecond)

			createTestLOM(objFQN, 512, pastThresholdTime)
			err = os.Chtimes(objFQN, pastThresholdTime, pastThresholdTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			Expect(objFQN).NotTo(BeAnExistingFile())
		})

		It("should preserve files just under threshold", func() {
			avail := fs.GetAvail()
			mi := avail[mpaths[0]]

			objFQN := filepath.Join(mi.MakePathCT(&bck, fs.ObjCT), "just-under-threshold.txt")

			// Set file time just 1 minute under the threshold
			config := cmn.GCO.Get()
			justUnderTime := now.Add(-config.Space.DontCleanupTime.D() + time.Minute)

			createTestLOM(objFQN, 512, justUnderTime)
			err := os.Chtimes(objFQN, justUnderTime, justUnderTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			// Just under threshold should be preserved
			Expect(objFQN).To(BeAnExistingFile())
		})
	})

	// TODO "Bucket mismatch detection"

	Describe("Workfile variations", func() {
		It("should remove workfiles with invalid encoding", func() {
			lom := &core.LOM{ObjName: "object-for-invalid-workfiles.txt"}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			workDir := filepath.Dir(lom.GenFQN(fs.WorkCT, "dummy"))
			err = cos.CreateDir(workDir)
			Expect(err).NotTo(HaveOccurred())

			invalidWorkFiles := []string{
				"object-name.",                 // empty tag
				"object-name..",                // double dot
				"object-name",                  // no tag at all
				".invalid-prefix",              // starts with dot
				"obj.tag.",                     // ends with dot after tag
				"nodotsatall",                  // no separators at all
				"obj.tag.tiebreaker.nothexpid", // invalid hex pid
			}

			for _, invalidName := range invalidWorkFiles {
				workFQN := filepath.Join(workDir, invalidName)
				createTestFile(workFQN, 256)

				oldTime := now.Add(-3 * time.Hour)
				err = os.Chtimes(workFQN, oldTime, oldTime)
				Expect(err).NotTo(HaveOccurred())

				Expect(workFQN).To(BeAnExistingFile())
			}

			space.RunCleanup(ini)

			for _, invalidName := range invalidWorkFiles {
				workFQN := filepath.Join(workDir, invalidName)
				Expect(workFQN).NotTo(BeAnExistingFile())
			}
		})

		It("should preserve workfiles with valid encoding but current PID", func() {
			objectName := "test-object-for-current-workfile.txt"
			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			workTag := "current-work-tag"
			workFQN := lom.GenFQN(fs.WorkCT, workTag)

			createTestFile(workFQN, 256)

			// Even if it's old, current PID workfiles should be preserved
			oldTime := now.Add(-3 * time.Hour)
			err = os.Chtimes(workFQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			space.RunCleanup(ini)

			Expect(workFQN).To(BeAnExistingFile())
		})

		It("should remove workfiles from different PID", func() {
			objectName := "a/b/c/test-object-for-old-pid-workfile.txt"
			lom := &core.LOM{ObjName: objectName}
			err := lom.InitBck(&bck)
			Expect(err).NotTo(HaveOccurred())

			workTag := "old-pid-work-tag"
			workFQN := lom.GenFQN(fs.WorkCT, workTag)

			i := strings.LastIndexByte(workFQN, '.')
			oldPidWorkFQN := workFQN[:i] + ".999999999" // definitely not current PID

			createTestFile(oldPidWorkFQN, 256)

			oldTime := now.Add(-3 * time.Hour)
			err = os.Chtimes(oldPidWorkFQN, oldTime, oldTime)
			Expect(err).NotTo(HaveOccurred())

			Expect(oldPidWorkFQN).To(BeAnExistingFile())

			space.RunCleanup(ini)

			// Old PID workfile should be removed
			Expect(oldPidWorkFQN).NotTo(BeAnExistingFile())
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

	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(size), CksumType: cos.ChecksumNone})
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

func createTestChunk(fqn string, size int, xxhash io.Writer) {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size > 0 {
		mw := cos.IniWriterMulti(testFile, xxhash)
		reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: int64(size), CksumType: cos.ChecksumNone})
		_, err := io.Copy(mw, reader)
		_ = testFile.Close()

		Expect(err).ShouldNot(HaveOccurred())
	} else {
		_ = testFile.Close()
	}
}
