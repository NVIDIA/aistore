// Package core_test provides tests for chunk manifest functionality
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/trand"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ufest Core Functionality", func() {
	const (
		tmpDir     = "/tmp/ufest_test"
		oneMpath   = tmpDir + "/onempath"
		bucketName = "UFEST_TEST_Bucket"
	)

	localBck := cmn.Bck{Name: bucketName, Provider: apc.AIS, Ns: cmn.NsGlobal}

	var (
		mix     = fs.Mountpath{Path: oneMpath}
		bmdMock = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketName, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, BID: 301},
			),
		)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(oneMpath)
		_, _ = fs.Add(oneMpath, "daeID")
		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_, _ = fs.Remove(oneMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("NewUfest Constructor", func() {
		testObjectName := "test-objects/constructor-test.bin"
		testFileSize := int64(cos.MiB)

		It("should create manifest with generated ID when ID is empty", func() {
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, testFileSize)

			startTime := time.Now()
			manifest, err := core.NewUfest("", lom, false)
			Expect(err).NotTo(HaveOccurred())

			Expect(manifest).NotTo(BeNil())
			Expect(len(manifest.ID())).To(BeNumerically(">=", 8))
			Expect(manifest.Created().After(startTime.Add(-time.Second))).To(BeTrue())
			Expect(manifest.Completed()).To(BeFalse())
			Expect(manifest.Lom()).To(Equal(lom))
		})

		It("should create manifest with provided ID", func() {
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, testFileSize)

			customID := "custom-upload-" + trand.String(8)
			manifest, err := core.NewUfest(customID, lom, false)
			Expect(err).NotTo(HaveOccurred())

			Expect(manifest.ID()).To(Equal(customID))
			Expect(manifest.Completed()).To(BeFalse())
		})
	})

	Describe("Add Method - Core Chunk Management", func() {
		var (
			manifest *core.Ufest
			lom      *core.LOM
		)

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/add-test.bin"
			testFileSize := int64(5 * cos.MiB)
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom = newBasicLom(localFQN, testFileSize)
			manifest, err = core.NewUfest("test-upload-123", lom, false)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should add first chunk successfully", func() {
			chunkSize := int64(cos.MiB)
			chunkNum := int64(1)

			chunk, err := manifest.NewChunk(int(chunkNum), lom)
			Expect(err).NotTo(HaveOccurred())
			chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"))

			err = manifest.Add(chunk, chunkSize, chunkNum)
			Expect(err).NotTo(HaveOccurred())

			// Verify chunk was added through public interface
			retrievedChunk := manifest.GetChunk(1, false)
			Expect(retrievedChunk).NotTo(BeNil())
			Expect(retrievedChunk.Path()).To(Equal(chunk.Path()))
			Expect(retrievedChunk.Size()).To(Equal(chunkSize))
			Expect(retrievedChunk.Num()).To(Equal(uint16(1)))
		})

		It("should add multiple chunks in order", func() {
			chunks := []struct {
				size int64
				num  int64
			}{
				{size: cos.MiB, num: 1},
				{size: cos.MiB, num: 2},
				{size: cos.MiB, num: 3},
			}

			// Add chunks in order
			for _, c := range chunks {
				chunk, err := manifest.NewChunk(int(c.num), lom)
				Expect(err).NotTo(HaveOccurred())
				chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee0ddf00d"))
				err = manifest.Add(chunk, c.size, c.num)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify all chunks are accessible
			for _, c := range chunks {
				retrievedChunk := manifest.GetChunk(int(c.num), false)
				Expect(retrievedChunk).NotTo(BeNil())
				Expect(retrievedChunk.Size()).To(Equal(c.size))
			}
		})

		It("should add chunks out of order and maintain correct ordering", func() {
			chunks := []struct {
				size int64
				num  int64
			}{
				{size: cos.MiB, num: 3},
				{size: cos.MiB, num: 1},
				{size: cos.MiB, num: 2},
			}

			// Add chunks out of order - use NewChunk to create proper chunks
			for _, c := range chunks {
				chunk, err := manifest.NewChunk(int(c.num), lom)
				Expect(err).NotTo(HaveOccurred())
				chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee045f00d"))
				err = manifest.Add(chunk, c.size, c.num)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify chunks are retrievable by number
			chunk1 := manifest.GetChunk(1, false)
			chunk2 := manifest.GetChunk(2, false)
			chunk3 := manifest.GetChunk(3, false)

			Expect(chunk1).NotTo(BeNil())
			Expect(chunk2).NotTo(BeNil())
			Expect(chunk3).NotTo(BeNil())
			Expect(chunk1.Num()).To(Equal(uint16(1)))
			Expect(chunk2.Num()).To(Equal(uint16(2)))
			Expect(chunk3.Num()).To(Equal(uint16(3)))
		})

		It("should replace existing chunk with same number (last wins)", func() {
			originalChunk, err := manifest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			originalChunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee045f00d"))
			err = manifest.Add(originalChunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Replace with new chunk - create new chunk object with same number
			replacementChunk, err := manifest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			replacementChunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "abcc0ffee045f00d"))
			err = manifest.Add(replacementChunk, 2*cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Verify replacement
			retrievedChunk := manifest.GetChunk(1, false)
			Expect(retrievedChunk).NotTo(BeNil())
			Expect(retrievedChunk.Path()).To(Equal(replacementChunk.Path()))
			Expect(retrievedChunk.Size()).To(Equal(int64(2 * cos.MiB)))
		})

		It("should reject chunks with invalid numbers", func() {
			// Test chunk number exceeding uint16 limit
			_, err := manifest.NewChunk(70000, lom) // > math.MaxUint16
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid chunk number"))

			_, err = manifest.NewChunk(-1, lom)
			Expect(err).To(HaveOccurred())
		})

		It("should handle concurrent chunk additions safely", func() {
			const numWorkers = 10
			var wg sync.WaitGroup

			// Add chunks concurrently
			for i := 1; i <= numWorkers; i++ {
				wg.Add(1)
				go func(chunkNum int) {
					defer wg.Done()
					chunk, err := manifest.NewChunk(chunkNum, lom)
					Expect(err).NotTo(HaveOccurred())
					chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee0ddf00d"))
					err = manifest.Add(chunk, cos.MiB, int64(chunkNum))
					Expect(err).NotTo(HaveOccurred())
				}(i)
			}

			wg.Wait()

			// Verify all chunks were added
			for i := 1; i <= numWorkers; i++ {
				chunk := manifest.GetChunk(i, false)
				Expect(chunk).NotTo(BeNil())
				Expect(chunk.Num()).To(Equal(uint16(i)))
			}
		})
	})

	Describe("GetChunk Method", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/getchunk-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest, err = core.NewUfest("test-get-123", lom, false)
			Expect(err).NotTo(HaveOccurred())

			// Add some test chunks using NewChunk
			for i := 1; i <= 3; i++ {
				chunk, err := manifest.NewChunk(i, lom)
				Expect(err).NotTo(HaveOccurred())
				chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee0ddf00d"))
				err = manifest.Add(chunk, cos.MiB, int64(i))
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("should retrieve existing chunks", func() {
			chunk := manifest.GetChunk(2, false)
			Expect(chunk).NotTo(BeNil())
			Expect(chunk.Num()).To(Equal(uint16(2)))

			expectedChunk, err := manifest.NewChunk(2, manifest.Lom())
			Expect(err).NotTo(HaveOccurred())
			Expect(chunk.Path()).To(Equal(expectedChunk.Path()))
		})

		It("should return nil for non-existent chunks", func() {
			chunk := manifest.GetChunk(99, false)
			Expect(chunk).To(BeNil())
		})

		It("should respect locking parameter", func() {
			// Test both locked and unlocked access
			chunk1 := manifest.GetChunk(1, false) // unlocked
			chunk2 := manifest.GetChunk(1, true)  // locked

			Expect(chunk1).NotTo(BeNil())
			Expect(chunk2).NotTo(BeNil())
			Expect(chunk1.Num()).To(Equal(chunk2.Num()))
		})
	})

	Describe("NewChunk Method - HRW Distribution", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/chunkname-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest, err = core.NewUfest("test-name-123", lom, false)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should generate chunk names for valid numbers", func() {
			chunk1, err := manifest.NewChunk(1, manifest.Lom())
			Expect(err).NotTo(HaveOccurred())
			Expect(chunk1.Path()).NotTo(BeEmpty())

			chunk2, err := manifest.NewChunk(2, manifest.Lom())
			Expect(err).NotTo(HaveOccurred())
			Expect(chunk2.Path()).NotTo(BeEmpty())

			// Names should be different
			Expect(chunk1.Path()).NotTo(Equal(chunk2.Path()))
		})

		It("should reject invalid chunk numbers", func() {
			_, err := manifest.NewChunk(0, manifest.Lom())
			Expect(err).To(HaveOccurred())

			_, err = manifest.NewChunk(-1, manifest.Lom())
			Expect(err).To(HaveOccurred())
		})

		It("should generate unique names for different upload IDs", func() {
			manifest2, err := core.NewUfest("different-id", manifest.Lom(), false)
			Expect(err).NotTo(HaveOccurred())

			chunk1, err1 := manifest.NewChunk(1, manifest.Lom())
			chunk2, err2 := manifest2.NewChunk(1, manifest2.Lom())

			Expect(err1).NotTo(HaveOccurred())
			Expect(err2).NotTo(HaveOccurred())
			Expect(chunk1.Path()).NotTo(Equal(chunk2.Path()))
		})
	})

	Describe("Abort Method - Cleanup", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/abort-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest, err = core.NewUfest("test-abort-123", lom, false)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should clean up chunk files on abort", func() {
			// Create actual chunk files using NewChunk
			var chunkPaths []string
			for i := 1; i <= 3; i++ {
				chunk, err := manifest.NewChunk(i, manifest.Lom())
				Expect(err).NotTo(HaveOccurred())
				chunkPaths = append(chunkPaths, chunk.Path())

				// Create directory and file
				_ = cos.CreateDir(filepath.Dir(chunk.Path()))
				if file, err := cos.CreateFile(chunk.Path()); err == nil {
					file.Close()
				}

				chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee0ddf00d"))
				err = manifest.Add(chunk, cos.MiB, int64(i))
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify files exist before abort
			for _, path := range chunkPaths {
				Expect(path).To(BeAnExistingFile())
			}

			// Abort and verify cleanup
			manifest.Abort(manifest.Lom())

			// Files should be removed (may not fail if already gone)
			for _, path := range chunkPaths {
				Expect(path).NotTo(BeAnExistingFile())
			}
		})

		It("should handle missing chunk files gracefully", func() {
			// Add chunk references to valid but non-existent files
			chunk, err := manifest.NewChunk(1, manifest.Lom())
			Expect(err).NotTo(HaveOccurred())
			chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "123c0ffee0ddf00d"))
			err = manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			manifest.Abort(manifest.Lom())
		})
	})

	Describe("Persistence - Store and Load", func() {
		var (
			manifest *core.Ufest
			lom      *core.LOM
		)

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/persist-test.bin"
			testFileSize := int64(3 * cos.MiB)
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)

			_ = cos.CreateDir(filepath.Dir(localFQN))

			createTestFile(localFQN, 0)

			lom = newBasicLom(localFQN, testFileSize)
			manifest, err = core.NewUfest("test-persist-123", lom, false)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should store partial manifest without completion", func() {
			chunk, err := manifest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"))
			createTestFile(chunk.Path(), cos.MiB)
			err = manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			err = manifest.StorePartial(lom, false /*locked*/)
			Expect(err).NotTo(HaveOccurred())

			Expect(manifest.Completed()).To(BeFalse())

			clone, err := core.NewUfest(manifest.ID(), lom, true)
			Expect(err).NotTo(HaveOccurred())
			err = clone.Add(chunk, chunk.Size(), 1)
			Expect(err).NotTo(HaveOccurred())

			lom.Lock(false)
			defer lom.Unlock(false)
			err = clone.LoadPartial(lom)
			Expect(err).NotTo(HaveOccurred())
			Expect(clone.Completed()).To(BeFalse())
		})

		It("should fail to load non-existent manifest", func() {
			var err error
			lom.Lock(false)
			defer lom.Unlock(false)
			loadManifest, err := core.NewUfest("", lom, true) // mustExist=true
			Expect(err).NotTo(HaveOccurred())
			err = loadManifest.LoadCompleted(lom)
			Expect(err).To(HaveOccurred())
		})

		It("persists and reloads manifest with identical ID and chunks", func() {
			testObject := "mpu/manifest-roundtrip.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN)

			u, err := core.NewUfest("rt12345-"+cos.GenTie(), lom, false)
			Expect(err).NotTo(HaveOccurred())

			// add a few chunks with MD5s
			sizes := []int64{64 * cos.KiB, 33 * cos.KiB, 128 * cos.KiB}
			var md5s [][]byte
			for i, sz := range sizes {
				c, err := u.NewChunk(i+1, lom)
				Expect(err).NotTo(HaveOccurred())
				m := creatChunkMD5andWhole(c.Path(), int(sz), nil) // writes file & returns MD5
				c.MD5 = m
				md5s = append(md5s, m)
				Expect(u.Add(c, sz, int64(i+1))).NotTo(HaveOccurred())
			}

			Expect(lom.CompleteUfest(u)).NotTo(HaveOccurred())

			// fresh manifest load
			loaded, err := core.NewUfest("", lom, true /* must-exist */)
			Expect(err).NotTo(HaveOccurred())
			lom.Lock(false)
			Expect(loaded.LoadCompleted(lom)).NotTo(HaveOccurred())
			lom.Unlock(false)

			Expect(loaded.ID()).To(Equal(u.ID()))
			Expect(loaded.Count()).To(Equal(len(sizes)))
			Expect(loaded.Size()).To(Equal(u.Size()))
			for i := 1; i <= len(sizes); i++ {
				oc := u.GetChunk(i, false)
				lc := loaded.GetChunk(i, false)
				Expect(lc).NotTo(BeNil())
				Expect(lc.Num()).To(Equal(oc.Num()))
				Expect(lc.Size()).To(Equal(oc.Size()))
				Expect(bytes.Equal(lc.MD5, md5s[i-1])).To(BeTrue())
			}
		})

		It("classifies corrupted manifest as ErrLmetaCorrupted on load", func() {
			testObject := "mpu/corrupted-manifest.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObject)
			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN)

			u, err := core.NewUfest("corrupt-"+cos.GenTie(), lom, false)
			Expect(err).NotTo(HaveOccurred())

			c, err := u.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			createTestChunk(c.Path(), 4*cos.KiB, nil)
			Expect(u.Add(c, 4*cos.KiB, 1)).NotTo(HaveOccurred())
			Expect(lom.CompleteUfest(u)).NotTo(HaveOccurred())

			// Corrupt the stored manifest
			mfqn := lom.GenFQN(fs.ChunkMetaCT)

			// 2) Corrupt it: flip a middle byte
			buf, err := os.ReadFile(mfqn)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(buf)).To(BeNumerically(">", 8)) // sanity
			buf[len(buf)/2] ^= 0x5A
			Expect(os.WriteFile(mfqn, buf, 0o644)).NotTo(HaveOccurred())

			// 3) Reload and expect classified corruption
			loaded, err := core.NewUfest("", lom, true)
			Expect(err).NotTo(HaveOccurred())
			lom.Lock(false)
			err = loaded.LoadCompleted(lom)
			lom.Unlock(false)
			Expect(err).To(HaveOccurred())

			fmt.Fprintf(GinkgoWriter, ">>> corruption load error: %T | %v\n", err, err)

			var (
				lmerr *cmn.ErrLmetaCorrupted
				bcerr *cos.ErrBadCksum
			)
			Expect(errors.As(err, &lmerr) || errors.As(err, &bcerr)).To(BeTrue(),
				"expected ErrLmetaCorrupted or ErrBadCksum")
		})

	})

	Describe("Locking Behavior", func() {
		It("should provide proper locking mechanisms", func() {
			testObjectName := "test-objects/lock-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, cos.MiB)
			manifest, err := core.NewUfest("test-lock-123", lom, false)
			Expect(err).NotTo(HaveOccurred())

			// Should be able to use locked operations
			chunk, err := manifest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			chunk.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"))
			err = manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			manifest.Lock()
			retrievedChunk := manifest.GetChunk(1, true /*locked*/)
			manifest.Unlock()

			Expect(retrievedChunk).NotTo(BeNil())
		})
	})

	Describe("Validation Tests", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			var err error
			testObjectName := "test-objects/validation-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest, err = core.NewUfest("test-validation-123", lom, false)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject nil chunk", func() {
			err := manifest.Add(nil, cos.MiB, 1)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("nil chunk"))
		})

		It("should reject invalid chunk number", func() {
			// Test zero chunk number
			_, err := manifest.NewChunk(0, manifest.Lom())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid chunk number"))

			// Test negative chunk number
			_, err = manifest.NewChunk(-5, manifest.Lom())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid chunk number"))
		})

		It("should enforce chunk number limits", func() {
			// Test exceeding uint16 limit
			_, err := manifest.NewChunk(70000, manifest.Lom()) // > math.MaxUint16 (65535)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid chunk number"))
		})
	})
})
