// Package core_test provides tests for chunk manifest functionality
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
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
		xattrMpath = tmpDir + "/xattr"
		bucketName = "UFEST_TEST_Bucket"
	)

	localBck := cmn.Bck{Name: bucketName, Provider: apc.AIS, Ns: cmn.NsGlobal}

	var (
		mix     = fs.Mountpath{Path: xattrMpath}
		bmdMock = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketName, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, BID: 301},
			),
		)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(xattrMpath)
		_, _ = fs.Add(xattrMpath, "daeID")
		_ = mock.NewTarget(bmdMock)

		// Register content resolvers
		fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
		fs.CSM.Reg(fs.ObjChunkType, &fs.ObjChunkContentResolver{}, true)
		fs.CSM.Reg(fs.ObjCMType, &fs.ObjCMContentResolver{}, true)
	})

	AfterEach(func() {
		_, _ = fs.Remove(xattrMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("NewUfest Constructor", func() {
		testObjectName := "test-objects/constructor-test.bin"
		testFileSize := int64(cos.MiB)

		It("should create manifest with generated ID when ID is empty", func() {
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, testFileSize)

			startTime := time.Now()
			manifest := core.NewUfest("", lom, false)

			Expect(manifest).NotTo(BeNil())
			Expect(len(manifest.ID)).To(BeNumerically(">", 0))
			Expect(manifest.Created.After(startTime.Add(-time.Second))).To(BeTrue())
			Expect(manifest.Completed()).To(BeFalse())
			Expect(manifest.Lom).To(Equal(lom))
		})

		It("should create manifest with provided ID", func() {
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, testFileSize)

			customID := "custom-upload-" + trand.String(8)
			manifest := core.NewUfest(customID, lom, false)

			Expect(manifest.ID).To(Equal(customID))
			Expect(manifest.Completed()).To(BeFalse())
		})
	})

	Describe("Add Method - Core Chunk Management", func() {
		var (
			manifest *core.Ufest
			lom      *core.LOM
		)

		BeforeEach(func() {
			testObjectName := "test-objects/add-test.bin"
			testFileSize := int64(5 * cos.MiB)
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom = newBasicLom(localFQN, testFileSize)
			manifest = core.NewUfest("test-upload-123", lom, false)
		})

		It("should add first chunk successfully", func() {
			chunkPath := "/tmp/chunk1"
			chunkSize := int64(cos.MiB)
			chunkNum := int64(1)

			chunk := &core.Uchunk{
				Path:  chunkPath,
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}

			err := manifest.Add(chunk, chunkSize, chunkNum)
			Expect(err).NotTo(HaveOccurred())

			// Verify chunk was added through public interface
			retrievedChunk := manifest.GetChunk(1, false)
			Expect(retrievedChunk).NotTo(BeNil())
			Expect(retrievedChunk.Path).To(Equal(chunkPath))
			Expect(retrievedChunk.Siz).To(Equal(chunkSize))
			Expect(retrievedChunk.Num).To(Equal(uint16(1)))
		})

		It("should add multiple chunks in order", func() {
			chunks := []struct {
				path string
				size int64
				num  int64
			}{
				{"/tmp/chunk1", cos.MiB, 1},
				{"/tmp/chunk2", cos.MiB, 2},
				{"/tmp/chunk3", cos.MiB, 3},
			}

			// Add chunks in order
			for _, c := range chunks {
				chunk := &core.Uchunk{
					Path:  c.path,
					Cksum: cos.NewCksum(cos.ChecksumOneXxh, trand.String(16)),
				}
				err := manifest.Add(chunk, c.size, c.num)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify all chunks are accessible
			for _, c := range chunks {
				retrievedChunk := manifest.GetChunk(uint16(c.num), false)
				Expect(retrievedChunk).NotTo(BeNil())
				Expect(retrievedChunk.Path).To(Equal(c.path))
				Expect(retrievedChunk.Siz).To(Equal(c.size))
			}
		})

		It("should add chunks out of order and maintain correct ordering", func() {
			chunks := []struct {
				path string
				size int64
				num  int64
			}{
				{"/tmp/chunk3", cos.MiB, 3},
				{"/tmp/chunk1", cos.MiB, 1},
				{"/tmp/chunk2", cos.MiB, 2},
			}

			// Add chunks out of order
			for _, c := range chunks {
				chunk := &core.Uchunk{
					Path:  c.path,
					Cksum: cos.NewCksum(cos.ChecksumOneXxh, trand.String(16)),
				}
				err := manifest.Add(chunk, c.size, c.num)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify chunks are retrievable by number
			chunk1 := manifest.GetChunk(1, false)
			chunk2 := manifest.GetChunk(2, false)
			chunk3 := manifest.GetChunk(3, false)

			Expect(chunk1.Path).To(Equal("/tmp/chunk1"))
			Expect(chunk2.Path).To(Equal("/tmp/chunk2"))
			Expect(chunk3.Path).To(Equal("/tmp/chunk3"))
		})

		It("should replace existing chunk with same number (last wins)", func() {
			originalChunk := &core.Uchunk{
				Path:  "/tmp/original_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "original_hash"),
			}
			err := manifest.Add(originalChunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Replace with new chunk
			replacementChunk := &core.Uchunk{
				Path:  "/tmp/replacement_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "replacement_hash"),
			}
			err = manifest.Add(replacementChunk, 2*cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Verify replacement
			retrievedChunk := manifest.GetChunk(1, false)
			Expect(retrievedChunk.Path).To(Equal("/tmp/replacement_chunk"))
			Expect(retrievedChunk.Siz).To(Equal(int64(2 * cos.MiB)))
		})

		It("should reject chunks with invalid numbers", func() {
			chunk := &core.Uchunk{Path: "/tmp/chunk"}

			// Test chunk number exceeding uint16 limit
			err := manifest.Add(chunk, cos.MiB, int64(70000)) // > math.MaxUint16
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("exceeds"))

			err = manifest.Add(chunk, cos.MiB, -1)
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
					chunk := &core.Uchunk{
						Path:  "/tmp/concurrent_chunk_" + strconv.Itoa(chunkNum),
						Cksum: cos.NewCksum(cos.ChecksumOneXxh, trand.String(16)),
					}
					err := manifest.Add(chunk, cos.MiB, int64(chunkNum))
					Expect(err).NotTo(HaveOccurred())
				}(i)
			}

			wg.Wait()

			// Verify all chunks were added
			for i := 1; i <= numWorkers; i++ {
				chunk := manifest.GetChunk(uint16(i), false)
				Expect(chunk).NotTo(BeNil())
				Expect(chunk.Num).To(Equal(uint16(i)))
			}
		})
	})

	Describe("GetChunk Method", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			testObjectName := "test-objects/getchunk-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest = core.NewUfest("test-get-123", lom, false)

			// Add some test chunks
			for i := 1; i <= 3; i++ {
				chunk := &core.Uchunk{
					Path:  "/tmp/chunk" + strconv.Itoa(i),
					Cksum: cos.NewCksum(cos.ChecksumOneXxh, "hash"+strconv.Itoa(i)),
				}
				err := manifest.Add(chunk, cos.MiB, int64(i))
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("should retrieve existing chunks", func() {
			chunk := manifest.GetChunk(2, false)
			Expect(chunk).NotTo(BeNil())
			Expect(chunk.Num).To(Equal(uint16(2)))
			Expect(chunk.Path).To(Equal("/tmp/chunk2"))
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
			Expect(chunk1.Num).To(Equal(chunk2.Num))
		})
	})

	Describe("ChunkName Method - HRW Distribution", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			testObjectName := "test-objects/chunkname-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest = core.NewUfest("test-name-123", lom, false)
		})

		It("should generate chunk names for valid numbers", func() {
			chunkName1, err := manifest.ChunkName(1)
			Expect(err).NotTo(HaveOccurred())
			Expect(chunkName1).NotTo(BeEmpty())

			chunkName2, err := manifest.ChunkName(2)
			Expect(err).NotTo(HaveOccurred())
			Expect(chunkName2).NotTo(BeEmpty())

			// Names should be different
			Expect(chunkName1).NotTo(Equal(chunkName2))
		})

		It("should reject invalid chunk numbers", func() {
			_, err := manifest.ChunkName(0)
			Expect(err).To(HaveOccurred())

			_, err = manifest.ChunkName(-1)
			Expect(err).To(HaveOccurred())
		})

		It("should generate unique names for different upload IDs", func() {
			manifest2 := core.NewUfest("different-id", manifest.Lom, false)

			name1, err1 := manifest.ChunkName(1)
			name2, err2 := manifest2.ChunkName(1)

			Expect(err1).NotTo(HaveOccurred())
			Expect(err2).NotTo(HaveOccurred())
			Expect(name1).NotTo(Equal(name2))
		})
	})

	Describe("Abort Method - Cleanup", func() {
		var manifest *core.Ufest

		BeforeEach(func() {
			testObjectName := "test-objects/abort-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, int64(cos.MiB))
			manifest = core.NewUfest("test-abort-123", lom, false)
		})

		It("should clean up chunk files on abort", func() {
			// Create actual chunk files - but don't use createDummyFile since it's redundant
			chunkPaths := []string{
				"/tmp/abort_chunk1",
				"/tmp/abort_chunk2",
				"/tmp/abort_chunk3",
			}

			for i, path := range chunkPaths {
				// Create files using cos.CreateFile like other parts of the codebase
				if file, err := cos.CreateFile(path); err == nil {
					file.Close()
				}

				chunk := &core.Uchunk{
					Path:  path,
					Cksum: cos.NewCksum(cos.ChecksumOneXxh, trand.String(16)),
				}
				err := manifest.Add(chunk, cos.MiB, int64(i+1))
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify files exist before abort
			for _, path := range chunkPaths {
				Expect(path).To(BeAnExistingFile())
			}

			// Abort and verify cleanup
			err := manifest.Abort(manifest.Lom)
			Expect(err).NotTo(HaveOccurred())

			// Files should be removed (may not fail if already gone)
			for _, path := range chunkPaths {
				Expect(path).NotTo(BeAnExistingFile())
			}
		})

		It("should handle missing chunk files gracefully", func() {
			// Add chunk references to non-existent files
			chunk := &core.Uchunk{
				Path:  "/tmp/nonexistent_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Abort should not fail even if files don't exist
			err = manifest.Abort(manifest.Lom)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Persistence - Store and Load", func() {
		var (
			manifest *core.Ufest
			lom      *core.LOM
		)

		BeforeEach(func() {
			testObjectName := "test-objects/persist-test.bin"
			testFileSize := int64(3 * cos.MiB)
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)

			_ = cos.CreateDir(filepath.Dir(localFQN))

			createTestFile(localFQN, 0)

			lom = newBasicLom(localFQN, testFileSize)
			manifest = core.NewUfest("test-persist-123", lom, false)
		})

		It("should store partial manifest without completion", func() {
			chunk := &core.Uchunk{
				Path:  "/tmp/partial_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			createTestFile(chunk.Path, cos.MiB)
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			err = manifest.StorePartial(lom)
			Expect(err).NotTo(HaveOccurred())

			Expect(manifest.Completed()).To(BeFalse())

			clone := core.NewUfest(manifest.ID, nil, true)
			err = clone.Add(chunk, chunk.Siz, 1)
			Expect(err).NotTo(HaveOccurred())

			err = clone.LoadPartial(lom)
			Expect(err).NotTo(HaveOccurred())
			Expect(clone.Completed()).To(BeFalse())
		})

		It("should store and load manifest correctly", func() {
			totalSize := int64(0)
			chunkSizes := []int64{cos.MiB, cos.MiB, cos.MiB}
			md5str := "d41d8cd98f00b204e9800998ecf8427e"

			for i, size := range chunkSizes {
				// Create proper MD5 bytes
				j := i % len(md5str)
				s := md5str[j:] + md5str[:j]
				md5bytes, _ := hex.DecodeString(s)

				chunk := &core.Uchunk{
					Path:  "/tmp/persist_chunk" + strconv.Itoa(i+1),
					Cksum: cos.NewCksum(cos.ChecksumOneXxh, "hash"+strconv.Itoa(i+1)),
					MD5:   md5bytes,
				}
				err := manifest.Add(chunk, size, int64(i+1))
				Expect(err).NotTo(HaveOccurred())
				totalSize += size
			}

			lom.SetSize(totalSize)

			err := manifest.StoreCompleted(lom, true) // testing=true to avoid file operations
			Expect(err).NotTo(HaveOccurred())
			Expect(manifest.Completed()).To(BeTrue())

			loadedManifest := core.NewUfest("", lom, true) // mustExist=true for loading
			err = loadedManifest.Load(lom)
			Expect(err).NotTo(HaveOccurred())

			Expect(loadedManifest.ID).To(Equal(manifest.ID))
			Expect(loadedManifest.Completed()).To(BeTrue())

			// Verify chunks
			for i := 1; i <= 3; i++ {
				originalChunk := manifest.GetChunk(uint16(i), false)
				loadedChunk := loadedManifest.GetChunk(uint16(i), false)

				Expect(loadedChunk).NotTo(BeNil())
				Expect(loadedChunk.Num).To(Equal(originalChunk.Num))
				Expect(loadedChunk.Path).To(Equal(originalChunk.Path))
				Expect(loadedChunk.Siz).To(Equal(originalChunk.Siz))
				Expect(loadedChunk.MD5).To(Equal(originalChunk.MD5))
			}
		})

		It("should fail to store invalid manifest", func() {
			chunk := &core.Uchunk{
				Path:  "/tmp/invalid_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			// Try to store with mismatched size (LOM size doesn't match chunk total)
			err = manifest.StoreCompleted(lom, true) // testing=true
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("size"))
		})

		It("should fail to load non-existent manifest", func() {
			// Try to load from LOM that has no stored manifest xattr
			loadManifest := core.NewUfest("", lom, true) // mustExist=true
			err := loadManifest.Load(lom)
			Expect(err).To(HaveOccurred())
		})

		It("should handle corrupted manifest data", func() {
			// Store valid manifest first to xattr
			chunk := &core.Uchunk{
				Path:  "/tmp/corrupt_test_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			lom.SetSize(cos.MiB)
			err = manifest.StoreCompleted(lom, true) // testing=true
			Expect(err).NotTo(HaveOccurred())

			// Verify that valid data can be loaded
			corruptManifest := core.NewUfest("", lom, true)
			err = corruptManifest.Load(lom)
			Expect(err).NotTo(HaveOccurred()) // Should succeed with valid data

			// TODO: add actual corruption testing when we have access to xattr manipulation
		})
	})

	Describe("State Management", func() {
		It("should track completion status correctly", func() {
			testObjectName := "test-objects/state-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)

			_ = cos.CreateDir(filepath.Dir(localFQN))

			createTestFile(localFQN, 0)

			lom := newBasicLom(localFQN, cos.MiB)
			manifest := core.NewUfest("test-state-123", lom, false)

			Expect(manifest.Completed()).To(BeFalse())

			chunk := &core.Uchunk{
				Path:  "/tmp/state_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			lom.SetSize(cos.MiB)
			err = manifest.StoreCompleted(lom, true) // testing=true
			Expect(err).NotTo(HaveOccurred())

			// Should be completed after successful store
			Expect(manifest.Completed()).To(BeTrue())
		})
	})

	Describe("Locking Behavior", func() {
		It("should provide proper locking mechanisms", func() {
			testObjectName := "test-objects/lock-test.bin"
			localFQN := mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
			lom := newBasicLom(localFQN, cos.MiB)
			manifest := core.NewUfest("test-lock-123", lom, false)

			// Should be able to use locked operations
			chunk := &core.Uchunk{
				Path:  "/tmp/lock_chunk",
				Cksum: cos.NewCksum(cos.ChecksumOneXxh, "badc0ffee0ddf00d"),
			}
			err := manifest.Add(chunk, cos.MiB, 1)
			Expect(err).NotTo(HaveOccurred())

			manifest.Lock()
			retrievedChunk := manifest.GetChunk(1, true /*locked*/)
			manifest.Unlock()

			Expect(retrievedChunk).NotTo(BeNil())
		})
	})
})
