// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"

	onexxh "github.com/OneOfOne/xxhash"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("MPU-UfestRead", func() {
	const (
		tmpDir    = "/tmp/lom_test"
		numMpaths = 3

		bucketLocalA = "LOM_TEST_Local_A"
		bucketLocalB = "LOM_TEST_Local_B"
		bucketLocalC = "LOM_TEST_Local_C"

		bucketCloudA = "LOM_TEST_Cloud_A"
		bucketCloudB = "LOM_TEST_Cloud_B"

		sameBucketName = "LOM_TEST_Local_and_Cloud"
	)

	var (
		localBckB = cmn.Bck{Name: bucketLocalB, Provider: apc.AIS, Ns: cmn.NsGlobal}
	)

	var (
		mpaths []string
		mis    []*fs.Mountpath

		oldCloudProviders = cmn.GCO.Get().Backend.Providers
	)

	for i := range numMpaths {
		mpath := fmt.Sprintf("%s/mpath%d", tmpDir, i)
		mpaths = append(mpaths, mpath)
		mis = append(mis, &fs.Mountpath{Path: mpath})
		_ = cos.CreateDir(mpath)
	}

	config := cmn.GCO.BeginUpdate()
	config.TestFSP.Count = 1
	cmn.GCO.CommitUpdate(config)

	fs.TestNew(nil)
	for _, mpath := range mpaths {
		_, _ = fs.Add(mpath, "daeID")
	}

	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}, true)
	fs.CSM.Reg(fs.ObjChunkType, &fs.ObjChunkContentResolver{}, true)

	bmd := mock.NewBaseBownerMock(
		meta.NewBck(
			bucketLocalA, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumNone}, BID: 1},
		),
		meta.NewBck(
			bucketLocalB, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, LRU: cmn.LRUConf{Enabled: true}, BID: 2},
		),
		meta.NewBck(
			bucketLocalC, apc.AIS, cmn.NsGlobal,
			&cmn.Bprops{
				Cksum:  cmn.CksumConf{Type: cos.ChecksumOneXxh},
				LRU:    cmn.LRUConf{Enabled: true},
				Mirror: cmn.MirrorConf{Enabled: true, Copies: 2},
				BID:    3,
			},
		),
		meta.NewBck(sameBucketName, apc.AIS, cmn.NsGlobal, &cmn.Bprops{BID: 4}),
		meta.NewBck(bucketCloudA, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 5}),
		meta.NewBck(bucketCloudB, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 6}),
		meta.NewBck(sameBucketName, apc.AWS, cmn.NsGlobal, &cmn.Bprops{BID: 7}),
	)

	BeforeEach(func() {
		// Dummy backend provider for tests involving cloud buckets
		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = map[string]cmn.Ns{
			apc.AWS: cmn.NsGlobal,
		}

		cmn.GCO.CommitUpdate(config)

		for _, mpath := range mpaths {
			_, _ = fs.Add(mpath, "daeID")
		}
		_ = mock.NewTarget(bmd)
	})

	AfterEach(func() {
		_ = os.RemoveAll(tmpDir)

		config := cmn.GCO.BeginUpdate()
		config.Backend.Providers = oldCloudProviders
		cmn.GCO.CommitUpdate(config)
	})

	Describe("UfestReader", func() {
		const (
			fileSize  = 1 * cos.GiB
			chunkSize = 64 * cos.MiB
		)

		It("should correctly read chunked file with checksum validation", func() {
			testObject := "chunked/large-file.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, fileSize)

			Expect(cos.CreateDir(filepath.Dir(localFQN))).NotTo(HaveOccurred())

			ufest := core.NewUfest("test-upload-"+cos.GenTie(), lom, false /*must-exist*/)

			// Generate large file in chunks and compute original checksum
			originalChecksum := onexxh.New64()
			var totalBytesWritten int64

			for chunkNum := 1; totalBytesWritten < fileSize; chunkNum++ {
				// Calculate this chunk's size
				remaining := fileSize - totalBytesWritten
				thisChunkSize := min(chunkSize, remaining)

				chunkPath, err := ufest.ChunkName(chunkNum)
				Expect(err).NotTo(HaveOccurred())

				createTestChunk(chunkPath, int(thisChunkSize), originalChecksum)

				chunk := &core.Uchunk{
					Path:  chunkPath,
					Cksum: nil, // No individual chunk checksums for this test
				}

				err = ufest.Add(chunk, thisChunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())

				totalBytesWritten += thisChunkSize
			}

			// Verify we wrote the expected amount
			Expect(totalBytesWritten).To(Equal(int64(fileSize)))
			Expect(ufest.Size).To(Equal(int64(fileSize)))

			// Store manifest (this will mark it as completed)
			err := ufest.StoreCompleted(lom)
			Expect(err).NotTo(HaveOccurred())
			Expect(ufest.Completed()).To(BeTrue())

			// Create chunk reader
			reader, err := ufest.NewReader()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				Expect(reader.Close()).NotTo(HaveOccurred())
			}()

			// Read entire file
			readChecksum := onexxh.New64()
			buffer := make([]byte, 32*cos.KiB) // 32KB read buffer
			var totalBytesRead int64

			for {
				n, rerr := reader.Read(buffer)
				if n > 0 {
					readChecksum.Write(buffer[:n])
					totalBytesRead += int64(n)
				}

				if rerr == io.EOF {
					break
				}
				Expect(rerr).NotTo(HaveOccurred())
			}

			// Verify
			Expect(totalBytesRead).To(Equal(int64(fileSize)))

			originalSum := originalChecksum.Sum64()
			readSum := readChecksum.Sum64()
			Expect(readSum).To(Equal(originalSum))

			By(fmt.Sprintf("Successfully read %d bytes in %d chunks, checksums match (0x%x)",
				totalBytesRead, ufest.Num, originalSum))
		})

		It("should handle edge cases correctly", func() {
			By("testing empty file")
			testObject := "chunked/empty-file.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, 0)
			ufest := core.NewUfest("empty-test-"+cos.GenTie(), lom, false /*must-exist*/)

			// Create single empty chunk
			chunkPath, err := ufest.ChunkName(1)
			Expect(err).NotTo(HaveOccurred())

			createTestChunk(chunkPath, 0, nil) // Create empty file, no xxhash needed

			chunk := &core.Uchunk{Path: chunkPath}
			err = ufest.Add(chunk, 0, 1)
			Expect(err).NotTo(HaveOccurred())

			err = ufest.StoreCompleted(lom)
			Expect(err).NotTo(HaveOccurred())

			reader, err := ufest.NewReader()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				Expect(reader.Close()).NotTo(HaveOccurred())
			}()

			buffer := make([]byte, 1024)
			n, err := reader.Read(buffer)
			Expect(n).To(Equal(0))
			Expect(err).To(Equal(io.EOF))

			By("testing incomplete manifest")
			fqn2 := mis[0].MakePathFQN(&localBckB, fs.ObjectType, "incomplete.bin")
			createTestFile(fqn2, 0)
			lom2 := newBasicLom(fqn2)
			ufest2 := core.NewUfest("incomplete-test-"+cos.GenTie(), lom2, false /*must-exist*/)

			// Don't call Store() - manifest remains incomplete
			_, err = ufest2.NewReader()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("incomplete"))
		})

		It("should validate chunk size consistency", func() {
			testObject := "chunked/size-test.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN)
			ufest := core.NewUfest("size-test-"+cos.GenTie(), lom, false /*must-exist*/)

			// Create chunk that's smaller than declared size
			chunkPath, err := ufest.ChunkName(1)
			Expect(err).NotTo(HaveOccurred())

			// Create file with only 100 bytes using our helper
			createTestChunk(chunkPath, 100, nil)

			// But claim it's 200 bytes in the manifest
			chunk := &core.Uchunk{Path: chunkPath}
			err = ufest.Add(chunk, 200, 1)
			Expect(err).NotTo(HaveOccurred())

			lom.SetSize(200)
			err = ufest.StoreCompleted(lom)
			Expect(err).NotTo(HaveOccurred())

			reader, err := ufest.NewReader()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				Expect(reader.Close()).NotTo(HaveOccurred())
			}()

			// Reading should fail with unexpected EOF
			buffer := make([]byte, 300)
			_, err = reader.Read(buffer)
			Expect(err).To(Equal(io.ErrUnexpectedEOF))
		})

		It("should handle multiple small chunks", func() {
			const (
				numChunks     = 100
				chunkSize     = 1 * cos.MiB
				totalFileSize = numChunks * chunkSize
			)

			testObject := "chunked/many-small-chunks.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, totalFileSize)
			ufest := core.NewUfest("multi-chunk-test-"+cos.GenTie(), lom, false /*must-exist*/)

			originalChecksum := onexxh.New64()

			// Create many small chunks
			for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
				chunkPath, err := ufest.ChunkName(chunkNum)
				Expect(err).NotTo(HaveOccurred())

				// Create chunk and update checksum in one step
				createTestChunk(chunkPath, chunkSize, originalChecksum)

				chunk := &core.Uchunk{Path: chunkPath}
				err = ufest.Add(chunk, chunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())
			}

			err := ufest.StoreCompleted(lom)
			Expect(err).NotTo(HaveOccurred())

			reader, err := ufest.NewReader()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				Expect(reader.Close()).NotTo(HaveOccurred())
			}()

			// Read with various buffer sizes to test edge cases
			readChecksum := onexxh.New64()
			buffer := make([]byte, 7*cos.KiB)
			var totalRead int64

			for {
				n, rerr := reader.Read(buffer)
				if n > 0 {
					readChecksum.Write(buffer[:n])
					totalRead += int64(n)
				}
				if rerr == io.EOF {
					break
				}
				Expect(rerr).NotTo(HaveOccurred())
			}

			Expect(totalRead).To(Equal(int64(totalFileSize)))
			Expect(readChecksum.Sum64()).To(Equal(originalChecksum.Sum64()))

			By(fmt.Sprintf("Successfully read %d chunks totaling %d bytes",
				numChunks, totalFileSize))
		})
	})

	Describe("MPU Complete -> GET scenario", func() {
		const (
			testObject = "mpu/test-chunked-object.bin"
			chunkSize  = 64 * cos.KiB
			numChunks  = 5
			totalSize  = numChunks * chunkSize
		)
		It("should persist chunked flag correctly and survive LOM reload", func() {
			By("Step 1: Simulating successful MPU completion")

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)
			// Create initial LOM for the object (no need for empty file - chunks are the real data)
			lom := newBasicLom(localFQN)

			// Create Ufest (multipart upload session)
			ufest := core.NewUfest("mpu-test-"+cos.GenTie(), lom, false /*must-exist*/)

			// Add chunks to simulate MPU
			for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
				chunkPath, err := ufest.ChunkName(chunkNum)
				Expect(err).NotTo(HaveOccurred())

				// Create actual chunk file
				createTestChunk(chunkPath, chunkSize, nil)

				chunk := &core.Uchunk{
					Path: chunkPath,
					Siz:  chunkSize,
				}

				err = ufest.Add(chunk, chunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())
			}

			// Complete the MPU - this should set the chunked flag
			err := lom.CompleteUfest(ufest)
			Expect(err).NotTo(HaveOccurred())
			Expect(lom.IsChunked()).To(BeTrue(), "LOM should be marked as chunked immediately after CompleteUfest")

			err = lom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())

			By("Step 2: Verify chunked flag is set immediately after Load")
			Expect(lom.IsChunked()).To(BeTrue(), "LOM should be marked as chunked immediately after Load")

			By("Step 3: Simulate first GET request with fresh LOM instance")

			// This simulates what happens on a GET request - fresh LOM instance
			getLom := &core.LOM{}
			err = getLom.InitFQN(localFQN, nil)
			Expect(err).NotTo(HaveOccurred())

			// Load metadata from filesystem (this is where the chunked flag should be read)
			err = getLom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())

			By("Step 4: Verify chunked flag is still present after reload")
			Expect(getLom.IsChunked()).To(BeTrue(), "Fresh LOM instance should detect chunked flag from persisted metadata")
		})

		It("should handle the chunked flag through manual persistence cycle", func() {
			By("Testing manual flag persistence without Ufest")

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)

			createTestFile(localFQN, totalSize)
			lom := newBasicLom(localFQN)
			lom.SetSize(totalSize)
			lom.SetAtimeUnix(time.Now().UnixNano())

			// TODO -- FIXME: Manually set chunked flag and persist
			lom.Lock(true)
			// lom.SetLmfl(core.LmflChunk) // Manually set the flag
			err := lom.PersistMain()
			lom.Unlock(true)
			Expect(err).NotTo(HaveOccurred())

			// Verify it persisted correctly
			freshLom := &core.LOM{}
			err = freshLom.InitFQN(localFQN, nil)
			Expect(err).NotTo(HaveOccurred())

			err = freshLom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())

			// Expect(freshLom.IsChunked()).To(BeTrue(), "Manually set chunked flag should persist")
		})

		It("should detect when chunked flag is lost", func() {
			By("Creating chunked object and then clearing flag to simulate the bug")

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)
			lom := newBasicLom(localFQN)

			// Set up as chunked object
			ufest := core.NewUfest("bug-test-"+cos.GenTie(), lom, false /*must-exist*/)
			chunkPath, _ := ufest.ChunkName(1)
			createTestChunk(chunkPath, chunkSize, nil)

			chunk := &core.Uchunk{Path: chunkPath, Siz: chunkSize}
			ufest.Add(chunk, chunkSize, 1)

			err := lom.CompleteUfest(ufest)
			Expect(err).NotTo(HaveOccurred())
			Expect(lom.IsChunked()).To(BeTrue())

			// Now simulate the bug - create fresh LOM and see if flag is missing
			bugLom := &core.LOM{}
			err = bugLom.InitFQN(localFQN, nil)
			Expect(err).NotTo(HaveOccurred())

			err = bugLom.Load(false, false)
			Expect(err).NotTo(HaveOccurred())

			if !bugLom.IsChunked() {
				Fail("BUG REPRODUCED: Fresh LOM instance lost chunked flag after CompleteUfest!")
			}

			By("If we reach here, the chunked flag persisted correctly")
		})
	})

	It("should validate sequential parts and compute whole checksums during complete upload", func() {
		var (
			partSize      = rand.IntN(2*cos.MiB) + 64*cos.KiB
			numParts      = rand.IntN(10) + 1
			totalFileSize = partSize * numParts
			testObject    = "mpu/complete-upload-test-" + cos.GenTie() + ".bin"
			localFQN      = mis[0].MakePathFQN(&localBckB, fs.ObjectType, testObject)
		)
		By("Step 1: Setting up chunked upload with sequential parts")
		lom := newBasicLom(localFQN)

		uploadID := "complete-test-" + cos.GenTie()
		manifest := core.NewUfest(uploadID, lom, false /*must-exist*/)

		// track expected checksums
		var (
			expectedWholeMD5 = cos.NewCksumHash(cos.ChecksumMD5)
			partMD5s         [][]byte
		)
		// create chunks in sorted order; compute "whole" MD5 as well
		for partNum := 1; partNum <= numParts; partNum++ {
			chunkPath, err := manifest.ChunkName(partNum)
			Expect(err).NotTo(HaveOccurred())

			partMD5 := creatChunkMD5andWhole(chunkPath, partSize, expectedWholeMD5.H)
			partMD5s = append(partMD5s, partMD5)

			chunk := &core.Uchunk{
				Path: chunkPath,
				MD5:  partMD5,
			}

			err = manifest.Add(chunk, int64(partSize), int64(partNum))
			Expect(err).NotTo(HaveOccurred())
		}

		By("Step 2: Validate manifest state before completion")
		Expect(manifest.Num).To(Equal(uint16(numParts)), "Should have correct number of parts")
		Expect(manifest.Size).To(Equal(int64(totalFileSize)), "Should have correct total size")
		Expect(manifest.Completed()).To(BeFalse(), "Should not be completed yet")

		By("Step 3: Simulate complete-upload validation logic")
		manifest.Lock()

		Expect(len(manifest.Chunks)).To(BeNumerically(">=", numParts),
			"Should have at least the requested number of parts")

		for i := range numParts {
			expectedPartNum := uint16(i + 1)
			actualChunk := manifest.GetChunk(expectedPartNum, true /*locked*/)
			Expect(actualChunk).NotTo(BeNil(), "Part %d should exist", expectedPartNum)
			Expect(actualChunk.Num).To(Equal(expectedPartNum),
				"Part should have correct sequential number")
			Expect(bytes.Equal(actualChunk.MD5, partMD5s[i])).To(BeTrue(), "Part should have correct MD5")
		}

		manifest.Unlock()

		By("Step 4: Compute whole-object checksum (production lines 378-395)")
		wholeCksum := cos.NewCksumHash(cos.ChecksumMD5) // Local bucket uses MD5
		err := manifest.ComputeWholeChecksum(wholeCksum)
		Expect(err).NotTo(HaveOccurred(), "Should compute whole checksum successfully")

		// computed checksum must match
		expectedWholeMD5.Finalize()
		Expect(wholeCksum.Cksum.Value()).To(Equal(expectedWholeMD5.Cksum.Value()),
			"Computed whole checksum should match expected")

		By("Step 5: Generate S3 multipart ETag")
		etag, err := manifest.ETagS3()
		Expect(err).NotTo(HaveOccurred(), "Should generate S3 ETag successfully")
		Expect(etag).To(MatchRegexp(`^"[a-f0-9]{32}-\d+"$`),
			"ETag should follow S3 multipart format: <md5>-<partcount>")
		Expect(etag).To(HaveSuffix(fmt.Sprintf("-%d\"", numParts)),
			"ETag should end with correct part count")

		By("Step 6: Perform atomic completion (production line 426)")
		lom.SetCksum(&wholeCksum.Cksum)
		lom.SetCustomKey(cmn.ETag, etag)

		err = lom.CompleteUfest(manifest)
		Expect(err).NotTo(HaveOccurred(), "CompleteUfest should succeed")

		By("Step 7: Verify post-completion state")
		Expect(manifest.Completed()).To(BeTrue(), "Manifest should be marked completed")
		Expect(lom.IsChunked()).To(BeTrue(), "LOM should be marked as chunked")

		// check LOM checksum
		lomCksum := lom.Checksum()
		Expect(lomCksum).NotTo(BeNil(), "LOM should have checksum set")
		Expect(lomCksum.Value()).To(Equal(wholeCksum.Cksum.Value()),
			"LOM checksum should match computed whole checksum")

		// ETag
		lomETag, exists := lom.GetCustomKey(cmn.ETag)
		Expect(exists).To(BeTrue(), "LOM's ETag must exist")
		Expect(lomETag).To(Equal(etag), "LOM should have correct ETag")

		By("Step 8: Verify persistence across LOM reload")
		freshLom := &core.LOM{}
		err = freshLom.InitFQN(localFQN, nil)
		Expect(err).NotTo(HaveOccurred())

		err = freshLom.Load(false, false)
		Expect(err).NotTo(HaveOccurred())

		Expect(freshLom.IsChunked()).To(BeTrue(), "Reloaded LOM should still be chunked")
		reloadedCksum := freshLom.Checksum()
		Expect(reloadedCksum).NotTo(BeNil(), "Reloaded LOM should have checksum")
		Expect(reloadedCksum.Value()).To(Equal(wholeCksum.Cksum.Value()),
			"Reloaded checksum should match")

		reloadedETag, exists := freshLom.GetCustomKey(cmn.ETag)
		Expect(exists).To(BeTrue(), "Reloaded ETag must exist")
		Expect(reloadedETag).To(Equal(etag), "Reloaded ETag should match")

		By(fmt.Sprintf("Successfully completed upload with %d parts, total size %d bytes, ETag: %s",
			numParts, totalFileSize, etag))
	})
	It("should support identical range reads for chunked and monolithic objects", func() {
		const (
			numChunks = 5
		)
		// make some interesting boundaries
		chunkSize := rand.IntN(400*cos.KiB) + 50*cos.KiB + rand.IntN(1000)

		chunkedObject := "mpu/range-test-chunked-" + cos.GenTie() + ".bin"
		monolithicObject := "mpu/range-test-mono-" + cos.GenTie() + ".bin"

		By("Step 1: Create chunked object")
		chunkedFQN := mis[0].MakePathFQN(&localBckB, fs.ObjectType, chunkedObject)
		Expect(cos.CreateDir(filepath.Dir(chunkedFQN))).NotTo(HaveOccurred())

		chunkedLom := newBasicLom(chunkedFQN)

		// cleanup
		chunkedLom.RemoveMain()

		uploadID := "range-test-" + cos.GenTie()
		manifest := core.NewUfest(uploadID, chunkedLom, false /*must-exist*/)

		// Create deterministic test data - we'll recreate this same data for monolithic
		seed := int64(12345)    // Fixed seed for reproducible data
		var allChunkData []byte // Collect all chunk data to create identical monolithic

		for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
			chunkPath, err := manifest.ChunkName(chunkNum)
			Expect(err).NotTo(HaveOccurred())

			// Create chunk and collect its data for monolithic reconstruction
			chunkData := createDeterministicChunk(chunkPath, chunkSize, seed+int64(chunkNum))
			allChunkData = append(allChunkData, chunkData...)

			chunk := &core.Uchunk{
				Path: chunkPath,
			}

			err = manifest.Add(chunk, int64(chunkSize), int64(chunkNum))
			Expect(err).NotTo(HaveOccurred())
		}

		// complete
		err := chunkedLom.CompleteUfest(manifest)
		Expect(err).NotTo(HaveOccurred())

		By("Step 2: Create identical monolithic object")
		monolithicFQN := mis[1].MakePathFQN(&localBckB, fs.ObjectType, monolithicObject)
		Expect(cos.CreateDir(filepath.Dir(monolithicFQN))).NotTo(HaveOccurred())

		totalFileSize := manifest.Size

		// create monolithic file with identical data
		createFileFromData(monolithicFQN, allChunkData)
		monolithicLom := newBasicLom(monolithicFQN)
		monolithicLom.SetSize(totalFileSize)
		monolithicLom.IncVersion()
		monolithicLom.SetAtimeUnix(time.Now().UnixNano())
		err = persist(monolithicLom)
		Expect(err).NotTo(HaveOccurred())
		monolithicLom.UncacheUnless()

		By("Step 3: Test range reads across various scenarios")

		testRanges := []struct {
			name   string
			offset int64
			length int
		}{
			// Within single chunks
			{"start of first chunk", 0, 1024},
			{"middle of first chunk", 1024, 2048},
			{"end of first chunk", int64(chunkSize) - 1024, 1024},
			{"middle of third chunk", 2*int64(chunkSize) + 5000, 3000},

			// Cross chunk boundaries
			{"across first two chunks", int64(chunkSize) - 512, 1024},
			{"across middle chunks", 2*int64(chunkSize) - 1000, 2000},
			{"across last two chunks", 4*int64(chunkSize) - 256, 512},

			// Larger ranges spanning multiple chunks
			{"span three chunks", int64(chunkSize) / 2, 2*chunkSize + 1000},
			{"most of object", 1000, int(totalFileSize - 2000)},

			// Edge cases
			{"single byte at start", 0, 1},
			{"single byte at end", totalFileSize - 1, 1},
			{"single byte at chunk boundary", int64(chunkSize), 1},
			{"zero length read", 5000, 0},
			{"exactly one chunk", int64(chunkSize), chunkSize},
			{"entire object", 0, int(totalFileSize)},
		}

		for _, tr := range testRanges {
			By(fmt.Sprintf("Testing range: %s (offset=%d, length=%d)", tr.name, tr.offset, tr.length))

			// read from chunked
			chunkedLom.Lock(false)
			chunkedHandle, err := chunkedLom.NewHandle(true /*loaded*/)
			Expect(err).NotTo(HaveOccurred(), "Should create chunked handle")

			chunkedData := make([]byte, tr.length)
			chunkedN, chunkedErr := chunkedHandle.ReadAt(chunkedData, tr.offset)
			chunkedHandle.Close()
			chunkedLom.Unlock(false)

			// read from monolithic
			monolithicLom.Lock(false)
			monolithicHandle, err := monolithicLom.NewHandle(true /*loaded*/)
			Expect(err).NotTo(HaveOccurred(), "Should create monolithic handle")

			monolithicData := make([]byte, tr.length)
			monolithicN, monolithicErr := monolithicHandle.ReadAt(monolithicData, tr.offset)
			monolithicHandle.Close()
			monolithicLom.Unlock(false)

			Expect(chunkedN).To(Equal(monolithicN), fmt.Sprintf("Read lengths should match for %s, got (chunked %d, mono %d)",
				tr.name, chunkedN, monolithicN))

			if chunkedErr != nil || monolithicErr != nil {
				Expect(chunkedErr).To(HaveOccurred(), "Chunked should have failed")
				Expect(monolithicErr).To(HaveOccurred(), "Monolithic should have failed")
			}

			if chunkedErr == nil && monolithicErr == nil {
				Expect(bytes.Equal(chunkedData[:chunkedN], monolithicData[:monolithicN])).To(BeTrue(),
					fmt.Sprintf("Read mismatch %q: %v (%d) vs %v (%d)",
						tr.name, cos.BHead(chunkedData[:chunkedN]), chunkedN, cos.BHead(monolithicData[:monolithicN]), monolithicN))
			}
		}

		By("Step 4: Test out-of-bounds reads")

		outOfBoundsTests := []struct {
			name      string
			offset    int64
			length    int
			expectErr bool
		}{
			{"read past end", totalFileSize, 1000, true},
			{"read way past end", totalFileSize + 1000000, 1000, true},
			{"negative offset", -1, 1000, true},
			{"read starting just before end", totalFileSize - 10, 100, false}, // Partial read, should work
		}

		for _, test := range outOfBoundsTests {
			By("Testing out-of-bounds: " + test.name)

			// chunked
			chunkedLom.Lock(false)
			chunkedHandle, err := chunkedLom.NewHandle(true /*loaded*/)
			Expect(err).NotTo(HaveOccurred())

			chunkedData := make([]byte, test.length)
			chunkedN, chunkedErr := chunkedHandle.ReadAt(chunkedData, test.offset)
			chunkedHandle.Close()
			chunkedLom.Unlock(false)

			// monolithic
			monolithicLom.Lock(false)
			monolithicHandle, err := monolithicLom.NewHandle(true /*loaded*/)
			Expect(err).NotTo(HaveOccurred())

			monolithicData := make([]byte, test.length)
			monolithicN, monolithicErr := monolithicHandle.ReadAt(monolithicData, test.offset)
			monolithicHandle.Close()
			monolithicLom.Unlock(false)

			Expect(chunkedN).To(Equal(monolithicN))

			if chunkedErr != nil || monolithicErr != nil {
				// both must fail as expected
				Expect(chunkedErr).To(HaveOccurred(), "Chunked should have failed")
				Expect(monolithicErr).To(HaveOccurred(), "Monolithic should have failed")
			}

			if test.expectErr {
				Expect(chunkedErr).To(HaveOccurred(), "Should fail for %s", test.name)
			}
		}

		By(fmt.Sprintf("Successfully tested range reads on %d-byte object with %d chunks", totalFileSize, numChunks))
	})
})

//
// HELPER functions
//

// optionally, compute "whole" MD5 as well
func creatChunkMD5andWhole(fqn string, size int, wholeMD5Writer io.Writer) []byte {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())

	if size == 0 {
		_ = testFile.Close()
		return make([]byte, md5.Size)
	}
	var (
		mw       io.Writer
		chunkMD5 = cos.NewCksumHash(cos.ChecksumMD5)
	)
	if wholeMD5Writer != nil {
		mw = cos.IniWriterMulti(testFile, chunkMD5.H, wholeMD5Writer)
	} else {
		mw = cos.IniWriterMulti(testFile, chunkMD5.H)
	}

	reader, _ := readers.NewRand(int64(size), cos.ChecksumNone)
	_, err = io.Copy(mw, reader)
	_ = testFile.Close()

	Expect(err).ShouldNot(HaveOccurred())
	chunkMD5.Finalize()

	md5Bytes := chunkMD5.Sum()
	Expect(len(md5Bytes)).To(Equal(md5.Size), fmt.Sprintf("MD5 size %d should be exactly %d bytes", len(md5Bytes), md5.Size))
	return md5Bytes
}

func createDeterministicChunk(fqn string, size int, seed int64) []byte {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())
	defer func(f *os.File) { Expect(f.Close()).ShouldNot(HaveOccurred()) }(testFile)

	if size == 0 {
		return []byte{}
	}

	data := make([]byte, size)
	pattern := byte(seed % 256) // Use seed as base pattern

	for i := range data {
		data[i] = byte((int(pattern) + i) % 256) // Simple incrementing pattern
	}

	_, err = testFile.Write(data)
	Expect(err).ShouldNot(HaveOccurred())

	return data
}

func createFileFromData(fqn string, data []byte) {
	_ = os.Remove(fqn)
	testFile, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())
	defer func(f *os.File) { Expect(f.Close()).ShouldNot(HaveOccurred()) }(testFile)

	_, err = testFile.Write(data)
	Expect(err).ShouldNot(HaveOccurred())
}
