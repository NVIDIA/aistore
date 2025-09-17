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
	"sync"
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
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, fileSize)

			Expect(cos.CreateDir(filepath.Dir(localFQN))).NotTo(HaveOccurred())

			ufest, err := core.NewUfest("test-upload-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			// Generate large file in chunks and compute original checksum
			originalChecksum := onexxh.New64()
			var totalBytesWritten int64

			for chunkNum := 1; totalBytesWritten < fileSize; chunkNum++ {
				// Calculate this chunk's size
				remaining := fileSize - totalBytesWritten
				thisChunkSize := min(chunkSize, remaining)

				chunk, err := ufest.NewChunk(chunkNum, lom)
				Expect(err).NotTo(HaveOccurred())

				createTestChunk(chunk.Path(), int(thisChunkSize), originalChecksum)

				err = ufest.Add(chunk, thisChunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())

				totalBytesWritten += thisChunkSize
			}

			// Verify we wrote the expected amount
			Expect(totalBytesWritten).To(Equal(int64(fileSize)))
			Expect(ufest.Size()).To(Equal(int64(fileSize)))

			// Store manifest (this will mark it as completed)
			err = lom.CompleteUfest(ufest)
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
				totalBytesRead, ufest.Count(), originalSum))
		})

		It("should handle edge cases correctly", func() {
			By("testing empty file")
			testObject := "chunked/empty-file.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, 0)
			ufest, err := core.NewUfest("empty-test-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			// Create single empty chunk
			chunk, err := ufest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())

			createTestChunk(chunk.Path(), 0, nil) // Create empty file, no xxhash needed

			err = ufest.Add(chunk, 0, 1)
			Expect(err).NotTo(HaveOccurred())

			err = lom.CompleteUfest(ufest)
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
			fqn2 := mis[0].MakePathFQN(&localBckB, fs.ObjCT, "incomplete.bin")
			createTestFile(fqn2, 0)
			lom2 := newBasicLom(fqn2)
			ufest2, err := core.NewUfest("incomplete-test-"+cos.GenTie(), lom2, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			// Don't call Store() - manifest remains incomplete
			_, err = ufest2.NewReader()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("incomplete"))
		})

		It("should validate chunk size consistency", func() {
			testObject := "chunked/size-test.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN)
			ufest, err := core.NewUfest("size-test-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			// Create chunk that's smaller than declared size
			chunk, err := ufest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())

			// Create file with only 100 bytes using our helper
			createTestChunk(chunk.Path(), 100, nil)

			// But claim it's 200 bytes in the manifest
			err = ufest.Add(chunk, 200, 1)
			Expect(err).NotTo(HaveOccurred())

			lom.SetSize(200)
			err = lom.CompleteUfest(ufest)
			Expect(err).NotTo(HaveOccurred())

			reader, err := ufest.NewReader()
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				Expect(reader.Close()).NotTo(HaveOccurred())
			}()

			// Reading should fail with unexpected EOF
			buffer := make([]byte, 300)
			_, err = reader.Read(buffer)
			Expect(err).To(MatchError(ContainSubstring("truncated")))
		})

		It("should handle multiple small chunks", func() {
			const (
				numChunks     = 100
				chunkSize     = 1 * cos.MiB
				totalFileSize = numChunks * chunkSize
			)

			testObject := "chunked/many-small-chunks.bin"
			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)

			createTestFile(localFQN, 0)
			lom := newBasicLom(localFQN, totalFileSize)
			ufest, err := core.NewUfest("multi-chunk-test-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			originalChecksum := onexxh.New64()

			// Create many small chunks
			for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
				chunk, err := ufest.NewChunk(chunkNum, lom)
				Expect(err).NotTo(HaveOccurred())

				// Create chunk and update checksum in one step
				createTestChunk(chunk.Path(), chunkSize, originalChecksum)

				err = ufest.Add(chunk, chunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())
			}

			err = lom.CompleteUfest(ufest)
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

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)
			// Create initial LOM for the object (no need for empty file - chunks are the real data)
			lom := newBasicLom(localFQN)

			// Create Ufest (multipart upload session)
			ufest, err := core.NewUfest("mpu-test-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())

			// Add chunks to simulate MPU
			for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
				chunk, err := ufest.NewChunk(chunkNum, lom)
				Expect(err).NotTo(HaveOccurred())

				// Create actual chunk file
				createTestChunk(chunk.Path(), chunkSize, nil)

				err = ufest.Add(chunk, chunkSize, int64(chunkNum))
				Expect(err).NotTo(HaveOccurred())
			}

			// Complete the MPU - this should set the chunked flag
			err = lom.CompleteUfest(ufest)
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

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)

			createTestFile(localFQN, totalSize)
			lom := newBasicLom(localFQN)
			lom.SetSize(totalSize)
			lom.SetAtimeUnix(time.Now().UnixNano())

			lom.Lock(true)
			err := lom.PersistMain(true /*isChunked*/)
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

			localFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)
			lom := newBasicLom(localFQN)

			// Set up as chunked object
			ufest, err := core.NewUfest("bug-test-"+cos.GenTie(), lom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())
			chunk, err := ufest.NewChunk(1, lom)
			Expect(err).NotTo(HaveOccurred())
			createTestChunk(chunk.Path(), chunkSize, nil)

			ufest.Add(chunk, chunkSize, 1)

			err = lom.CompleteUfest(ufest)
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
			localFQN      = mis[0].MakePathFQN(&localBckB, fs.ObjCT, testObject)
		)
		By("Step 1: Setting up chunked upload with sequential parts")
		lom := newBasicLom(localFQN)

		uploadID := "complete-test-" + cos.GenTie()
		manifest, err := core.NewUfest(uploadID, lom, false /*must-exist*/)
		Expect(err).NotTo(HaveOccurred())

		// track expected checksums
		var (
			expectedWholeMD5 = cos.NewCksumHash(cos.ChecksumMD5)
			partMD5s         [][]byte
		)
		// create chunks in sorted order; compute "whole" MD5 as well
		for partNum := 1; partNum <= numParts; partNum++ {
			chunk, err := manifest.NewChunk(partNum, lom)
			Expect(err).NotTo(HaveOccurred())

			partMD5 := creatChunkMD5andWhole(chunk.Path(), partSize, expectedWholeMD5.H)
			partMD5s = append(partMD5s, partMD5)

			chunk.MD5 = partMD5

			err = manifest.Add(chunk, int64(partSize), int64(partNum))
			Expect(err).NotTo(HaveOccurred())
		}

		By("Step 2: Validate manifest state before completion")
		Expect(manifest.Count()).To(Equal(numParts), "Should have correct number of parts")
		Expect(manifest.Size()).To(Equal(int64(totalFileSize)), "Should have correct total size")
		Expect(manifest.Completed()).To(BeFalse(), "Should not be completed yet")

		By("Step 3: Simulate complete-upload validation logic")
		manifest.Lock()

		Expect(manifest.Count()).To(BeNumerically(">=", numParts),
			"Should have at least the requested number of parts")

		for i := range numParts {
			expectedPartNum := i + 1
			actualChunk := manifest.GetChunk(expectedPartNum, true /*locked*/)
			Expect(actualChunk).NotTo(BeNil(), "Part %d should exist", expectedPartNum)
			Expect(int(actualChunk.Num())).To(Equal(expectedPartNum),
				"Part should have correct sequential number")
			Expect(bytes.Equal(actualChunk.MD5, partMD5s[i])).To(BeTrue(), "Part should have correct MD5")
		}

		manifest.Unlock()

		By("Step 4: Compute whole-object checksum (production lines 378-395)")
		wholeCksum := cos.NewCksumHash(cos.ChecksumMD5) // Local bucket uses MD5
		err = manifest.ComputeWholeChecksum(wholeCksum)
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
		chunkedFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, chunkedObject)
		Expect(cos.CreateDir(filepath.Dir(chunkedFQN))).NotTo(HaveOccurred())

		chunkedLom := newBasicLom(chunkedFQN)

		// cleanup
		chunkedLom.RemoveMain()

		uploadID := "range-test-" + cos.GenTie()
		manifest, err := core.NewUfest(uploadID, chunkedLom, false /*must-exist*/)
		Expect(err).NotTo(HaveOccurred())

		// Create deterministic test data - we'll recreate this same data for monolithic
		seed := int64(12345)    // Fixed seed for reproducible data
		var allChunkData []byte // Collect all chunk data to create identical monolithic

		for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
			chunk, err := manifest.NewChunk(chunkNum, chunkedLom)
			Expect(err).NotTo(HaveOccurred())

			// Create chunk and collect its data for monolithic reconstruction
			chunkData := createDeterministicChunk(chunk.Path(), chunkSize, seed+int64(chunkNum))
			allChunkData = append(allChunkData, chunkData...)

			err = manifest.Add(chunk, int64(chunkSize), int64(chunkNum))
			Expect(err).NotTo(HaveOccurred())
		}

		// complete
		err = chunkedLom.CompleteUfest(manifest)
		Expect(err).NotTo(HaveOccurred())

		By("Step 2: Create identical monolithic object")
		monolithicFQN := mis[1].MakePathFQN(&localBckB, fs.ObjCT, monolithicObject)
		Expect(cos.CreateDir(filepath.Dir(monolithicFQN))).NotTo(HaveOccurred())

		totalFileSize := manifest.Size()

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

	It("should handle concurrent uploads with unique IDs and serialized completion", func() {
		const numUploads = 4

		results := make(chan error, numUploads)

		for i := range numUploads {
			go func(idx int) {
				defer GinkgoRecover()

				objName := fmt.Sprintf("parallel/upload-test-%d.bin", idx)
				objFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, objName)
				lom := newBasicLom(objFQN, 0) // Size will come from chunks

				manifest, err := core.NewUfest(fmt.Sprintf("upload-%d", idx), lom, false)
				if err != nil {
					results <- err
					return
				}

				// Write and add chunk - size accumulates automatically
				chunk, err := manifest.NewChunk(1, lom)
				if err != nil {
					results <- err
					return
				}
				createTestChunk(chunk.Path(), int(cos.MiB), nil)

				err = manifest.Add(chunk, cos.MiB, 1)
				if err != nil {
					results <- err
					return
				}

				// Complete via LOM - this is the correct interface
				results <- lom.CompleteUfest(manifest)
			}(i)
		}

		// Verify all completed successfully
		for range numUploads {
			Expect(<-results).NotTo(HaveOccurred())
		}

		By("All concurrent completions succeeded")
	})

	It("completes multiple concurrent uploads (unique IDs) and verifies content", func() {
		const numUploads = 3

		type uploadInfo struct {
			lom          *core.LOM
			expectedSize int64
			expectedMD5  []byte
			err          error
		}

		results := make(chan uploadInfo, numUploads)

		var wg sync.WaitGroup
		wg.Add(numUploads)

		// Spawn workers (Go 1.22 integer range)
		for i := range numUploads {
			go func(idx int) {
				defer GinkgoRecover()
				defer wg.Done()

				objName := fmt.Sprintf("parallel/upload-test-%d.bin", idx)
				objFQN := mis[idx%len(mis)].MakePathFQN(&localBckB, fs.ObjCT, objName)
				lom := newBasicLom(objFQN, 0) // size comes from chunks

				manifest, err := core.NewUfest(fmt.Sprintf("upload-%d", idx), lom, false)
				if err != nil {
					results <- uploadInfo{lom: lom, err: err}
					return
				}

				// 2–3 chunks
				var (
					numChunks    = 2 + rand.IntN(2) // 2 or 3
					allChunkData []byte
					chunkSize    = rand.IntN(400*cos.KiB) + 50*cos.KiB + rand.IntN(1000)
				)
				for chunkNum := 1; chunkNum <= numChunks; chunkNum++ {
					chunk, err := manifest.NewChunk(chunkNum, lom)
					if err != nil {
						results <- uploadInfo{lom: lom, err: err}
						return
					}

					seed := int64(idx*100 + chunkNum)
					chunkData := createDeterministicChunk(chunk.Path(), chunkSize, seed)
					allChunkData = append(allChunkData, chunkData...)

					if err := manifest.Add(chunk, int64(chunkSize), int64(chunkNum)); err != nil {
						results <- uploadInfo{lom: lom, err: err}
						return
					}
				}

				// expected size + MD5 over concatenated chunks
				expectedSize := int64(len(allChunkData))
				h := md5.New()
				h.Write(allChunkData)
				expectedMD5 := h.Sum(nil)

				// complete
				err = lom.CompleteUfest(manifest)
				results <- uploadInfo{
					lom:          lom,
					expectedSize: expectedSize,
					expectedMD5:  expectedMD5,
					err:          err,
				}
			}(i)
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		// collect and verify completions
		var completedUploads []uploadInfo
		for u := range results {
			Expect(u.err).NotTo(HaveOccurred(), "complete should succeed for %q", u.lom.Cname())
			completedUploads = append(completedUploads, u)
		}
		Expect(len(completedUploads)).To(Equal(numUploads), "should receive all results")

		By("All concurrent completions succeeded — verifying on-disk objects")

		// verify each completed object (size + whole-object MD5), then clean up
		for i, upload := range completedUploads {
			upload.lom.Lock(false) // -----------------------------------------

			err := upload.lom.Load(false, true /*locked*/)
			Expect(err).NotTo(HaveOccurred(), "should load completed object %d (%s)", i, upload.lom.Cname())

			actualSize := upload.lom.Lsize(true)
			Expect(actualSize).To(Equal(upload.expectedSize),
				"size mismatch for object %d (%s)", i, upload.lom.Cname())

			handle, err := upload.lom.NewHandle(true)
			Expect(err).NotTo(HaveOccurred(), "should create handle for object %d (%s)", i, upload.lom.Cname())

			actualData := make([]byte, actualSize)
			n, err := handle.ReadAt(actualData, 0)
			handle.Close()
			Expect(err).NotTo(HaveOccurred(), "should read object %d (%s)", i, upload.lom.Cname())
			Expect(int64(n)).To(Equal(actualSize), "should read full object %d (%s)", i, upload.lom.Cname())

			upload.lom.Unlock(false) // -----------------------------------------

			h := md5.New()
			h.Write(actualData[:n])
			actualMD5 := h.Sum(nil)
			Expect(actualMD5).To(Equal(upload.expectedMD5),
				"checksum mismatch for object %d (%s)", i, upload.lom.Cname())

			By(fmt.Sprintf("Verified object %d: %s (size=%d bytes, checksum OK)", i, upload.lom.Cname(), actualSize))

			// Cleanup
			err = upload.lom.RemoveMain()
			Expect(err).NotTo(HaveOccurred(), "cleanup should remove %s", upload.lom.Cname())
		}

		By(fmt.Sprintf("Successfully verified %d concurrent multi-chunk uploads", numUploads))
	})

	It("races completes for one object (3 uploads) and ensures final content equals one contender", func() {
		const numWorkers = 3

		type workerResult struct {
			expectedSize int64
			expectedMD5  []byte
			err          error
		}

		// one common object name
		objName := "parallel/one-key-race.bin"
		objFQN := mis[0].MakePathFQN(&localBckB, fs.ObjCT, objName)
		lom := newBasicLom(objFQN, 0)

		results := make(chan workerResult, numWorkers)

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for i := range numWorkers {
			go func(worker int) {
				defer GinkgoRecover()
				defer wg.Done()

				uploadID := fmt.Sprintf("complete-manifest-race-%d", worker)
				manifest, err := core.NewUfest(uploadID, lom, false)
				if err != nil {
					results <- workerResult{err: err}
					return
				}

				var (
					numChunks = 2 + rand.IntN(2) // 2 or 3
					chunkSize = rand.IntN(400*cos.KiB) + 50*cos.KiB + rand.IntN(1000)
					allData   []byte
				)
				for part := 1; part <= numChunks; part++ {
					chunk, err := manifest.NewChunk(part, lom)
					if err != nil {
						results <- workerResult{err: err}
						return
					}
					seed := int64(worker*1000 + part)
					data := createDeterministicChunk(chunk.Path(), chunkSize, seed) // writes file and returns bytes
					allData = append(allData, data...)

					if err := manifest.Add(chunk, int64(chunkSize), int64(part)); err != nil {
						results <- workerResult{err: err}
						return
					}
				}

				h := md5.New()
				h.Write(allData)
				expMD5 := h.Sum(nil)
				expSize := int64(len(allData))

				// complete (API should serialize)
				err = lom.CompleteUfest(manifest)

				results <- workerResult{
					expectedSize: expSize,
					expectedMD5:  expMD5,
					err:          err,
				}
			}(i)
		}

		go func() { wg.Wait(); close(results) }()

		// collect and assert all completions succeeded
		var wrs []workerResult
		for r := range results {
			Expect(r.err).NotTo(HaveOccurred(), "each CompleteUfest should succeed")
			wrs = append(wrs, r)
		}
		Expect(len(wrs)).To(Equal(numWorkers), "should receive all worker results")

		By("All concurrent completions succeeded — verifying final on-disk object matches one of the contenders")

		// load and read final object
		lom.Lock(false)
		Expect(lom.Load(false, true /*locked*/)).NotTo(HaveOccurred())
		finalSize := lom.Lsize(true)

		handle, err := lom.NewHandle(true)
		Expect(err).NotTo(HaveOccurred())
		buf := make([]byte, finalSize)
		n, err := handle.ReadAt(buf, 0)
		_ = handle.Close()
		lom.Unlock(false)

		Expect(err).NotTo(HaveOccurred())
		Expect(int64(n)).To(Equal(finalSize))

		wh := md5.New()
		wh.Write(buf[:n])
		finalMD5 := wh.Sum(nil)

		// final content must equal one of the workers' payloads (the last finisher)
		matched := false
		for _, r := range wrs {
			if r.expectedSize == finalSize && bytes.Equal(r.expectedMD5, finalMD5) {
				matched = true
				break
			}
		}
		Expect(matched).To(BeTrue(), "final object must match one of the completed manifests")

		Expect(lom.RemoveMain()).NotTo(HaveOccurred())
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
