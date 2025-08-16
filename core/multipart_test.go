// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"fmt"
	"io"
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
})
