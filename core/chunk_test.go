// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"os"

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

// on-disk xattr name for chunks
const (
	xattrChunk = "user.ais.chunk"
)

var _ = Describe("Chunk Manifest Xattrs", func() {
	const (
		tmpDir     = "/tmp/chunk_xattr_test"
		xattrMpath = tmpDir + "/xattr"

		bucketLocal = "CHUNK_TEST_Local"
	)

	localBck := cmn.Bck{Name: bucketLocal, Provider: apc.AIS, Ns: cmn.NsGlobal}

	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.ObjChunkType, &fs.ObjChunkContentResolver{}, true)

	var (
		mix     = fs.Mountpath{Path: xattrMpath}
		bmdMock = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketLocal, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, BID: 201},
			),
		)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(xattrMpath)
		_, _ = fs.Add(xattrMpath, "daeID")
		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_, _ = fs.Remove(xattrMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("chunk manifest", func() {
		var (
			testFileSize   = int64(1024 * 1024) // 1MB
			testObjectName = "chunked/test-obj.bin"
			localFQN       = mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
		)

		createChunkManifest := func(totalSize int64, numChunks uint16, chunkSizes []int64) *core.Ufest {
			manifest := &core.Ufest{
				Size:     totalSize,
				Num:      numChunks,
				CksumTyp: cos.ChecksumOneXxh,
				Chunks:   make([]core.Uchunk, numChunks),
			}

			for i := range numChunks {
				manifest.Chunks[i] = core.Uchunk{
					Siz:      chunkSizes[i],
					CksumVal: trand.String(16), // mock checksum
				}
			}
			return manifest
		}

		Describe("Store and Load", func() {
			It("should store and load chunk manifest correctly", func() {
				// Create test file (chunk #1)
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Create chunk manifest for 3 chunks - ensure sizes sum to testFileSize
				chunkSizes := []int64{400000, 400000, 248576} // total = 1048576 (1MB)
				manifest := createChunkManifest(testFileSize, 3, chunkSizes)

				// Store manifest
				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				// Verify xattr was written
				b, err := fs.GetXattr(localFQN, xattrChunk)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				// Load manifest from disk
				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				// Verify manifest contents
				Expect(loadedManifest.Size).To(Equal(testFileSize))
				Expect(loadedManifest.Num).To(Equal(uint16(3)))
				Expect(loadedManifest.CksumTyp).To(Equal(cos.ChecksumOneXxh))
				Expect(loadedManifest.Chunks).To(HaveLen(3))

				for i := range 3 {
					Expect(loadedManifest.Chunks[i].Siz).To(Equal(chunkSizes[i]))
					Expect(loadedManifest.Chunks[i].CksumVal).To(Equal(manifest.Chunks[i].CksumVal))
				}
			})

			It("should handle single chunk manifest", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Single chunk manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes)

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.Num).To(Equal(uint16(1)))
				Expect(loadedManifest.Chunks[0].Siz).To(Equal(testFileSize))
			})

			It("should handle many small chunks", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// 100 chunks of ~10KB each
				numChunks := uint16(100)
				chunkSize := testFileSize / int64(numChunks)
				chunkSizes := make([]int64, numChunks)
				for i := range numChunks {
					chunkSizes[i] = chunkSize
				}
				// Adjust last chunk for remainder
				chunkSizes[numChunks-1] += testFileSize % int64(numChunks)

				manifest := createChunkManifest(testFileSize, numChunks, chunkSizes)

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.Num).To(Equal(numChunks))
				Expect(loadedManifest.Chunks).To(HaveLen(int(numChunks)))

				var totalSize int64
				for _, chunk := range loadedManifest.Chunks {
					totalSize += chunk.Siz
				}
				Expect(totalSize).To(Equal(testFileSize))
			})
		})

		Describe("validation", func() {
			It("should fail when num doesn't match chunks length", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Create invalid manifest - num says 3 but only 2 chunks
				manifest := &core.Ufest{
					Size:     testFileSize,
					Num:      3,
					CksumTyp: cos.ChecksumOneXxh,
					Chunks: []core.Uchunk{
						{Siz: 500000, CksumVal: "abc123"},
						{Siz: 524000, CksumVal: "def456"},
					},
				}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid manifest num=3, len=2"))
			})

			It("should fail when num is zero", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				manifest := &core.Ufest{
					Size:     testFileSize,
					Num:      0,
					CksumTyp: cos.ChecksumOneXxh,
					Chunks:   []core.Uchunk{},
				}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid manifest num=0"))
			})

			It("should fail when manifest is too large for xattr", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Create a manifest that will exceed xattr size limits
				// Use many chunks with long checksum values
				numChunks := uint16(1000)
				chunkSizes := make([]int64, numChunks)
				for i := range numChunks {
					chunkSizes[i] = 1024
				}

				manifest := createChunkManifest(int64(numChunks)*1024, numChunks, chunkSizes)
				// Make checksum values very long to exceed xattr limits
				for i := range manifest.Chunks {
					manifest.Chunks[i].CksumVal = trand.String(1000)
				}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("manifest too large"))
			})
		})

		Describe("serialization errors", func() {
			It("should fail when loading non-existent manifest", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				manifest := &core.Ufest{}
				err := manifest.Load(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("chunk-manifest"))
			})

			It("should fail when metadata version is invalid", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Store valid manifest first
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes)
				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				// Corrupt the version byte
				b, err := fs.GetXattr(localFQN, xattrChunk)
				Expect(err).NotTo(HaveOccurred())
				b[0] = 99 // invalid version
				Expect(fs.SetXattr(localFQN, xattrChunk, b)).NotTo(HaveOccurred())

				// Loading should fail
				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unsupported chunk-manifest meta-version 99"))
			})

			It("should fail when checksum verification fails", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Store valid manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes)
				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, xattrChunk)
				Expect(err).NotTo(HaveOccurred())

				// corrupting data that's already been parsed but is still part of the checksummed payload.
				corruptIdx := len(b) - 10
				b[corruptIdx]++

				Expect(fs.SetXattr(localFQN, xattrChunk, b)).NotTo(HaveOccurred())

				// Loading should fail due to checksum mismatch
				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
			})

			It("should fail when xattr data is truncated", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				// Store valid manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes)
				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				// Truncate the xattr data
				b, err := fs.GetXattr(localFQN, xattrChunk)
				Expect(err).NotTo(HaveOccurred())
				truncated := b[:len(b)/2] // cut in half
				Expect(fs.SetXattr(localFQN, xattrChunk, truncated)).NotTo(HaveOccurred())

				// Loading should fail
				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).To(HaveOccurred())
			})
		})

		Describe("packing/unpacking edge cases", func() {
			It("should handle empty checksum values", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				manifest := &core.Ufest{
					Size:     testFileSize,
					Num:      2,
					CksumTyp: cos.ChecksumOneXxh,
					Chunks: []core.Uchunk{
						{Siz: 500000, CksumVal: ""},               // empty checksum
						{Siz: 548576, CksumVal: "valid_checksum"}, // total = 1048576
					},
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.Chunks[0].CksumVal).To(Equal(""))
				Expect(loadedManifest.Chunks[1].CksumVal).To(Equal("valid_checksum"))
			})

			It("should handle zero-sized chunks", func() {
				createTestFile(localFQN, int(testFileSize))
				lom := NewBasicLom(localFQN)

				manifest := &core.Ufest{
					Size:     testFileSize,
					Num:      3,
					CksumTyp: cos.ChecksumOneXxh,
					Chunks: []core.Uchunk{
						{Siz: 0, CksumVal: "empty"}, // zero size
						{Siz: testFileSize, CksumVal: "full"},
						{Siz: 0, CksumVal: "empty2"}, // another zero - total = 1048576
					},
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.Chunks[0].Siz).To(Equal(int64(0)))
				Expect(loadedManifest.Chunks[1].Siz).To(Equal(testFileSize))
				Expect(loadedManifest.Chunks[2].Siz).To(Equal(int64(0)))
			})
		})
	})
})
