// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"os"
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
			testFileSize   = int64(cos.MiB)
			testObjectName = "chunked/test-obj.bin"
			localFQN       = mix.MakePathFQN(&localBck, fs.ObjectType, testObjectName)
		)

		createChunkManifest := func(totalSize int64, numChunks uint16, chunkSizes []int64, id string, lom *core.LOM) *core.Ufest {
			manifest := core.NewUfest(id, lom)
			manifest.Size = totalSize
			manifest.Num = numChunks
			manifest.Chunks = make([]core.Uchunk, numChunks)

			for i := range numChunks {
				manifest.Chunks[i] = core.Uchunk{
					Siz:      chunkSizes[i],
					Num:      i + 1,
					Path:     trand.String(7),
					CksumVal: trand.String(16),
				}
			}
			return manifest
		}

		Describe("Constructor", func() {
			It("should create manifest with generated ID when empty", func() {
				manifest := core.NewUfest("", nil)

				Expect(manifest.ID).ToNot(BeEmpty())
				Expect(manifest.StartTime).ToNot(BeZero())
				Expect(manifest.Chunks).To(HaveLen(0))
				Expect(manifest.CksumTyp).To(Equal(cos.ChecksumOneXxh))

				// ID should contain timestamp
				timeStr := manifest.StartTime.Format(cos.StampSec2)
				Expect(manifest.ID).To(ContainSubstring(timeStr))
			})

			It("should create manifest with provided ID", func() {
				customID := "test-session-123"
				manifest := core.NewUfest(customID, nil)

				Expect(manifest.ID).To(Equal(customID))
				Expect(manifest.StartTime).ToNot(BeZero())
			})

			It("should create unique IDs for concurrent calls", func() {
				manifest1 := core.NewUfest("", nil)
				manifest2 := core.NewUfest("", nil)

				Expect(manifest1.ID).ToNot(Equal(manifest2.ID))
			})
		})

		Describe("Store and Load", func() {
			It("should store and load chunk manifest correctly", func() {
				// Create test file (chunk #1)
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Create chunk manifest for 3 chunks - ensure sizes sum to testFileSize
				chunkSizes := []int64{400000, 400000, 248576} // total = 1048576 (1MB)
				manifest := createChunkManifest(testFileSize, 3, chunkSizes, "test-session-001", lom)

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

				// Verify manifest contents including new fields
				Expect(loadedManifest.ID).To(Equal("test-session-001"))
				Expect(loadedManifest.StartTime.Unix()).To(Equal(manifest.StartTime.Unix()))
				Expect(loadedManifest.Size).To(Equal(testFileSize))
				Expect(loadedManifest.Num).To(Equal(uint16(3)))
				Expect(loadedManifest.CksumTyp).To(Equal(cos.ChecksumOneXxh))
				Expect(loadedManifest.Chunks).To(HaveLen(3))

				for i := range 3 {
					Expect(loadedManifest.Chunks[i].Siz).To(Equal(chunkSizes[i]))
					Expect(loadedManifest.Chunks[i].Path).To(Equal(manifest.Chunks[i].Path))
					Expect(loadedManifest.Chunks[i].CksumVal).To(Equal(manifest.Chunks[i].CksumVal))
				}
			})

			It("should handle single chunk manifest", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Single chunk manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes, "single-chunk-session", lom)

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal("single-chunk-session"))
				Expect(loadedManifest.Num).To(Equal(uint16(1)))
				Expect(loadedManifest.Chunks[0].Siz).To(Equal(testFileSize))
			})

			It("should handle many small chunks", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// 100 chunks of ~10KB each
				numChunks := uint16(100)
				chunkSize := testFileSize / int64(numChunks)
				chunkSizes := make([]int64, numChunks)
				for i := range numChunks {
					chunkSizes[i] = chunkSize
				}
				// Adjust last chunk for remainder
				chunkSizes[numChunks-1] += testFileSize % int64(numChunks)

				manifest := createChunkManifest(testFileSize, numChunks, chunkSizes, "many-chunks-session", lom)

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal("many-chunks-session"))
				Expect(loadedManifest.Num).To(Equal(numChunks))
				Expect(loadedManifest.Chunks).To(HaveLen(int(numChunks)))

				var totalSize int64
				for _, chunk := range loadedManifest.Chunks {
					totalSize += chunk.Siz
				}
				Expect(totalSize).To(Equal(testFileSize))
			})

			It("should preserve timestamp precision", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes, "timestamp-test", lom)

				// Set a specific time for testing
				testTime := time.Date(2025, 8, 2, 15, 30, 45, 0, time.UTC)
				manifest.StartTime = testTime

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				// Unix timestamp has second precision
				Expect(loadedManifest.StartTime.Unix()).To(Equal(testTime.Unix()))
			})
		})

		Describe("validation", func() {
			It("should fail when num doesn't match chunks length", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Create invalid manifest - num says 3 but only 2 chunks
				manifest := core.NewUfest("invalid-manifest", lom)
				manifest.Size = testFileSize
				manifest.Num = 3
				manifest.Chunks = []core.Uchunk{
					{Siz: 500000, Num: 1, Path: "a", CksumVal: "abc123"},
					{Siz: 524000, Num: 2, Path: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CksumVal: "def456"},
				}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid chunk-manifest"))
			})

			It("should fail when num is zero", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				manifest := core.NewUfest("zero-chunks", lom)
				manifest.Size = testFileSize
				manifest.Num = 0
				manifest.Chunks = []core.Uchunk{}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid chunk-manifest"))
			})

			It("should fail when manifest is too large for xattr", func() {
				numChunks := 1000
				chunkSize := 1024
				totalSize := int64(numChunks * chunkSize)
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, totalSize)

				// Create a manifest that will exceed xattr size limits
				// Use many chunks with long checksum values
				chunkSizes := make([]int64, numChunks)
				for i := range numChunks {
					chunkSizes[i] = int64(chunkSize)
				}

				manifest := createChunkManifest(totalSize, uint16(numChunks), chunkSizes, "large-manifest", lom)
				// Make values very long to exceed xattr limits
				for i := range manifest.Chunks {
					manifest.Chunks[i].Path = trand.String(1000)
					manifest.Chunks[i].CksumVal = trand.String(1000)
				}

				err := manifest.Store(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("too large"))
			})
		})

		Describe("serialization errors", func() {
			It("should fail when loading non-existent manifest", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				manifest := &core.Ufest{}
				err := manifest.Load(lom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("chunk-manifest"))
			})

			It("should fail when meta-version corrupted", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Store valid manifest first
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes, "version-test", lom)
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
				Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
			})

			It("should fail when checksum verification fails", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Store valid manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes, "checksum-test", lom)
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
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				// Store valid manifest
				chunkSizes := []int64{testFileSize}
				manifest := createChunkManifest(testFileSize, 1, chunkSizes, "truncated-test", lom)
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
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				manifest := core.NewUfest("empty-checksum-test", lom)
				manifest.Size = testFileSize
				manifest.Num = 2
				manifest.Chunks = []core.Uchunk{
					{Siz: 500000, Num: 1, CksumVal: ""},               // empty checksum
					{Siz: 548576, Num: 2, CksumVal: "valid_checksum"}, // total = 1048576
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal("empty-checksum-test"))
				Expect(loadedManifest.Chunks[0].CksumVal).To(Equal(""))
				Expect(loadedManifest.Chunks[1].CksumVal).To(Equal("valid_checksum"))
			})

			It("should handle zero-sized chunks", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				manifest := core.NewUfest("zero-size-test", lom)
				manifest.Size = testFileSize
				manifest.Num = 3
				manifest.Chunks = []core.Uchunk{
					{Siz: 0, Num: 1, Path: "a/b/c/ddd", CksumVal: "empty"}, // zero size
					{Siz: testFileSize, Num: 2, CksumVal: "full"},
					{Siz: 0, Num: 3, CksumVal: "empty2"}, // another zero - total = 1048576
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal("zero-size-test"))
				Expect(loadedManifest.Chunks[0].Siz).To(Equal(int64(0)))
				Expect(loadedManifest.Chunks[1].Siz).To(Equal(testFileSize))
				Expect(loadedManifest.Chunks[2].Siz).To(Equal(int64(0)))
			})

			It("should handle empty ID", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				manifest := core.NewUfest("", lom)
				manifest.Size = testFileSize
				manifest.Num = 1
				manifest.Chunks = []core.Uchunk{
					{Siz: testFileSize, Num: 1, Path: "test", CksumVal: "checksum"},
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal(manifest.ID)) // should preserve generated ID
				Expect(loadedManifest.ID).ToNot(BeEmpty())
			})

			It("should handle very long ID", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				longID := "very-long-session-id-" + trand.String(100)
				manifest := core.NewUfest(longID, lom)
				manifest.Size = testFileSize
				manifest.Num = 1
				manifest.Chunks = []core.Uchunk{
					{Siz: testFileSize, Num: 1, Path: "test", CksumVal: "checksum"},
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal(longID))
			})
		})

		Describe("ID and StartTime functionality", func() {
			It("should generate different IDs for sessions started at different times", func() {
				manifest1 := core.NewUfest("", nil)
				time.Sleep(time.Second) // ensure different timestamps
				manifest2 := core.NewUfest("", nil)

				Expect(manifest1.ID).ToNot(Equal(manifest2.ID))
				Expect(manifest1.StartTime.Before(manifest2.StartTime)).To(BeTrue())
			})

			It("should preserve custom ID through store/load cycle", func() {
				createDummyFile(localFQN)
				lom := newBasicLom(localFQN, testFileSize)

				customID := "my-custom-upload-session-12345"
				manifest := core.NewUfest(customID, lom)
				manifest.Size = testFileSize
				manifest.Num = 1
				manifest.Chunks = []core.Uchunk{
					{Siz: testFileSize, Num: 1, Path: "chunk1", CksumVal: "abc123"},
				}

				err := manifest.Store(lom)
				Expect(err).NotTo(HaveOccurred())

				loadedManifest := &core.Ufest{}
				err = loadedManifest.Load(lom)
				Expect(err).NotTo(HaveOccurred())

				Expect(loadedManifest.ID).To(Equal(customID))
			})

			It("should validate ID format in generated IDs", func() {
				manifest := core.NewUfest("", nil)

				// Generated ID should be UUID-compliant time-based format
				// GenTAID format: "t" + HHMMSS + "-" + 3-char-tie
				Expect(manifest.ID).To(MatchRegexp(`^t\d{6}-[a-zA-Z0-9_-]{3}`))

				// Should be valid UUID format
				Expect(cos.IsValidUUID(manifest.ID)).To(BeTrue())

				// Should contain time information
				Expect(manifest.ID).To(HavePrefix("t"))
			})
		})
	})
})

func createDummyFile(fqn string) {
	_ = os.Remove(fqn)
	_, err := cos.CreateFile(fqn)
	Expect(err).ShouldNot(HaveOccurred())
}
