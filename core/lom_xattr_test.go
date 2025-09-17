// Package core_test provides tests for cluster package
/*
* Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"

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

// on-disk xattr name
const (
	xattrLOM = "user.ais.lom"
)

var _ = Describe("LOM Xattributes", func() {
	const (
		tmpDir     = "/tmp/lom_xattr_test"
		xattrMpath = tmpDir + "/xattr"
		copyMpath  = tmpDir + "/copy"

		bucketLocal  = "LOM_TEST_Local"
		bucketCached = "LOM_TEST_Cached"
	)

	localBck := cmn.Bck{Name: bucketLocal, Provider: apc.AIS, Ns: cmn.NsGlobal}
	cachedBck := cmn.Bck{Name: bucketCached, Provider: apc.AIS, Ns: cmn.NsGlobal}

	var (
		copyMpathInfo *fs.Mountpath
		mix           = fs.Mountpath{Path: xattrMpath}
		bmdMock       = mock.NewBaseBownerMock(
			meta.NewBck(
				bucketLocal, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumOneXxh}, BID: 201},
			),
			meta.NewBck(
				bucketCached, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{
					Cksum:       cmn.CksumConf{Type: cos.ChecksumOneXxh},
					WritePolicy: cmn.WritePolicyConf{Data: apc.WriteImmediate, MD: apc.WriteNever},
					BID:         202,
				},
			),
		)
	)

	BeforeEach(func() {
		_ = cos.CreateDir(xattrMpath)
		_ = cos.CreateDir(copyMpath)

		_, _ = fs.Add(xattrMpath, "daeID")
		_, _ = fs.Add(copyMpath, "daeID")

		available := fs.GetAvail()
		copyMpathInfo = available[copyMpath]

		_ = mock.NewTarget(bmdMock)
	})

	AfterEach(func() {
		_, _ = fs.Remove(xattrMpath)
		_, _ = fs.Remove(copyMpath)
		_ = os.RemoveAll(tmpDir)
	})

	Describe("xattrs", func() {
		var (
			testFileSize   = 456
			testObjectName = "xattr-foldr/test-obj.ext"

			// Bucket needs to have checksum enabled
			localFQN  = mix.MakePathFQN(&localBck, fs.ObjCT, testObjectName+".qqq")
			cachedFQN = mix.MakePathFQN(&cachedBck, fs.ObjCT, testObjectName)

			fqns []string
		)

		BeforeEach(func() {
			fqns = []string{
				copyMpathInfo.MakePathFQN(&localBck, fs.ObjCT, "copy/111/fqn"),
				copyMpathInfo.MakePathFQN(&localBck, fs.ObjCT, "other/copy/fqn"),
			}

			// NOTE:
			// the test creates copies; there's a built-in assumption that `mi` will be
			// the HRW mountpath,
			// while `mi2` will not (and, therefore, can be used to place the copy).
			// Ultimately, this depends on the specific HRW hash; adding
			// Expect(lom.IsHRW()).To(Be...)) to catch that sooner.

			for _, fqn := range fqns {
				lom := filePut(fqn, testFileSize)
				Expect(lom.IsHRW()).To(BeFalse())
			}
		})

		Describe("Persist", func() {
			It("should save correct meta to disk", func() {
				lom := filePut(localFQN, testFileSize)
				Expect(lom.IsHRW()).To(BeTrue())
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
				lom.SetVersion("dummy_version")
				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, xattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				newLom := newBasicLom(localFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
				Expect(lom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should _not_ save meta to disk", func() {
				lom := filePut(cachedFQN, testFileSize)
				Expect(lom.IsHRW()).To(BeTrue())
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
				lom.SetVersion("dummy_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(cachedFQN, xattrLOM)
				Expect(b).To(BeEmpty())
				Expect(err).To(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				ver := lom.Version()
				lom.UncacheUnless()

				newLom := newBasicLom(cachedFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(ver).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
				Expect(lom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should copy object with meta in memory", func() {
				lom := filePut(cachedFQN, testFileSize)
				lom.Lock(true)
				defer lom.Unlock(true)
				cksumHash, err := lom.ComputeCksum(lom.CksumType(), true)
				Expect(err).NotTo(HaveOccurred())
				lom.SetCksum(cksumHash.Clone())
				lom.SetVersion("first_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "etag",
					cmn.CRC32CObjMD: "crc32",
				})
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				lom.SetVersion("second_version")
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(cachedFQN, xattrLOM)
				Expect(b).To(BeEmpty())
				Expect(err).To(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				lom.UncacheUnless()
				lom.Load(true, false)
				Expect(lom.Version()).To(BeEquivalentTo("second_version"))
				Expect(lom.GetCopies()).To(HaveLen(3))

				buf := make([]byte, cos.KiB)
				newLom, err := lom.Copy2FQN(cachedFQN+"-copy", buf)
				Expect(err).NotTo(HaveOccurred())

				err = newLom.Load(false, false)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(newLom.GetCustomMD()).To(HaveLen(3))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))
			})

			It("should copy chunked object with meta correctly", func() {
				// Create chunked object similar to cached object test but with chunks
				lom := createChunkedLOM(cachedFQN, 2)
				lom.Lock(true)
				defer lom.Unlock(true)

				lom.SetVersion("chunked_version")
				lom.SetCustomMD(cos.StrKVs{
					cmn.SourceObjMD: apc.GCP,
					cmn.ETag:        "chunked_etag",
				})

				Expect(persist(lom)).NotTo(HaveOccurred())
				lom.UncacheUnless()

				dst := newBasicLom(cachedFQN + "-chunked-copy")
				dst.Lock(true)
				defer dst.Unlock(true)

				// Copy the chunked object
				buf := make([]byte, cos.KiB)
				newLom, err := lom.Copy2FQN(dst.FQN, buf)
				Expect(err).NotTo(HaveOccurred())

				// Verify copied object
				Expect(newLom.IsChunked()).To(BeTrue())
				Expect(newLom.Load(false, false)).NotTo(HaveOccurred())
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(newLom.GetCustomMD()).To(HaveLen(2))
				Expect(lom.GetCustomMD()).To(BeEquivalentTo(newLom.GetCustomMD()))

				// Verify chunks were copied correctly
				srcUfest, err := core.NewUfest("", lom, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(srcUfest.LoadCompleted(lom)).NotTo(HaveOccurred())

				dstUfest, err := core.NewUfest("", newLom, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(dstUfest.LoadCompleted(newLom)).NotTo(HaveOccurred())

				Expect(srcUfest.Count()).To(Equal(dstUfest.Count()))

				// Verify individual chunks were copied correctly
				for i := 1; i <= srcUfest.Count(); i++ {
					srcChunk := srcUfest.GetChunk(i, false)
					dstChunk := dstUfest.GetChunk(i, false)
					Expect(srcChunk).NotTo(BeNil())
					Expect(dstChunk).NotTo(BeNil())
					Expect(srcChunk.Path()).To(BeARegularFile())
					Expect(dstChunk.Path()).To(BeARegularFile())
				}

				// Final validation: Compare full object content using lom.Open() readers
				srcReader, err := lom.Open()
				Expect(err).NotTo(HaveOccurred())
				defer srcReader.Close()

				dstReader, err := newLom.Open()
				Expect(err).NotTo(HaveOccurred())
				defer dstReader.Close()

				// Read and compare entire content
				srcContent, err := io.ReadAll(srcReader)
				Expect(err).NotTo(HaveOccurred())
				dstContent, err := io.ReadAll(dstReader)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(srcContent)).To(Equal(len(dstContent)))
				Expect(srcContent).To(Equal(dstContent))
			})

			It("should override old values", func() {
				lom := filePut(localFQN, testFileSize)
				lom.Lock(true)
				defer lom.Unlock(true)
				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
				lom.SetVersion("dummy_version1")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())

				lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
				lom.SetVersion("dummy_version2")
				Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom)).NotTo(HaveOccurred())

				b, err := fs.GetXattr(localFQN, xattrLOM)
				Expect(b).ToNot(BeEmpty())
				Expect(err).NotTo(HaveOccurred())

				hrwLom := &core.LOM{ObjName: testObjectName}
				Expect(hrwLom.InitBck(&localBck)).NotTo(HaveOccurred())
				hrwLom.UncacheUnless()

				newLom := newBasicLom(localFQN)
				err = newLom.Load(false, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(lom.Checksum()).To(BeEquivalentTo(newLom.Checksum()))
				Expect(lom.Version()).To(BeEquivalentTo(newLom.Version()))
				Expect(lom.GetCopies()).To(HaveLen(3))
				Expect(lom.GetCopies()).To(BeEquivalentTo(newLom.GetCopies()))
			})
		})

		Describe("LoadMetaFromFS", func() {
			It("should read fresh meta from fs", func() {
				createTestFile(localFQN, testFileSize)
				lom1 := newBasicLom(localFQN)
				lom2 := newBasicLom(localFQN)
				lom1.Lock(true)
				defer lom1.Unlock(true)
				lom1.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
				lom1.SetVersion("dummy_version")
				Expect(lom1.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(lom1.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
				Expect(persist(lom1)).NotTo(HaveOccurred())

				err := lom2.LoadMetaFromFS()
				Expect(err).NotTo(HaveOccurred())

				Expect(lom1.Checksum()).To(BeEquivalentTo(lom2.Checksum()))
				Expect(lom1.Version(true)).To(BeEquivalentTo(lom2.Version(true)))
				Expect(lom1.GetCopies()).To(HaveLen(3))
				Expect(lom1.GetCopies()).To(BeEquivalentTo(lom2.GetCopies()))
			})

			Describe("error cases", func() {
				var lom *core.LOM

				BeforeEach(func() {
					createTestFile(localFQN, testFileSize)
					lom = newBasicLom(localFQN)
					lom.Lock(true)
					defer lom.Unlock(true)
					lom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "deadbeefcafebabe"))
					lom.SetVersion("dummy_version")
					Expect(lom.AddCopy(fqns[0], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(lom.AddCopy(fqns[1], copyMpathInfo)).NotTo(HaveOccurred())
					Expect(persist(lom)).NotTo(HaveOccurred())
				})

				It("should fail when checksum does not match", func() {
					b, err := fs.GetXattr(localFQN, xattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[2]++ // changing first byte of meta checksum
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
				})

				It("should fail when checksum type is invalid", func() {
					b, err := fs.GetXattr(localFQN, xattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 200 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown checksum 200"))

					b, err = fs.GetXattr(localFQN, xattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[1] = 0 // corrupting checksum type
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown checksum 0"))
				})

				It("should fail when metadata version is invalid", func() {
					b, err := fs.GetXattr(localFQN, xattrLOM)
					Expect(err).NotTo(HaveOccurred())

					b[0] = 128 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown LOM meta-version 128"))

					b[0] = 0 // corrupting metadata version
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: unknown LOM meta-version 0"))
				})

				It("should fail when metadata is too short", func() {
					Expect(fs.SetXattr(localFQN, xattrLOM, []byte{1})).NotTo(HaveOccurred())
					err := lom.LoadMetaFromFS()
					Expect(err).To(HaveOccurred())

					Expect(fs.SetXattr(localFQN, xattrLOM, []byte{1, 1, 2})).NotTo(HaveOccurred())
					err = lom.LoadMetaFromFS()
					Expect(err).To(MatchError("bad lmeta: too short (3)"))
				})

				It("should fail when meta is corrupted", func() {
					// This test is supposed to end with LoadMetaFromFS error
					// not with nil pointer exception / panic
					b, err := fs.GetXattr(localFQN, xattrLOM)
					Expect(err).NotTo(HaveOccurred())
					copy(b[40:], "1321wr")
					Expect(fs.SetXattr(localFQN, xattrLOM, b)).NotTo(HaveOccurred())

					err = lom.LoadMetaFromFS()
					Expect(err.Error()).To(ContainSubstring("BAD META CHECKSUM"))
				})
			})
		})

		Describe("MetaverLOM=2", func() {
			It("persists MetaverLOM=2 and round-trips basic fields", func() {
				obj := "lomv2/smoke-" + cos.GenTie()
				fqn := mix.MakePathFQN(&localBck, fs.ObjCT, obj)

				// create empty file
				Expect(cos.CreateDir(filepath.Dir(fqn))).NotTo(HaveOccurred())
				f, err := cos.CreateFile(fqn)
				Expect(err).NotTo(HaveOccurred())
				Expect(f.Close()).To(Succeed())

				// persist (writer now emits v2)
				lom := newBasicLom(fqn)
				lom.SetSize(12345) // any size

				// TODO: set flag lom.md.lid = lom.md.lid.setlmfl(lmflChunk); check upon loading

				Expect(persist(lom)).To(Succeed())

				// read raw xattr → first byte is metadata version
				raw, err := fs.GetXattr(fqn, xattrLOM)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(raw)).To(BeNumerically(">=", 10))    // >= prefLen
				Expect(raw[0]).To(Equal(byte(core.MetaverLOM))) // MetaverLOM == 2

				// reload and sanity-check size
				fresh := newBasicLom(fqn)
				Expect(fresh.Load(false, false)).To(Succeed())
				Expect(fresh.Lsize()).To(Equal(int64(12345)))
			})

			It("loads legacy v1 lmeta and upgrades to v2 on persist", func() {
				obj := "lomv2/coexist-" + cos.GenTie()
				fqn := mix.MakePathFQN(&localBck, fs.ObjCT, obj)

				// ----- build minimal v1 blob: [cksumT|sep][cksumV|sep][size] -----
				// constants from v1 writer/parser
				// NOTE: duplicating actual values here for strictly-testing purposes
				const (
					prefLen         = 10
					mdCksumTyXXHash = 1
					recordSepa      = "\xe3/\xbd"
				)
				const (
					packedCksumT = iota
					packedCksumV
					packedVer
					packedSize
					packedCopies
					packedCustom
					packedLid
				)
				wantSize := uint64(777777)

				createTestFile(fqn, int(wantSize))

				var payload bytes.Buffer
				writeRec := func(key uint16, val []byte, addSep bool) {
					var k [2]byte
					binary.BigEndian.PutUint16(k[:], key)
					payload.Write(k[:])
					if len(val) > 0 {
						payload.Write(val)
					}
					if addSep {
						payload.WriteString(recordSepa)
					}
				}

				// cksum type/value for object metadata (any pair is fine)
				writeRec(packedCksumT, cos.UnsafeB(cos.ChecksumMD5), true)
				writeRec(packedCksumV, cos.UnsafeB(strings.Repeat("0", 32)), true)

				// size (u64 BE), last record (no trailing sepa)
				var b8 [8]byte
				binary.BigEndian.PutUint64(b8[:], wantSize)
				writeRec(packedSize, b8[:], false)

				// preamble (v1) + xxhash(payload)
				raw := make([]byte, prefLen+payload.Len())
				raw[0] = 1 // MetaverLOM_V1
				raw[1] = mdCksumTyXXHash
				copy(raw[prefLen:], payload.Bytes())
				sum := onexxh.Checksum64S(raw[prefLen:], cos.MLCG32)
				binary.BigEndian.PutUint64(raw[2:], sum)

				// write legacy v1 xattr
				Expect(fs.SetXattr(fqn, xattrLOM, raw)).To(Succeed())

				// Load legacy v1 *without* enforcing BID (policy: no adoption on Load)
				lom := newBasicLom(fqn)
				Expect(lom.LoadMetaFromFS()).To(Succeed())
				Expect(lom.Lsize(true)).To(Equal(int64(wantSize)))

				// Persist should rewrite as v2
				Expect(persist(lom)).To(Succeed())

				// Verify xattr version flipped to 2
				got, err := fs.GetXattr(fqn, xattrLOM)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(got)).To(BeNumerically(">=", prefLen))
				Expect(got[0]).To(Equal(byte(2)))

				// After upgrade, full Load() should succeed (BID stamped during persist)
				fresh := newBasicLom(fqn)
				Expect(fresh.Load(false, false)).To(Succeed())
			})

		})
	})
})

// createChunkedLOM helper for creating chunked objects in xattr tests
func createChunkedLOM(fqn string, numChunks int) *core.LOM {
	const chunkSize = 16 * cos.KiB

	lom := &core.LOM{}
	err := lom.InitFQN(fqn, nil)
	Expect(err).NotTo(HaveOccurred())

	totalSize := int64(numChunks * chunkSize)
	lom.SetSize(totalSize)

	// Create main object file
	createTestFile(fqn, int(totalSize))

	// Create Ufest for chunked upload
	ufest, err := core.NewUfest("", lom, false)
	Expect(err).NotTo(HaveOccurred())

	// Create chunks
	for i := 1; i <= numChunks; i++ {
		chunk, err := ufest.NewChunk(i, lom)
		Expect(err).NotTo(HaveOccurred())

		createTestFile(chunk.Path(), chunkSize)

		err = ufest.Add(chunk, int64(chunkSize), int64(i))
		Expect(err).NotTo(HaveOccurred())
	}

	// Complete the Ufest - this handles chunked flag setting and persistence internally
	err = lom.CompleteUfest(ufest)
	Expect(err).NotTo(HaveOccurred())

	// Reload to pick up chunked flag
	lom.UncacheUnless()
	Expect(lom.Load(false, false)).NotTo(HaveOccurred())
	Expect(lom.IsChunked()).To(BeTrue())
	return lom
}
