// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	siShardBucket  = "si-test-shards"
	siShardBucket2 = "si-test-shards-2"
)

// siContent maps each TAR entry name to the readers.Reader used to write it.
// The reader carries the xxhash checksum computed during writing.
type siContent map[string]readers.Reader

func siArchLOM(t GinkgoTInterface, name string) *core.LOM {
	return siArchLOMInBucket(t, siShardBucket, name)
}

func siArchLOMInBucket(t GinkgoTInterface, bckName, name string) *core.LOM {
	t.Helper()
	lom := &core.LOM{ObjName: name}
	if err := lom.InitCmnBck(&cmn.Bck{Name: bckName, Provider: apc.AIS}); err != nil {
		t.Fatalf("siArchLOMInBucket InitCmnBck: %v", err)
	}
	// Create a stub shard file so SaveShardIndex can persist xattr on archlom.
	fh, err := cos.CreateFile(lom.FQN)
	if err != nil {
		t.Fatalf("siArchLOMInBucket CreateFile %s: %v", lom.FQN, err)
	}
	cos.Close(fh)
	// Seed minimal metadata that PersistMain validates before writing xattr.
	lom.SetAtimeUnix(time.Now().UnixNano())
	return lom
}

// siMakeTAR creates a temporary TAR with count regular files of random sizes and content.
func siMakeTAR(t GinkgoTInterface, tmpDir string, format tar.Format, count int) (*os.File, int64, siContent) {
	t.Helper()
	tmp, err := os.CreateTemp(tmpDir, "shard_*.tar")
	if err != nil {
		t.Fatalf("siMakeTAR CreateTemp: %v", err)
	}
	maxSize := int64(4 * cos.KiB)
	if !testing.Short() {
		maxSize = int64(32 * cos.KiB)
	}
	content := make(siContent, count)
	tw := tar.NewWriter(tmp)
	for i := range count {
		name := fmt.Sprintf("file_%04d.bin", i)
		size := rand.Int64N(maxSize) // [0, maxSize); varied, non-uniform offsets
		var r readers.Reader
		if size == 0 {
			r = readers.NewBytes(nil)
		} else {
			r, err = readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: cos.ChecksumOneXxh})
			if err != nil {
				t.Fatalf("siMakeTAR readers.New(size=%d): %v", size, err)
			}
		}
		content[name] = r
		hdr := &tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Size:     size,
			Format:   format,
			ModTime:  time.Now(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("siMakeTAR WriteHeader %q: %v", name, err)
		}
		if size > 0 {
			if _, err := io.Copy(tw, r); err != nil {
				t.Fatalf("siMakeTAR Write %q: %v", name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("siMakeTAR Close: %v", err)
	}
	// After Close the position is at EOF — use it as the file size directly.
	size, err := tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("siMakeTAR tell: %v", err)
	}
	return tmp, size, content
}

// siSave persists the stub archlom's initial metadata (simulating a real PUT's xattr write)
func siSave(archlom *core.LOM, idx *archive.ShardIndex) error {
	archlom.Lock(true)
	err := archlom.PersistMain(false)
	archlom.Unlock(true)
	if err != nil {
		return err
	}
	return core.SaveShardIndex(archlom, idx)
}

// siShardCksum computes the xxhash checksum of the first `size` bytes of fh.
func siShardCksum(t GinkgoTInterface, fh *os.File, size int64) *cos.Cksum {
	t.Helper()
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("siShardCksum Seek: %v", err)
	}
	_, ckh, err := cos.CopyAndChecksum(io.Discard, io.LimitReader(fh, size), nil, cos.ChecksumOneXxh)
	if err != nil {
		t.Fatalf("siShardCksum CopyAndChecksum: %v", err)
	}
	return ckh.Clone()
}

// siVerifyContent confirms that every entry in loaded can be used to read back
// the correct content from the archive file via DataOffset().
func siVerifyContent(fh *os.File, loaded *archive.ShardIndex, content siContent) {
	for name, entry := range loaded.Entries {
		if entry.Size == 0 {
			continue // nothing to checksum for zero-size entries
		}
		section := io.NewSectionReader(fh, entry.DataOffset(), entry.Size)
		n, actual, err := cos.CopyAndChecksum(io.Discard, section, nil, cos.ChecksumOneXxh)
		Expect(err).NotTo(HaveOccurred(), "CopyAndChecksum entry %q", name)
		Expect(n).To(Equal(entry.Size), "entry %q: read byte count mismatch", name)
		Expect(actual.Equal(content[name].Cksum())).To(BeTrue(), "entry %q: xxhash mismatch at DataOffset %d", name, entry.DataOffset())
	}
}

var _ = Describe("SaveShardIndex / LoadShardIndex", func() {
	var (
		tmpDir string
		mpath  string
	)

	siBMD := mock.NewBaseBownerMock(
		meta.NewBck(cmn.SysShardIdx, apc.AIS, cmn.NsGlobal, &cmn.Bprops{}),
		meta.NewBck(siShardBucket, apc.AIS, cmn.NsGlobal, &cmn.Bprops{}),
		meta.NewBck(siShardBucket2, apc.AIS, cmn.NsGlobal, &cmn.Bprops{}),
	)

	BeforeEach(func() {
		tmpDir = GinkgoT().TempDir()
		mpath = filepath.Join(tmpDir, "mpath")
		cos.CreateDir(mpath)
		fs.Add(mpath, "daeID")

		sysBck := (*cmn.Bck)(meta.SysBckShardIdx())
		for _, mi := range fs.GetAvail() {
			mi.CreateMissingBckDirs(sysBck)
		}

		mock.NewTarget(siBMD)
	})

	AfterEach(func() {
		fs.Remove(mpath)
	})

	Describe("LoadShardIndex", func() {
		It("returns nil when no index has been saved yet", func() {
			archlom := siArchLOM(GinkgoT(), "nosuchshard.tar")
			idx, err := core.LoadShardIndex(archlom)
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(BeNil())
		})
	})

	Describe("round-trip", func() {
		DescribeTable("Save then Load returns identical entries and valid offsets",
			func(format tar.Format) {
				const nFiles = 20

				fh, size, content := siMakeTAR(GinkgoT(), tmpDir, format, nFiles)
				defer fh.Close()

				orig, err := archive.BuildShardIndex(fh, size)
				Expect(err).NotTo(HaveOccurred())
				Expect(orig.Entries).To(HaveLen(nFiles))

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("shard_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Entries).To(HaveLen(nFiles))

				for name, want := range orig.Entries {
					have, ok := loaded.Entries[name]
					Expect(ok).To(BeTrue(), "entry %q missing after LoadShardIndex", name)
					Expect(have.Offset).To(Equal(want.Offset), "entry %q: Offset mismatch", name)
					Expect(have.Size).To(Equal(want.Size), "entry %q: Size mismatch", name)
				}
				siVerifyContent(fh, loaded, content)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("empty shard (zero entries) round-trips correctly",
			func(format tar.Format) {
				fh, size, content := siMakeTAR(GinkgoT(), tmpDir, format, 0)
				defer fh.Close()

				orig, err := archive.BuildShardIndex(fh, size)
				Expect(err).NotTo(HaveOccurred())
				Expect(orig.Entries).To(BeEmpty())

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("empty_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Entries).To(BeEmpty())
				siVerifyContent(fh, loaded, content)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("sets HasShardIdx on archlom after save",
			func(format tar.Format) {
				fh, sz, content := siMakeTAR(GinkgoT(), tmpDir, format, 5)
				defer fh.Close()
				idx, err := archive.BuildShardIndex(fh, sz)
				Expect(err).NotTo(HaveOccurred())

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("flagged_%s.tar", format))
				Expect(archlom.HasShardIdx()).To(BeFalse())
				Expect(siSave(archlom, idx)).To(Succeed())
				Expect(archlom.HasShardIdx()).To(BeTrue())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				siVerifyContent(fh, loaded, content)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("overwrites an existing index with a new one",
			func(format tar.Format) {
				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("overwrite_%s.tar", format))

				fh1, size1, _ := siMakeTAR(GinkgoT(), tmpDir, format, 5)
				defer fh1.Close()
				first, err := archive.BuildShardIndex(fh1, size1)
				Expect(err).NotTo(HaveOccurred())
				Expect(siSave(archlom, first)).To(Succeed())

				fh2, size2, content2 := siMakeTAR(GinkgoT(), tmpDir, format, 15)
				defer fh2.Close()
				second, err := archive.BuildShardIndex(fh2, size2)
				Expect(err).NotTo(HaveOccurred())
				Expect(siSave(archlom, second)).To(Succeed())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Entries).To(HaveLen(15))
				siVerifyContent(fh2, loaded, content2)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("saving one shard does not affect another shard's index",
			func(format tar.Format) {
				archlomA := siArchLOM(GinkgoT(), fmt.Sprintf("shard-a_%s.tar", format))
				archlomB := siArchLOM(GinkgoT(), fmt.Sprintf("shard-b_%s.tar", format))

				fhA, szA, contentA := siMakeTAR(GinkgoT(), tmpDir, format, 3)
				defer fhA.Close()
				idxA, err := archive.BuildShardIndex(fhA, szA)
				Expect(err).NotTo(HaveOccurred())

				fhB, szB, contentB := siMakeTAR(GinkgoT(), tmpDir, format, 7)
				defer fhB.Close()
				idxB, err := archive.BuildShardIndex(fhB, szB)
				Expect(err).NotTo(HaveOccurred())

				Expect(siSave(archlomA, idxA)).To(Succeed())
				Expect(siSave(archlomB, idxB)).To(Succeed())

				loadedA, err := core.LoadShardIndex(archlomA)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedA.Entries).To(HaveLen(3))
				siVerifyContent(fhA, loadedA, contentA)

				loadedB, err := core.LoadShardIndex(archlomB)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedB.Entries).To(HaveLen(7))
				siVerifyContent(fhB, loadedB, contentB)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("same object name in different buckets uses distinct index objects",
			func(format tar.Format) {
				// If idxObjName ignored the bucket, saving one would overwrite the other.
				shardName := fmt.Sprintf("shared_%s.tar", format)
				archlomA := siArchLOM(GinkgoT(), shardName)
				archlomB := siArchLOMInBucket(GinkgoT(), siShardBucket2, shardName)

				fhA, szA, contentA := siMakeTAR(GinkgoT(), tmpDir, format, 4)
				defer fhA.Close()
				idxA, err := archive.BuildShardIndex(fhA, szA)
				Expect(err).NotTo(HaveOccurred())

				fhB, szB, contentB := siMakeTAR(GinkgoT(), tmpDir, format, 9)
				defer fhB.Close()
				idxB, err := archive.BuildShardIndex(fhB, szB)
				Expect(err).NotTo(HaveOccurred())

				Expect(siSave(archlomA, idxA)).To(Succeed())
				Expect(siSave(archlomB, idxB)).To(Succeed())

				loadedA, err := core.LoadShardIndex(archlomA)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedA.Entries).To(HaveLen(4))
				siVerifyContent(fhA, loadedA, contentA)

				loadedB, err := core.LoadShardIndex(archlomB)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedB.Entries).To(HaveLen(9))
				siVerifyContent(fhB, loadedB, contentB)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("shard with a nested object path round-trips correctly",
			func(format tar.Format) {
				// ObjName with slashes: idxObjName produces a multi-level path inside
				// .sys-shardidx, exercising the slow-path directory creation in lom._cf.
				fh, sz, content := siMakeTAR(GinkgoT(), tmpDir, format, 10)
				defer fh.Close()
				orig, err := archive.BuildShardIndex(fh, sz)
				Expect(err).NotTo(HaveOccurred())

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("a/b/c/shard_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Entries).To(HaveLen(10))
				siVerifyContent(fh, loaded, content)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		DescribeTable("large index (1000 entries) round-trips correctly",
			func(format tar.Format) {
				if testing.Short() {
					Skip("skipping large-index test in short mode")
				}
				const nFiles = 1000

				fh, sz, content := siMakeTAR(GinkgoT(), tmpDir, format, nFiles)
				defer fh.Close()
				orig, err := archive.BuildShardIndex(fh, sz)
				Expect(err).NotTo(HaveOccurred())
				Expect(orig.Entries).To(HaveLen(nFiles))

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("large_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := core.LoadShardIndex(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Entries).To(HaveLen(nFiles))

				for name, want := range orig.Entries {
					have, ok := loaded.Entries[name]
					Expect(ok).To(BeTrue(), "entry %q missing", name)
					Expect(have.Offset).To(Equal(want.Offset), "entry %q: Offset mismatch", name)
					Expect(have.Size).To(Equal(want.Size), "entry %q: Size mismatch", name)
				}
				siVerifyContent(fh, loaded, content)
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)
	})

	Describe("staleness", func() {
		It("returns ErrShardIdxStale after shard is re-uploaded", func() {
			// Build original shard, stamp index with its cksum/size.
			fh1, size1, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 10)
			defer fh1.Close()
			ck1 := siShardCksum(GinkgoT(), fh1, size1)

			orig, err := archive.BuildShardIndex(fh1, size1)
			Expect(err).NotTo(HaveOccurred())
			orig.SrcCksum = ck1
			orig.SrcSize = size1

			archlom := siArchLOM(GinkgoT(), "reupload.tar")
			archlom.SetCksum(ck1)
			archlom.SetSize(size1)
			Expect(siSave(archlom, orig)).To(Succeed())

			// Cksum matches — index is fresh.
			loaded, err := core.LoadShardIndex(archlom)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).NotTo(BeNil())

			// Re-upload: new shard with different content → different cksum/size.
			fh2, size2, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh2.Close()
			ck2 := siShardCksum(GinkgoT(), fh2, size2)

			archlom.SetCksum(ck2)
			archlom.SetSize(size2)

			// Index now stale — must return ErrShardIdxStale.
			idx, err := core.LoadShardIndex(archlom)
			Expect(errors.Is(err, archive.ErrShardIdxStale)).To(BeTrue())
			Expect(idx).To(BeNil())
			// HasShardIdx must be cleared so the next load rebuilds the index.
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})
	})

	Describe("corruption", func() {
		DescribeTable("reports checksum error when index payload is corrupted",
			func(format tar.Format) {
				shardName := fmt.Sprintf("corrupt_%s.tar", format)
				fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, format, 10)
				defer fh.Close()
				idx, err := archive.BuildShardIndex(fh, sz)
				Expect(err).NotTo(HaveOccurred())

				archlom := siArchLOM(GinkgoT(), shardName)
				Expect(siSave(archlom, idx)).To(Succeed())

				// Compute the index FQN directly using the same LOM machinery as SaveShardIndex.
				idxlom := &core.LOM{ObjName: archlom.Bck().SysObjName(archlom.ObjName + core.IdxSuffix)}
				Expect(idxlom.InitBck(meta.SysBckShardIdx())).To(Succeed())
				idxPath := idxlom.FQN
				data, err := os.ReadFile(idxPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(data)).To(BeNumerically(">", 11))
				data[11] ^= 0xFF
				Expect(os.WriteFile(idxPath, data, 0o644)).To(Succeed())

				_, err = core.LoadShardIndex(archlom)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("checksum mismatch"))
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)
	})
})
