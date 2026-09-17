// Package core_test provides tests for cluster package
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core_test

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	onexxh "github.com/OneOfOne/xxhash"

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
	if err := os.RemoveAll(lom.FQN); err != nil {
		t.Fatalf("siArchLOMInBucket RemoveAll %s: %v", lom.FQN, err)
	}
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

// - make the stub source reflect the metadata captured by BuildShardIndex
// - persist it (simulating a real PUT's xattr write)
func siSave(archlom *core.LOM, idx *archive.ShardIndex) error {
	if err := os.Truncate(archlom.FQN, idx.SrcSize()); err != nil {
		return err
	}
	archlom.SetSize(idx.SrcSize())
	archlom.SetCksum(idx.SrcCksum().Clone())
	archlom.Lock(true)
	err := archlom.PersistMain(false)
	archlom.Unlock(true)
	if err != nil {
		return err
	}
	return archlom.SaveShardIndex(idx)
}

// call LoadShardIndex with the shard read-locked
func siLoad(archlom *core.LOM) (*archive.ShardIndex, error) {
	archlom.Lock(false)
	defer archlom.Unlock(false)
	idx, err := archlom.LoadShardIndex()
	if idx != nil {
		DeferCleanup(idx.Free)
	}
	return idx, err
}

// siSetShardSize gives the stub archlom a real on-disk size, so that the size guard in
// LoadShardIndex (and IsStale) sees something other than zero.
func siSetShardSize(t GinkgoTInterface, archlom *core.LOM, size int64) {
	t.Helper()
	if err := os.Truncate(archlom.FQN, size); err != nil {
		t.Fatalf("siSetShardSize Truncate %s: %v", archlom.FQN, err)
	}
	archlom.SetSize(size)
	archlom.SetCksum(cos.NoneCksum)
}

func siIdxLOM(t GinkgoTInterface, archlom *core.LOM) *core.LOM {
	t.Helper()
	idxlom := &core.LOM{ObjName: archlom.Bck().SysObjName(archlom.ObjName + core.IdxSuffix)}
	if err := idxlom.InitBck(meta.SysBckShardIdx()); err != nil {
		t.Fatalf("siIdxLOM InitBck: %v", err)
	}
	return idxlom
}

func siIdxPath(t GinkgoTInterface, archlom *core.LOM) string { return siIdxLOM(t, archlom).FQN }

// siShardCksum computes the xxhash checksum of the first `size` bytes of fh.
func siShardCksum(t GinkgoTInterface, fh *os.File, size int64) *cos.Cksum {
	t.Helper()
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("siShardCksum Seek: %v", err)
	}
	_, ckh, err := cos.ChecksumReader(io.LimitReader(fh, size), cos.ChecksumOneXxh)
	if err != nil {
		t.Fatalf("siShardCksum CopyAndChecksum: %v", err)
	}
	return ckh.Clone()
}

// siVerifyContent confirms that every entry in loaded can be used to read back
// the correct content from the archive file via DataOffset().
func siVerifyContent(fh *os.File, loaded *archive.ShardIndex, content siContent) {
	for name, entry := range loaded.AllTestOnly() {
		if entry.Size == 0 {
			continue // nothing to checksum for zero-size entries
		}
		section := io.NewSectionReader(fh, entry.DataOffset(), entry.Size)
		n, actual, err := cos.ChecksumReader(section, cos.ChecksumOneXxh)
		Expect(err).NotTo(HaveOccurred(), "CopyAndChecksum entry %q", name)
		Expect(n).To(Equal(entry.Size), "entry %q: read byte count mismatch", name)
		Expect(actual.Equal(content[name].Cksum())).To(BeTrue(), "entry %q: xxhash mismatch at DataOffset %d", name, entry.DataOffset())
	}
}

// copy the TAR into the stub archlom, then save its index
func siSaveTAR(archlom *core.LOM, fh *os.File, size int64) {
	orig, err := archive.BuildShardIndex(fh, size, siShardCksum(GinkgoT(), fh, size))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(orig.Free)
	dst, err := os.OpenFile(archlom.FQN, os.O_WRONLY|os.O_TRUNC, 0)
	Expect(err).NotTo(HaveOccurred())
	_, err = io.Copy(dst, io.NewSectionReader(fh, 0, size))
	cos.Close(dst)
	Expect(err).NotTo(HaveOccurred())
	Expect(siSave(archlom, orig)).To(Succeed())
}

// read archpath the way GET does; report whether the index (fast path) served it
func siRead(archlom *core.LOM, archpath string) (fast bool) { //nolint:unparam // pass it for possible future ext-s
	fh, err := os.Open(archlom.FQN)
	Expect(err).NotTo(HaveOccurred())
	defer fh.Close()
	archlom.Lock(false)
	defer archlom.Unlock(false)
	csl, err := archlom.NewArchpathReader(fh, archpath, archive.ExtTar)
	Expect(err).NotTo(HaveOccurred())
	defer csl.Close()
	_, fast = csl.(*cos.SectionHandle)
	_, err = io.Copy(io.Discard, csl)
	Expect(err).NotTo(HaveOccurred())
	return fast
}

func siPrimeCache(archlom *core.LOM, archpath string) { //nolint:unparam // pass it for possible future ext-s
	GinkgoHelper()
	before := core.SidxStats()
	if siRead(archlom, archpath) {
		return
	}
	if core.SidxStats().Deny > before.Deny {
		Skip("cache admission denied under high memory load")
	}
	Fail("expected the shard index to be loaded")
}

// load a fresh LOM from disk, bypassing the in-memory LOM cache
func siReload(archlom *core.LOM) *core.LOM {
	GinkgoHelper()
	archlom.UncacheDel()
	fresh := &core.LOM{ObjName: archlom.ObjName}
	Expect(fresh.InitCmnBck(archlom.Bucket())).To(Succeed())
	fresh.Lock(false)
	defer fresh.Unlock(false)
	Expect(fresh.Load(false /*cache it*/, true /*locked*/)).To(Succeed())
	return fresh
}

// finalize new content the way PUT (append, cold GET) does: work file => RenameFinalize => persist
func siRewrite(archlom *core.LOM, fh *os.File, size int64) {
	GinkgoHelper()
	wfqn := archlom.GenFQN(fs.WorkCT, fs.WorkfilePut)
	wfh, err := archlom.CreateWork(wfqn)
	Expect(err).NotTo(HaveOccurred())
	_, err = io.Copy(wfh, io.NewSectionReader(fh, 0, size))
	cos.Close(wfh)
	Expect(err).NotTo(HaveOccurred())

	archlom.Lock(true)
	defer archlom.Unlock(true)
	Expect(archlom.RenameFinalize(wfqn)).To(Succeed())
	Expect(archlom.PersistMain(false /*chunked*/)).To(Succeed())
}

// source read-locked, destination write-locked (correctly)
func siCopy(src, dst *core.LOM) {
	GinkgoHelper()
	src.Lock(false)
	defer src.Unlock(false)
	dst.Lock(true)
	defer dst.Unlock(true)
	Expect(src.Load(false /*cache it*/, true /*locked*/)).To(Succeed())
	dst2, err := src.Copy2FQN(dst.FQN, make([]byte, 32*cos.KiB))
	Expect(err).NotTo(HaveOccurred())
	core.FreeLOM(dst2)
}

func siRmIdx(archlom *core.LOM) {
	idxlom := siIdxLOM(GinkgoT(), archlom)
	idxlom.UncacheDel()
	Expect(os.Remove(idxlom.FQN)).To(Succeed())
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
		fs.AddTestMpath(mpath, "daeID")

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
			idx, err := siLoad(archlom)
			Expect(err).NotTo(HaveOccurred())
			Expect(idx).To(BeNil())
		})
	})

	Describe("SaveShardIndex", func() {
		It("rejects a freed index before creating the sidecar", func() {
			idx, err := archive.NewShardIndexTestOnly(nil, 1,
				map[string]archive.ShardIndexEntry{"file": {Offset: 0, Size: 1}})
			Expect(err).NotTo(HaveOccurred())
			idx.Free()

			archlom := siArchLOM(GinkgoT(), "freed-index.tar")
			Expect(archlom.SaveShardIndex(idx)).To(MatchError(ContainSubstring("index is not built")))
			Expect(archlom.HasShardIdx()).To(BeFalse())
			Expect(siIdxPath(GinkgoT(), archlom)).NotTo(BeAnExistingFile())
		})

		It("saves an index for a source outside its HRW mountpath", func() {
			otherPath := filepath.Join(tmpDir, "other")
			Expect(cos.CreateDir(otherPath)).To(Succeed())
			mi, err := fs.AddTestMpath(otherPath, "daeID")
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { fs.Remove(otherPath) })
			Expect(mi.CreateMissingBckDirs(meta.SysBckShardIdx().Bucket())).To(Succeed())
			archlom := siArchLOM(GinkgoT(), "misplaced.tar")
			idx, err := archive.NewShardIndexTestOnly(cos.NoneCksum, cos.KiB,
				map[string]archive.ShardIndexEntry{"file": {Offset: 0, Size: 1}})
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)
			Expect(siSave(archlom, idx)).To(Succeed())
			archlom.UncacheDel()
			if mi == archlom.Mountpath() {
				mi = fs.GetAvail()[mpath]
			}
			fqn := mi.MakePathFQN(archlom.Bucket(), fs.ObjCT, archlom.ObjName)
			Expect(cos.CreateDir(filepath.Dir(fqn))).To(Succeed())
			Expect(os.Rename(archlom.FQN, fqn)).To(Succeed())
			Expect(archlom.InitFQN(fqn, nil)).To(Succeed())
			Expect(archlom.IsHRW()).To(BeFalse())
			Expect(archlom.SaveShardIndex(idx)).To(Succeed())
			Expect(archlom.FQN).To(Equal(fqn))
			Expect(archlom.HasShardIdx()).To(BeTrue())
		})

		It("rejects a TAR replaced between building and saving its index", func() {
			source := siArchLOM(GinkgoT(), "replaced.tar")
			put := func(name string) *core.LOM {
				lom := &core.LOM{ObjName: source.ObjName}
				Expect(lom.InitCmnBck(source.Bucket())).To(Succeed())
				fh, err := os.CreateTemp(tmpDir, "replacement-*.tar")
				Expect(err).NotTo(HaveOccurred())
				defer fh.Close()
				tw := tar.NewWriter(fh)
				Expect(tw.WriteHeader(&tar.Header{Name: name, Size: 1, Mode: 0o644, Typeflag: tar.TypeReg})).To(Succeed())
				_, err = tw.Write([]byte(name))
				Expect(err).NotTo(HaveOccurred())
				Expect(tw.Close()).To(Succeed())
				size, err := fh.Seek(0, io.SeekCurrent)
				Expect(err).NotTo(HaveOccurred())
				lom.SetSize(size)
				lom.SetCksum(siShardCksum(GinkgoT(), fh, size))
				lom.SetAtimeUnix(time.Now().UnixNano())
				lom.Lock(true)
				defer lom.Unlock(true)
				Expect(os.Rename(fh.Name(), lom.FQN)).To(Succeed())
				Expect(lom.PersistMain(false)).To(Succeed())
				return lom
			}
			build := func(lom *core.LOM) *archive.ShardIndex {
				lom.Lock(false)
				defer lom.Unlock(false)
				Expect(lom.Load(false, true)).To(Succeed())
				fh, err := lom.Open()
				Expect(err).NotTo(HaveOccurred())
				defer fh.Close()
				idx, err := archive.BuildShardIndex(fh, lom.Lsize(), lom.Checksum())
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idx.Free)
				return idx
			}

			original := put("a")
			idx := build(original)
			// Replace after releasing the scan's read lock, keeping the old LOM and index.
			replacement := put("b")
			Expect(replacement.Lsize()).To(Equal(original.Lsize()))
			Expect(idx.IsStale(original.Checksum(), original.Lsize())).To(BeFalse())
			err := original.SaveShardIndex(idx)
			Expect(cmn.IsErrBusy(err)).To(BeTrue(), "%v", err)
			Expect(err).To(MatchError(ContainSubstring("rewritten while indexing")))
			Expect(siIdxPath(GinkgoT(), original)).NotTo(BeAnExistingFile())

			// Reload from disk: the failed commit must not have set HasShardIdx.
			replacement.UncacheDel()
			rebuilt := build(replacement)
			Expect(replacement.HasShardIdx()).To(BeFalse())
			Expect(replacement.SaveShardIndex(rebuilt)).To(Succeed())
			Expect(replacement.HasShardIdx()).To(BeTrue())
			replacement.Lock(false)
			loaded, err := replacement.LoadShardIndex()
			replacement.Unlock(false)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).NotTo(BeNil())
			defer loaded.Free()
			_, ok := loaded.Lookup("b")
			Expect(ok).To(BeTrue())
			_, ok = loaded.Lookup("a")
			Expect(ok).To(BeFalse())
		})

		DescribeTable("validates current source before committing", func(change string, cached bool) {
			archlom := siArchLOM(GinkgoT(), "recreated.tar")
			archlom.SetVersion("old-version")
			archlom.SetCustomKey("old-key", "old-value")
			cksum := cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef")
			idx, err := archive.NewShardIndexTestOnly(cksum, cos.KiB,
				map[string]archive.ShardIndexEntry{"file": {Offset: 0, Size: 1}})
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)
			Expect(siSave(archlom, idx)).To(Succeed())

			original := archlom.Bprops()
			if change == "recreated" {
				props := *original
				DeferCleanup(func() { siBMD.Set(archlom.Bck(), original) })
				props.BID = core.NewBID(12345, true /*isAIS*/)
				siBMD.Set(archlom.Bck(), &props)
			}
			replacement := siArchLOM(GinkgoT(), archlom.ObjName)
			siSetShardSize(GinkgoT(), replacement, cos.KiB)
			replacement.SetCksum(cksum)
			switch change {
			case "size":
				siSetShardSize(GinkgoT(), replacement, 2*cos.KiB)
				replacement.SetCksum(cksum)
			case "checksum":
				replacement.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "fedcba9876543210"))
			}
			replacement.Lock(true)
			Expect(replacement.PersistMain(false)).To(Succeed())
			replacement.Unlock(true)
			if !cached || change == "missing" {
				replacement.UncacheDel()
			}
			if change == "missing" {
				Expect(os.Remove(replacement.FQN)).To(Succeed())
			}
			if change == "deleted" {
				Expect(siBMD.Del(replacement.Bck())).To(BeTrue())
				DeferCleanup(func() { siBMD.Set(archlom.Bck(), original) })
			}
			err = archlom.SaveShardIndex(idx)
			switch change {
			case "recreated":
				// handle from the previous bucket incarnation
				Expect(cmn.IsErrObjDefunct(err)).To(BeTrue(), "%v", err)
				return
			case "deleted":
				var notFound *cmn.ErrBckNotFound
				Expect(errors.As(err, &notFound)).To(BeTrue(), "%v", err)
				return
			case "missing":
				Expect(cos.IsNotExist(err)).To(BeTrue(), "%v", err)
				return
			case "same":
				Expect(err).NotTo(HaveOccurred())
				Expect(archlom.Bprops()).To(BeIdenticalTo(original))
			default:
				Expect(cmn.IsErrBusy(err)).To(BeTrue(), "%v", err)
			}
			replacement.UncacheDel()
			replacement.Lock(false)
			defer replacement.Unlock(false)
			Expect(replacement.Load(false, true)).To(Succeed())
			Expect(replacement.HasShardIdx()).To(Equal(change == "same"))
			Expect(replacement.Version()).To(BeEmpty())
			Expect(replacement.GetCustomMD()).To(BeEmpty())
		},
			Entry("matching content, cached", "same", true),
			Entry("matching content, persisted", "same", false),
			Entry("changed size", "size", false),
			Entry("changed checksum", "checksum", true),
			Entry("missing source", "missing", false),
			Entry("recreated bucket", "recreated", false),
			Entry("deleted bucket", "deleted", false),
		)
	})

	Describe("round-trip", func() {
		DescribeTable("Save then Load returns identical entries and valid offsets",
			func(format tar.Format) {
				const nFiles = 20

				fh, size, content := siMakeTAR(GinkgoT(), tmpDir, format, nFiles)
				defer fh.Close()

				orig, err := archive.BuildShardIndex(fh, size, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(orig.Free)
				Expect(orig.Len()).To(Equal(nFiles))

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("shard_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := siLoad(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Len()).To(Equal(nFiles))

				for name, want := range orig.AllTestOnly() {
					have, ok := loaded.Lookup(name)
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

				orig, err := archive.BuildShardIndex(fh, size, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(orig.Free)
				Expect(orig.Len()).To(BeZero())

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("empty_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := siLoad(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Len()).To(BeZero())
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
				idx, err := archive.BuildShardIndex(fh, sz, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idx.Free)

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("flagged_%s.tar", format))
				Expect(archlom.HasShardIdx()).To(BeFalse())
				Expect(siSave(archlom, idx)).To(Succeed())
				Expect(archlom.HasShardIdx()).To(BeTrue())

				loaded, err := siLoad(archlom)
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
				first, err := archive.BuildShardIndex(fh1, size1, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(first.Free)
				Expect(siSave(archlom, first)).To(Succeed())

				fh2, size2, content2 := siMakeTAR(GinkgoT(), tmpDir, format, 15)
				defer fh2.Close()
				second, err := archive.BuildShardIndex(fh2, size2, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(second.Free)
				Expect(siSave(archlom, second)).To(Succeed())

				loaded, err := siLoad(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Len()).To(Equal(15))
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
				idxA, err := archive.BuildShardIndex(fhA, szA, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idxA.Free)

				fhB, szB, contentB := siMakeTAR(GinkgoT(), tmpDir, format, 7)
				defer fhB.Close()
				idxB, err := archive.BuildShardIndex(fhB, szB, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idxB.Free)

				Expect(siSave(archlomA, idxA)).To(Succeed())
				Expect(siSave(archlomB, idxB)).To(Succeed())

				loadedA, err := siLoad(archlomA)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedA.Len()).To(Equal(3))
				siVerifyContent(fhA, loadedA, contentA)

				loadedB, err := siLoad(archlomB)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedB.Len()).To(Equal(7))
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
				idxA, err := archive.BuildShardIndex(fhA, szA, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idxA.Free)

				fhB, szB, contentB := siMakeTAR(GinkgoT(), tmpDir, format, 9)
				defer fhB.Close()
				idxB, err := archive.BuildShardIndex(fhB, szB, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idxB.Free)

				Expect(siSave(archlomA, idxA)).To(Succeed())
				Expect(siSave(archlomB, idxB)).To(Succeed())

				loadedA, err := siLoad(archlomA)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedA.Len()).To(Equal(4))
				siVerifyContent(fhA, loadedA, contentA)

				loadedB, err := siLoad(archlomB)
				Expect(err).NotTo(HaveOccurred())
				Expect(loadedB.Len()).To(Equal(9))
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
				orig, err := archive.BuildShardIndex(fh, sz, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(orig.Free)

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("a/b/c/shard_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := siLoad(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Len()).To(Equal(10))
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
				orig, err := archive.BuildShardIndex(fh, sz, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(orig.Free)
				Expect(orig.Len()).To(Equal(nFiles))

				archlom := siArchLOM(GinkgoT(), fmt.Sprintf("large_%s.tar", format))
				Expect(siSave(archlom, orig)).To(Succeed())

				loaded, err := siLoad(archlom)
				Expect(err).NotTo(HaveOccurred())
				Expect(loaded).NotTo(BeNil())
				Expect(loaded.Len()).To(Equal(nFiles))

				for name, want := range orig.AllTestOnly() {
					have, ok := loaded.Lookup(name)
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

			orig, err := archive.BuildShardIndex(fh1, size1, ck1)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(orig.Free)

			archlom := siArchLOM(GinkgoT(), "reupload.tar")
			archlom.SetCksum(ck1)
			archlom.SetSize(size1)
			Expect(siSave(archlom, orig)).To(Succeed())

			// Cksum matches — index is fresh.
			loaded, err := siLoad(archlom)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).NotTo(BeNil())

			// Re-upload: new shard with different content → different cksum/size.
			fh2, size2, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh2.Close()
			ck2 := siShardCksum(GinkgoT(), fh2, size2)

			archlom.SetCksum(ck2)
			archlom.SetSize(size2)

			// Index now stale — must return ErrShardIdxStale.
			idx, err := siLoad(archlom)
			Expect(errors.Is(err, archive.ErrShardIdxStale)).To(BeTrue())
			Expect(idx).To(BeNil())
			// HasShardIdx must be cleared so the next load rebuilds the index.
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})
	})

	Describe("removal", func() {
		It("removes the shard index when the shard object is removed", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "remove-index.tar")
			Expect(siSave(archlom, idx)).To(Succeed())

			idxPath := siIdxPath(GinkgoT(), archlom)
			Expect(idxPath).To(BeAnExistingFile())

			archlom.Lock(true)
			err = archlom.RemoveObj()
			archlom.Unlock(true)
			Expect(err).NotTo(HaveOccurred())
			Expect(idxPath).NotTo(BeAnExistingFile())
		})

		It("does not fail when the shard index is already absent", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "remove-missing-index.tar")
			Expect(siSave(archlom, idx)).To(Succeed())

			idxPath := siIdxPath(GinkgoT(), archlom)
			Expect(os.Remove(idxPath)).To(Succeed())

			archlom.Lock(true)
			err = archlom.RemoveObj()
			archlom.Unlock(true)
			Expect(err).NotTo(HaveOccurred())
		})

		It("keeps the shard index when main object removal fails", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "remove-main-fails.tar")
			Expect(siSave(archlom, idx)).To(Succeed())

			idxPath := siIdxPath(GinkgoT(), archlom)
			Expect(idxPath).To(BeAnExistingFile())
			defer os.RemoveAll(archlom.FQN)
			defer os.Remove(idxPath)

			// replaces the shard object file with a non-empty directory
			// so lom.RemoveMain() fails because cos.RemoveFile(lom.FQN) cannot remove that directory
			Expect(os.Remove(archlom.FQN)).To(Succeed())
			Expect(os.Mkdir(archlom.FQN, 0o755)).To(Succeed())
			Expect(os.WriteFile(filepath.Join(archlom.FQN, "blocked"), []byte("x"), 0o644)).To(Succeed())

			archlom.Lock(true)
			err = archlom.RemoveObj()
			archlom.Unlock(true)
			Expect(err).To(HaveOccurred())
			Expect(idxPath).To(BeAnExistingFile())
		})
	})

	Describe("new content", func() {
		const archpath = "file_0003.bin"

		It("drops the shard index (flag, index object, cache) on finalize", func() {
			fh, size, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 8)
			DeferCleanup(fh.Close)
			archlom := siArchLOM(GinkgoT(), fmt.Sprintf("rewritten-%d.tar", rand.Int64()))
			siSaveTAR(archlom, fh, size)
			siPrimeCache(archlom, archpath)

			siRewrite(archlom, fh, size) // same size and checksum: IsStale alone would keep the index

			Expect(archlom.HasShardIdx()).To(BeFalse())
			Expect(siIdxPath(GinkgoT(), archlom)).NotTo(BeAnExistingFile())
			Expect(siReload(archlom).HasShardIdx()).To(BeFalse(), "cleared flag was not persisted")
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
		})

		It("rejects an index built before the shard was rewritten", func() {
			fh1, size1, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 10)
			DeferCleanup(fh1.Close)
			old, err := archive.BuildShardIndex(fh1, size1, siShardCksum(GinkgoT(), fh1, size1))
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(old.Free)

			// meanwhile, rewritten and re-indexed
			fh2, size2, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			DeferCleanup(fh2.Close)
			archlom := siArchLOM(GinkgoT(), fmt.Sprintf("raced-%d.tar", rand.Int64()))
			siSaveTAR(archlom, fh2, size2)
			Expect(archlom.HasShardIdx()).To(BeTrue())

			// the late save loses without disturbing the current index
			err = archlom.SaveShardIndex(old)
			Expect(cmn.IsErrBusy(err)).To(BeTrue(), "expected busy, got %v", err)
			Expect(archlom.HasShardIdx()).To(BeTrue())
			Expect(siIdxPath(GinkgoT(), archlom)).To(BeAnExistingFile())
			Expect(siReload(archlom).HasShardIdx()).To(BeTrue())
			Expect(siRead(archlom, archpath)).To(BeTrue(), "current index was disturbed")
		})
	})

	Describe("copy to a different object", func() {
		const archpath = "file_0003.bin"

		It("does not inherit the source's shard index", func() {
			fh, size, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 8)
			DeferCleanup(fh.Close)
			src := siArchLOM(GinkgoT(), fmt.Sprintf("copy-src-%d.tar", rand.Int64()))
			siSaveTAR(src, fh, size)

			dst := &core.LOM{ObjName: src.ObjName}
			Expect(dst.InitCmnBck(&cmn.Bck{Name: siShardBucket2, Provider: apc.AIS})).To(Succeed())
			Expect(os.RemoveAll(dst.FQN)).To(Succeed())

			siCopy(src, dst)

			fresh := siReload(dst)
			Expect(fresh.HasShardIdx()).To(BeFalse(), "destination inherited the source's flag")
			Expect(siRead(fresh, archpath)).To(BeFalse(), "expected scan fallback")
			Expect(siRead(src, archpath)).To(BeTrue(), "source index was disturbed")
		})

		It("drops the destination's own shard index", func() {
			fh1, size1, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 10)
			DeferCleanup(fh1.Close)
			fh2, size2, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			DeferCleanup(fh2.Close)

			name := fmt.Sprintf("copy-over-%d.tar", rand.Int64())
			src := siArchLOM(GinkgoT(), name)
			siSaveTAR(src, fh1, size1)
			dst := siArchLOMInBucket(GinkgoT(), siShardBucket2, name)
			siSaveTAR(dst, fh2, size2)
			siPrimeCache(dst, archpath)
			Expect(siIdxPath(GinkgoT(), dst)).To(BeAnExistingFile())

			siCopy(src, dst)

			Expect(siIdxPath(GinkgoT(), dst)).NotTo(BeAnExistingFile(), "destination's previous index survived")
			fresh := siReload(dst)
			Expect(fresh.HasShardIdx()).To(BeFalse())
			Expect(siRead(fresh, archpath)).To(BeFalse(), "expected scan fallback")
			Expect(siRead(src, archpath)).To(BeTrue(), "source index was disturbed")
		})
	})

	Describe("busy source (best-effort flip)", func() {
		It("returns a busy error (cmn.IsErrBusy) when the source shard is read-locked", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), fmt.Sprintf("save-busy-%d.tar", rand.Int64()))

			// Simulate a concurrent reader (e.g. get-batch streaming the shard): the Phase-2
			// write lock must fail fast and leave HasShardIdx unset - never block.
			archlom.Lock(false)
			defer archlom.Unlock(false)
			err = archlom.SaveShardIndex(idx)
			Expect(cmn.IsErrBusy(err)).To(BeTrue())
			Expect(archlom.HasShardIdx()).To(BeFalse())
			Expect(siIdxPath(GinkgoT(), archlom)).NotTo(BeAnExistingFile())
		})
	})

	Describe("chunked source", func() {
		It("preserves chunking when setting and clearing HasShardIdx", func() {
			const chunkSize = 4 * cos.KiB

			archlom := siArchLOM(GinkgoT(), "chunked.tar")
			ufest, err := core.NewUfest("shard-idx-"+cos.GenTie(), archlom, false /*must-exist*/)
			Expect(err).NotTo(HaveOccurred())
			chunkPaths := make([]string, 0, 2)
			for num := 1; num <= 2; num++ {
				chunk, err := ufest.NewChunk(num, archlom)
				Expect(err).NotTo(HaveOccurred())
				chunkPaths = append(chunkPaths, chunk.Path())
				createTestChunk(chunk.Path(), chunkSize, nil)
				Expect(ufest.Add(chunk, chunkSize, int64(num))).To(Succeed())
			}
			Expect(archlom.CompleteUfest(ufest, false /*locked*/)).To(Succeed())
			Expect(archlom.IsChunked()).To(BeTrue())

			idx, err := archive.NewShardIndexTestOnly(archlom.Checksum(), archlom.Lsize(),
				map[string]archive.ShardIndexEntry{"file": {Offset: 0, Size: 1}})
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)
			Expect(archlom.SaveShardIndex(idx)).To(Succeed())
			Expect(archlom.HasShardIdx()).To(BeTrue())
			Expect(archlom.IsChunked()).To(BeTrue(), "setting HasShardIdx changed the source layout")
			Expect(chunkPaths[1]).To(BeAnExistingFile(), "setting HasShardIdx removed a chunk")

			idxPath := siIdxPath(GinkgoT(), archlom)
			data, err := os.ReadFile(idxPath)
			Expect(err).NotTo(HaveOccurred())
			data[archive.ShardIdxMinLen] ^= 0xff
			Expect(os.WriteFile(idxPath, data, 0o644)).To(Succeed())

			_, err = siLoad(archlom)
			Expect(errors.Is(err, archive.ErrShardIdxCorrupt)).To(BeTrue())
			Expect(archlom.HasShardIdx()).To(BeFalse())
			Expect(archlom.IsChunked()).To(BeTrue(), "clearing HasShardIdx changed the source layout")
			Expect(chunkPaths[1]).To(BeAnExistingFile(), "clearing HasShardIdx removed a chunk")

			archlom.UncacheDel()
			fresh := &core.LOM{ObjName: archlom.ObjName}
			Expect(fresh.InitCmnBck(&cmn.Bck{Name: siShardBucket, Provider: apc.AIS})).To(Succeed())
			fresh.Lock(false)
			Expect(fresh.Load(false /*cache it*/, true /*locked*/)).To(Succeed())
			Expect(fresh.IsChunked()).To(BeTrue(), "chunked flag was not persisted")
			fresh.Unlock(false)
		})
	})

	Describe("corruption", func() {
		DescribeTable("reports checksum error when index payload is corrupted",
			func(format tar.Format) {
				shardName := fmt.Sprintf("corrupt_%s.tar", format)
				fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, format, 10)
				defer fh.Close()
				idx, err := archive.BuildShardIndex(fh, sz, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(idx.Free)

				archlom := siArchLOM(GinkgoT(), shardName)
				Expect(siSave(archlom, idx)).To(Succeed())

				// Compute the index FQN directly using the same LOM machinery as SaveShardIndex.
				idxPath := siIdxPath(GinkgoT(), archlom)
				data, err := os.ReadFile(idxPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(data)).To(BeNumerically(">", 11))
				data[11] ^= 0xFF
				Expect(os.WriteFile(idxPath, data, 0o644)).To(Succeed())

				_, err = siLoad(archlom)
				Expect(err).To(HaveOccurred())
				Expect(errors.Is(err, archive.ErrShardIdxCorrupt)).To(BeTrue(),
					"payload corruption must classify as ErrShardIdxCorrupt, got %v", err)
				Expect(err.Error()).To(ContainSubstring("checksum mismatch"))
				// corrupt => flag cleared so the next read skips the index entirely
				Expect(archlom.HasShardIdx()).To(BeFalse())
			},
			Entry("USTAR", tar.FormatUSTAR),
			Entry("GNU", tar.FormatGNU),
			Entry("PAX", tar.FormatPAX),
		)

		It("classifies an uncached index metadata mismatch as corrupt", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "corrupt-lmeta.tar")
			Expect(siSave(archlom, idx)).To(Succeed())

			idxlom := siIdxLOM(GinkgoT(), archlom)
			Expect(os.Truncate(idxlom.FQN, archive.ShardIdxMinLen-1)).To(Succeed())
			idxlom.UncacheDel() // force LoadShardIndex to validate xattr size against stat

			loaded, err := siLoad(archlom)
			Expect(loaded).To(BeNil())
			Expect(errors.Is(err, archive.ErrShardIdxCorrupt)).To(BeTrue(),
				"invalid index LOM metadata must be rebuildable, got %v", err)
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})
	})

	// The upgrade path: every pre-v5.1 deployment has metaver-1 indexes on disk.
	Describe("meta-version compatibility", func() {
		It("reports a metaver-1 index as stale and clears the flag", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 10)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "metaver1.tar")
			Expect(siSave(archlom, idx)).To(Succeed())
			Expect(archlom.HasShardIdx()).To(BeTrue())

			// Rewrite the stored index in place as metaver 1. The v2 payload layout is
			// byte-identical to v1; only the version byte and the checksum domain differ
			// (v1 covers the payload alone, v2 the preamble kind plus payload). Same
			// length, so the index LOM's recorded size still matches.
			const prefLen = 11 // shardIdxPrefLen
			idxPath := siIdxPath(GinkgoT(), archlom)
			b, err := os.ReadFile(idxPath)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(b)).To(BeNumerically(">", prefLen))
			b[0] = 1
			binary.BigEndian.PutUint64(b[3:], onexxh.Checksum64S(b[prefLen:], cos.MLCG32))
			Expect(os.WriteFile(idxPath, b, cos.PermRWR)).To(Succeed())

			loaded, err := siLoad(archlom)
			Expect(loaded).To(BeNil())
			Expect(errors.Is(err, archive.ErrShardIdxStale)).To(BeTrue(),
				"metaver 1 must be stale (rebuildable), not corrupt, got %v", err)
			Expect(archlom.HasShardIdx()).To(BeFalse(),
				"a stale index must clear the flag so the next read falls back to the sequential scan")
		})
	})

	Describe("implausible index size", func() {
		It("classifies a below-minimum index as corrupt", func() {
			fh, sz, _ := siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 5)
			defer fh.Close()
			idx, err := archive.BuildShardIndex(fh, sz, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)

			archlom := siArchLOM(GinkgoT(), "runt-index.tar")
			Expect(siSave(archlom, idx)).To(Succeed())

			// truncate the index object below the preamble
			idxPath := siIdxPath(GinkgoT(), archlom)
			Expect(os.Truncate(idxPath, archive.ShardIdxMinLen-1)).To(Succeed())

			loaded, err := siLoad(archlom)
			Expect(loaded).To(BeNil())
			Expect(errors.Is(err, archive.ErrShardIdxCorrupt)).To(BeTrue(),
				"a below-minimum index must be corrupt, not silently absent, got %v", err)
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})

		It("ignores the shard-size bound below the allocation floor", func() {
			// A 20-entry index against a 64-byte shard is impossible for a real TAR, but the
			// allocation it implies is trivial, so the bound must not fire - see shardIdxLenOk.
			// SrcSize matches the shard, so IsStale cannot fire either: any error here is the guard.
			const shardSize = 64

			archlom := siArchLOM(GinkgoT(), "tiny-shard.tar")
			siSetShardSize(GinkgoT(), archlom, shardSize)

			entries := make(map[string]archive.ShardIndexEntry, 20)
			for i := range 20 {
				entries[fmt.Sprintf("obj-%03d", i)] = archive.ShardIndexEntry{
					Offset: int64(i) * archive.TarBlockSize,
					Size:   1,
				}
			}
			idx, err := archive.NewShardIndexTestOnly(cos.NoneCksum, shardSize, entries)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)
			Expect(siSave(archlom, idx)).To(Succeed())

			loaded, err := siLoad(archlom)
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded).NotTo(BeNil())
			Expect(loaded.Len()).To(Equal(20))
			Expect(archlom.HasShardIdx()).To(BeTrue())
		})

		It("classifies an index larger than its shard as stale, past the floor", func() {
			const (
				shardSize = int64(cos.MiB)
				nEntries  = 2048
				nameLen   = 700
			)

			archlom := siArchLOM(GinkgoT(), "oversized-index.tar")
			siSetShardSize(GinkgoT(), archlom, shardSize)

			entries := make(map[string]archive.ShardIndexEntry, nEntries)
			for i := range nEntries {
				entries[fmt.Sprintf("%0*d", nameLen, i)] = archive.ShardIndexEntry{
					Offset: int64(i) * archive.TarBlockSize,
					Size:   1,
				}
			}
			idx, err := archive.NewShardIndexTestOnly(cos.NoneCksum, shardSize, entries)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(idx.Free)
			Expect(siSave(archlom, idx)).To(Succeed())

			packed, err := os.Stat(siIdxPath(GinkgoT(), archlom))
			Expect(err).NotTo(HaveOccurred())
			Expect(packed.Size()).To(BeNumerically(">", shardSize), "fixture must exceed the floor")

			loaded, err := siLoad(archlom)
			Expect(loaded).To(BeNil())
			Expect(errors.Is(err, archive.ErrShardIdxStale)).To(BeTrue(),
				"an index bigger than its shard is a mispairing, not corruption, got %v", err)
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})
	})

	Describe("cache", func() {
		const archpath = "file_0003.bin"
		var (
			archlom *core.LOM
			fh      *os.File
			size    int64
		)

		BeforeEach(func() {
			fh, size, _ = siMakeTAR(GinkgoT(), tmpDir, tar.FormatUSTAR, 8)
			DeferCleanup(fh.Close)
			archlom = siArchLOM(GinkgoT(), fmt.Sprintf("cached-%d.tar", rand.Int64()))
			siSaveTAR(archlom, fh, size)
		})

		It("serves hits from memory", func() {
			before := core.SidxStats()
			siPrimeCache(archlom, archpath)
			siRmIdx(archlom)
			Expect(siRead(archlom, archpath)).To(BeTrue(), "expected a cache hit")
			Expect(archlom.HasShardIdx()).To(BeTrue())

			after := core.SidxStats()
			Expect(after.Miss - before.Miss).To(BeEquivalentTo(1))
			Expect(after.Load - before.Load).To(BeEquivalentTo(1))
		})

		It("handles concurrent cold reads", func() {
			const n = 16
			before := core.SidxStats()
			var wg sync.WaitGroup
			fast := make([]bool, n)
			for i := range n {
				wg.Go(func() {
					defer GinkgoRecover()
					fast[i] = siRead(archlom, archpath)
				})
			}
			wg.Wait()
			after := core.SidxStats()
			if after.Deny > before.Deny {
				Skip("cache admission denied under high memory load")
			}
			for _, usedIdx := range fast {
				Expect(usedIdx).To(BeTrue(), "concurrent readers must share the cold load")
			}
			Expect(after.Load-before.Load).To(BeEquivalentTo(1), "expected a single winning load")
			siRmIdx(archlom)
			Expect(siRead(archlom, archpath)).To(BeTrue(), "expected a cache hit")
		})

		It("is not populated by SaveShardIndex", func() {
			siRmIdx(archlom)
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})

		It("is invalidated by SaveShardIndex", func() {
			siPrimeCache(archlom, archpath)
			siSaveTAR(archlom, fh, size) // same content: cached index stays valid unless invalidated
			siRmIdx(archlom)
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})

		It("evicts idle entries", func() {
			siPrimeCache(archlom, archpath)
			siRmIdx(archlom)
			before := core.SidxStats()
			core.SidxEvictIdle(0)
			Expect(core.SidxStats().Evict).To(BeNumerically(">", before.Evict))
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
		})

		It("keeps entries not yet idle", func() {
			siPrimeCache(archlom, archpath)
			siRmIdx(archlom)
			core.SidxEvictIdle(time.Hour)
			Expect(siRead(archlom, archpath)).To(BeTrue(), "expected a cache hit")
		})

		It("clears all under critical pressure", func() {
			siPrimeCache(archlom, archpath)
			siRmIdx(archlom)
			before := core.SidxStats()
			core.SidxClearAll()
			Expect(core.SidxStats().Clear - before.Clear).To(BeEquivalentTo(1))
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
		})

		It("counts loaded bytes toward the next memory sample", func() {
			siPrimeCache(archlom, archpath)
			Expect(core.SidxNbytes()).To(BeNumerically(">", 0))
		})

		It("detects stale cached index", func() {
			siPrimeCache(archlom, archpath)
			archlom.SetCksum(cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef"))
			Expect(siRead(archlom, archpath)).To(BeFalse(), "expected scan fallback")
			Expect(archlom.HasShardIdx()).To(BeFalse())
		})
	})
})
