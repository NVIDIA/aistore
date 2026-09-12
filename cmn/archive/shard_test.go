// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive_test

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	onexxh "github.com/OneOfOne/xxhash"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
)

var (
	modTime    = time.Unix(1_000_000, 0)
	tarFormats = []tar.Format{tar.FormatUSTAR, tar.FormatGNU, tar.FormatPAX}
)

type captureReader struct {
	b   []byte
	dst []byte
}

func (r *captureReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	if r.dst == nil {
		r.dst = p
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

func shardIdxHash(metaver, format, cksumType byte, payload []byte) uint64 {
	if metaver == 1 {
		return onexxh.Checksum64S(payload, cos.MLCG32)
	}
	seed := uint64(cos.MLCG32) |
		uint64(metaver)<<32 |
		uint64(format)<<40 |
		uint64(cksumType)<<48
	return onexxh.Checksum64S(payload, seed)
}

func readShardIndex(t testing.TB, b []byte) *archive.ShardIndex {
	t.Helper()
	idx, err := archive.ReadShardIndex(bytes.NewReader(b), int64(len(b)), nil)
	if err != nil {
		t.Fatal("ReadShardIndex:", err)
	}
	t.Cleanup(idx.Free)
	return idx
}

func packShardIndex(t testing.TB, idx *archive.ShardIndex) []byte {
	t.Helper()
	b, err := idx.Pack()
	if err != nil {
		t.Fatal("Pack:", err)
	}
	return b
}

func readShardIndexErr(b []byte) error {
	idx, err := archive.ReadShardIndex(bytes.NewReader(b), int64(len(b)), nil)
	if idx != nil {
		idx.Free()
	}
	return err
}

// verify the Pack => ReadShardIndex round-trip:
// - build an index from a real TAR, pack it to bytes, read it back
// - assert that every entry's Offset and Size survive the serialization unchanged
func TestShardPackUnpackRoundTrip(t *testing.T) {
	rng := cos.NowRand()
	counts := []int{0, 1, 100}
	if !testing.Short() {
		counts = append(counts, 10_000)
	}
	for _, f := range tarFormats {
		for _, count := range counts {
			t.Run(fmt.Sprintf("%s/count=%d", f, count), func(t *testing.T) {
				fh := tempFile(t)
				defer fh.Close()

				tw := tar.NewWriter(fh)
				for i := range count {
					name := fmt.Sprintf("file_%06d.bin", i)
					size := 1 + rng.Int64N(4*cos.KiB)
					r := randReader(t, size)
					writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: f}, size, r)
				}
				size := sealTAR(t, fh, tw)

				orig, err := archive.BuildShardIndex(fh, size, nil)
				if err != nil {
					t.Fatal("BuildShardIndex:", err)
				}

				b := packShardIndex(t, orig)

				got := readShardIndex(t, b)

				if got.Len() != orig.Len() {
					t.Fatalf("entry count: got %d, want %d", got.Len(), orig.Len())
				}
				for name, want := range orig.AllTestOnly() {
					have, ok := got.Lookup(name)
					if !ok {
						t.Fatalf("entry %q: missing after ReadShardIndex", name)
					}
					if have.Offset != want.Offset {
						t.Fatalf("entry %q: Offset got %d, want %d", name, have.Offset, want.Offset)
					}
					if have.Size != want.Size {
						t.Fatalf("entry %q: Size got %d, want %d", name, have.Size, want.Size)
					}
				}
			})
		}
	}
}

func TestShardReadOwnsBuffer(t *testing.T) {
	orig, err := archive.NewShardIndexTestOnly(nil, 4096, map[string]archive.ShardIndexEntry{
		"a.txt": {Offset: 0, Size: 1},
	})
	if err != nil {
		t.Fatal("NewShardIndexTestOnly:", err)
	}
	t.Cleanup(orig.Free)
	packed := packShardIndex(t, orig)
	src := bytes.Clone(packed)

	got := readShardIndex(t, src)
	clear(src)
	entry, ok := got.Lookup("a.txt")
	if !ok || entry != (archive.ShardIndexEntry{Offset: 0, Size: 1}) {
		t.Fatalf("lookup after clearing source: got %+v, %t", entry, ok)
	}
}

// verify that map insertion and iteration order do not affect the packed representation
func TestShardPackIsDeterministic(t *testing.T) {
	const (
		nEntries = 512
		nRepeats = 16
	)
	names := make([]string, nEntries)
	for i := range names {
		names[i] = fmt.Sprintf("dir_%02d/file_%06d.bin", i%7, i)
	}
	entries := make(map[string]archive.ShardIndexEntry, nEntries)
	for i, name := range names {
		entries[name] = archive.ShardIndexEntry{
			Offset: int64(i) * archive.TarBlockSize * 3,
			Size:   int64(i) * 17,
		}
	}

	idx, err := archive.NewShardIndexTestOnly(nil, 1<<20, entries)
	if err != nil {
		t.Fatal("NewShardIndexTestOnly:", err)
	}
	want := packShardIndex(t, idx)
	if want[0] != 2 {
		t.Fatalf("metaver: got %d, want 2", want[0])
	}

	// must be idempotent
	for i := range nRepeats {
		b := packShardIndex(t, idx)
		if !bytes.Equal(b, want) {
			t.Fatalf("repeat %d: Pack output differs (%d vs %d bytes)", i, len(b), len(want))
		}
	}

	// same entries in a distinct map, built in reverse insertion order
	reversed := make(map[string]archive.ShardIndexEntry, nEntries)
	for i := nEntries - 1; i >= 0; i-- {
		reversed[names[i]] = entries[names[i]]
	}
	other, err := archive.NewShardIndexTestOnly(nil, 1<<20, reversed)
	if err != nil {
		t.Fatal("NewShardIndexTestOnly:", err)
	}
	b := packShardIndex(t, other)
	if !bytes.Equal(b, want) {
		t.Fatal("Pack output differs for an identical index built in a different order")
	}
}

func TestShardPackConcurrent(t *testing.T) {
	idx, err := archive.NewShardIndexTestOnly(nil, 1<<20, map[string]archive.ShardIndexEntry{
		"a": {Offset: 0, Size: 1},
		"b": {Offset: archive.TarBlockSize, Size: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := packShardIndex(t, idx)
	want = bytes.Clone(want)

	const (
		nworkers = 16
		nloops   = 100
	)
	var wg sync.WaitGroup
	wg.Add(nworkers)
	for range nworkers {
		go func() {
			defer wg.Done()
			for range nloops {
				got, err := idx.Pack()
				if err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(got, want) {
					t.Error("concurrent Pack changed the packed representation")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestShardPackAfterFree(t *testing.T) {
	idx, err := archive.NewShardIndexTestOnly(nil, 1<<20,
		map[string]archive.ShardIndexEntry{"a": {Offset: 0, Size: 1}})
	if err != nil {
		t.Fatal(err)
	}
	idx.Free()

	if packed, err := idx.Pack(); err == nil || packed != nil {
		t.Fatalf("Pack after Free: got %d bytes, err %v", len(packed), err)
	}
}

// Pack() must reject entries with negative Offset or Size
func TestShardPackNegativeValues(t *testing.T) {
	cases := []struct {
		name  string
		entry archive.ShardIndexEntry
	}{
		{"negative offset", archive.ShardIndexEntry{Offset: -1, Size: 0}},
		{"negative size", archive.ShardIndexEntry{Offset: 0, Size: -1}},
		{"both negative", archive.ShardIndexEntry{Offset: -512, Size: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := archive.NewShardIndexTestOnly(nil, 0,
				map[string]archive.ShardIndexEntry{"f": tc.entry}); err == nil {
				t.Fatalf("%s: expected NewShardIndexTestOnly to return error, got nil", tc.name)
			}
		})
	}
}

// Metaver 2 makes strict name ordering a format invariant. Metaver 1 is
// intentionally rebuild-only.
func TestShardUnpackOrderInvariant(t *testing.T) {
	const prefLen = 11

	mk := func(metaver byte, names ...string) []byte {
		var payload []byte
		payload = binary.AppendUvarint(payload, uint64(len("none"))) // src cksum type
		payload = append(payload, "none"...)
		payload = binary.AppendUvarint(payload, 0)    // src cksum value
		payload = binary.AppendUvarint(payload, 4096) // src size
		payload = binary.AppendUvarint(payload, uint64(len(names)))
		for i, name := range names {
			payload = binary.AppendUvarint(payload, uint64(len(name)))
			payload = append(payload, name...)
			payload = binary.AppendUvarint(payload, uint64(i)*archive.TarBlockSize)
			payload = binary.AppendUvarint(payload, 1)
		}
		b := make([]byte, prefLen+len(payload))
		b[0], b[1], b[2] = metaver, 0, 1
		copy(b[prefLen:], payload)
		binary.BigEndian.PutUint64(b[3:], shardIdxHash(metaver, b[1], b[2], payload))
		return b
	}

	for _, names := range [][]string{{"a.txt", "b.txt", "c.txt"}, {"only.txt"}, nil} {
		idx := readShardIndex(t, mk(2, names...))
		if idx.Len() != len(names) {
			t.Fatalf("Len: got %d, want %d", idx.Len(), len(names))
		}
	}

	// metaver 1 is intact-but-unusable, not damaged - see TestShardUnpackOlderMetaver
	if err := readShardIndexErr(mk(1, "a.txt")); !errors.Is(err, archive.ErrShardIdxStale) {
		t.Fatalf("metaver 1: expected ErrShardIdxStale, got %v", err)
	}

	for _, tc := range []struct {
		name  string
		names []string
	}{
		{"descending", []string{"c.txt", "b.txt", "a.txt"}},
		{"one-swap", []string{"a.txt", "c.txt", "b.txt"}},
		{"duplicate", []string{"a.txt", "a.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := readShardIndexErr(mk(2, tc.names...)); !errors.Is(err, archive.ErrShardIdxCorrupt) {
				t.Fatalf("expected ErrShardIdxCorrupt, got %v", err)
			}
		})
	}
}

// metaver-1 index (v1.4.5 .. v5.0) is intact but emitted its entries in map iteration order -
// it cannot be binary-searched
func TestShardUnpackOlderMetaver(t *testing.T) {
	const prefLen = 11

	var payload []byte
	payload = binary.AppendUvarint(payload, uint64(len("none")))
	payload = append(payload, "none"...)
	payload = binary.AppendUvarint(payload, 0)
	payload = binary.AppendUvarint(payload, 4096)
	payload = binary.AppendUvarint(payload, 2)
	for i, name := range []string{"z.txt", "a.txt"} {
		payload = binary.AppendUvarint(payload, uint64(len(name)))
		payload = append(payload, name...)
		payload = binary.AppendUvarint(payload, uint64(i)*archive.TarBlockSize)
		payload = binary.AppendUvarint(payload, 1)
	}
	mk := func(metaver byte) []byte {
		b := make([]byte, prefLen+len(payload))
		b[0], b[1], b[2] = metaver, 0, 1
		copy(b[prefLen:], payload)
		binary.BigEndian.PutUint64(b[3:], shardIdxHash(metaver, b[1], b[2], payload))
		return b
	}

	for _, tc := range []struct {
		name    string
		metaver byte
		want    error
	}{
		{"v1-is-stale", 1, archive.ErrShardIdxStale},
		{"zero-is-corrupt", 0, archive.ErrShardIdxCorrupt},
		{"future-is-corrupt", 3, archive.ErrShardIdxCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := readShardIndexErr(mk(tc.metaver))
			if !errors.Is(err, tc.want) {
				t.Fatalf("metaver %d: got %v, want %v", tc.metaver, err, tc.want)
			}
		})
	}

	// a corrupted payload must stay corrupt even when byte 0 reads as a valid old version
	bad := mk(1)
	bad[len(bad)-1] ^= 0xff
	if err := readShardIndexErr(bad); !errors.Is(err, archive.ErrShardIdxCorrupt) {
		t.Fatalf("damaged v1 payload: got %v, want ErrShardIdxCorrupt", err)
	}

	// the version byte itself was not covered by the v1 checksum
	for _, tc := range []struct {
		name string
		b    []byte
		to   byte
	}{
		{"v1-as-v2", mk(1), 2},
		{"v2-as-v1", mk(2), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.b[0] = tc.to
			if err := readShardIndexErr(tc.b); !errors.Is(err, archive.ErrShardIdxCorrupt) {
				t.Fatalf("changed version byte: got %v, want ErrShardIdxCorrupt", err)
			}
		})
	}
}

// the source checksum must survive Free: it may be cloned out and persisted, and Free
// can return the packed buffer to a slab where the bytes get reused
func TestShardSrcCksumSurvivesFree(t *testing.T) {
	orig, err := archive.NewShardIndexTestOnly(cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef"), 4096,
		map[string]archive.ShardIndexEntry{"a.txt": {Offset: 0, Size: 512}})
	if err != nil {
		t.Fatal("NewShardIndexTestOnly:", err)
	}
	b := packShardIndex(t, orig)
	r := &captureReader{b: b}
	loaded, err := archive.ReadShardIndex(r, int64(len(b)), nil)
	if err != nil {
		t.Fatal("ReadShardIndex:", err)
	}
	clone := loaded.SrcCksum().Clone()
	want := clone.String()

	loaded.Free()
	// scribble over the buffer the index was decoded from
	for i := range r.dst {
		r.dst[i] = 0xff
	}
	if got := clone.String(); got != want {
		t.Fatalf("cloned src cksum aliased the packed buffer: got %q, want %q", got, want)
	}
}

// checksum-valid, oversized string length must be rejected
func TestShardUnpackLengthOverflow(t *testing.T) {
	const prefLen = 11
	payload := binary.AppendUvarint(nil, uint64(math.MaxInt))
	b := make([]byte, prefLen+len(payload))
	b[0], b[1], b[2] = 2, 0, 1
	copy(b[prefLen:], payload)
	binary.BigEndian.PutUint64(b[3:], shardIdxHash(b[0], b[1], b[2], payload))

	err := readShardIndexErr(b)
	if !errors.Is(err, archive.ErrShardIdxCorrupt) {
		t.Fatalf("expected ErrShardIdxCorrupt, got %v", err)
	}
}

// ownership and cleanup of both slab-backed buffers
func TestShardReadUnpackPooled(t *testing.T) {
	entries := make(map[string]archive.ShardIndexEntry, 100)
	for i := range 100 {
		entries[fmt.Sprintf("file-%03d", i)] = archive.ShardIndexEntry{Offset: int64(i) * archive.TarBlockSize, Size: 1}
	}
	orig, err := archive.NewShardIndexTestOnly(nil, 1<<20, entries)
	if err != nil {
		t.Fatal(err)
	}
	packed := packShardIndex(t, orig)

	mm := memsys.PageMM()
	got, err := archive.ReadShardIndex(bytes.NewReader(packed), int64(len(packed)), mm)
	if err != nil {
		t.Fatal(err)
	}
	if entry, ok := got.Lookup("file-050"); !ok || entry.Offset != 50*archive.TarBlockSize {
		t.Fatalf("lookup after pooled read: got %+v, %t", entry, ok)
	}
	got.Free()
	got.Free() // idempotent

	const prefLen = 11
	payload := binary.AppendUvarint(nil, uint64(len("none")))
	payload = append(payload, "none"...)
	payload = binary.AppendUvarint(payload, 0) // cksum value
	payload = binary.AppendUvarint(payload, 1) // source size
	payload = binary.AppendUvarint(payload, 1) // count
	payload = binary.AppendUvarint(payload, 1) // name length
	payload = append(payload, 'x')             // name
	payload = binary.AppendUvarint(payload, 0) // offset; size is missing
	malformed := make([]byte, prefLen+len(payload))
	malformed[0], malformed[1], malformed[2] = 2, 0, 1
	copy(malformed[prefLen:], payload)
	binary.BigEndian.PutUint64(malformed[3:], shardIdxHash(malformed[0], malformed[1], malformed[2], payload))
	if _, err := archive.ReadShardIndex(bytes.NewReader(malformed), int64(len(malformed)), mm); !errors.Is(err, archive.ErrShardIdxCorrupt) {
		t.Fatalf("expected ErrShardIdxCorrupt, got %v", err)
	}
}

// corrupt a single bit anywhere in the packed payload;
// check all supported TAR formats
func TestShardUnpackChecksumCorruption(t *testing.T) {
	for _, f := range tarFormats {
		t.Run(f.String(), func(t *testing.T) {
			fh := tempFile(t)
			defer fh.Close()

			tw := tar.NewWriter(fh)
			for i := range 10 {
				name := fmt.Sprintf("file_%02d.bin", i)
				r := randReader(t, 512)
				writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: f}, 512, r)
			}
			size := sealTAR(t, fh, tw)

			idx, err := archive.BuildShardIndex(fh, size, nil)
			if err != nil {
				t.Fatal("BuildShardIndex:", err)
			}
			b := packShardIndex(t, idx)

			// flip one bit at each byte position in the payload (everything after the preamble)
			// and confirm Unpack rejects it every time
			const prefLen = 11 // shardIdxPrefLen
			for i := prefLen; i < len(b); i++ {
				corrupted := make([]byte, len(b))
				copy(corrupted, b)
				corrupted[i] ^= 0x01

				if err := readShardIndexErr(corrupted); err == nil {
					t.Fatalf("byte %d: expected checksum error, got nil", i)
				}
			}
		})
	}
}

func TestShardUnpackLargeEntryCount(t *testing.T) {
	const prefLen = 11
	payload := binary.AppendUvarint([]byte{0, 0, 0}, uint64(1)<<32)
	b := make([]byte, prefLen+len(payload))
	b[0], b[1], b[2] = 2, 0, 1
	copy(b[prefLen:], payload)
	binary.BigEndian.PutUint64(b[3:], shardIdxHash(b[0], b[1], b[2], payload))

	if err := readShardIndexErr(b); err == nil {
		t.Fatal("expected oversized entry count to fail")
	}
}

// fileMap maps archived filenames to the readers used to write them.
// The same reader is reopened via Open() during validation, so each entry
// carries both the write-side content and its own comparison source.
type fileMap map[string]readers.Reader

// scenarios names the cases run against every format in TestShardBuildIndex.
// Build logic lives in buildScenario; adding a new case requires one entry here
// and one case in that switch.
var scenarios = []string{
	"MultipleFiles",
	"SingleFile",
	"ZeroSizeFile",
	"DuplicateNames",
	"512ByteBoundaries",
	"BinaryContent",
	"SkipsNonRegular",
}

// every scenario × every format
func TestShardBuildIndex(t *testing.T) {
	for _, sc := range scenarios {
		for _, f := range tarFormats {
			t.Run(sc+"/"+f.String(), func(t *testing.T) {
				fh := tempFile(t)
				defer fh.Close()

				tw := tar.NewWriter(fh)
				expected := buildScenario(t, sc, tw, f)
				size := sealTAR(t, fh, tw)

				idx, err := archive.BuildShardIndex(fh, size, nil)
				if err != nil {
					t.Fatal(err)
				}
				if idx.Len() != len(expected) {
					t.Fatalf("entry count: got %d, want %d", idx.Len(), len(expected))
				}
				for name, r := range expected {
					entry, ok := idx.Lookup(name)
					if !ok {
						t.Fatalf("entry %q: missing from index", name)
					}
					checkEntry(t, fh, name, entry, r)
				}
			})
		}
	}
}

// verify no error and an empty Entries map for an empty TAR (format-independent)
func TestShardBuildIndex_Empty(t *testing.T) {
	fh := tempFile(t)
	defer fh.Close()

	size := sealTAR(t, fh, tar.NewWriter(fh))

	idx, err := archive.BuildShardIndex(fh, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	if idx.Len() != 0 {
		t.Fatalf("expected empty index, got %d entries", idx.Len())
	}
}

// Verify that a TypeGNUSparse entry is skipped.
// A GNU sparse file's logical Size exceeds its physical data, so the recorded
// Offset would not point to contiguous content.  The regular file that follows
// must still be correctly indexed.
//
// tar.Writer does not emit TypeGNUSparse directly; the header is constructed
// as a raw 512-byte block with a valid checksum (see gnuSparseHeader).
func TestShardBuildIndex_GNUSparse(t *testing.T) {
	var buf bytes.Buffer

	// TypeGNUSparse header, size=0 - no data blocks follow.
	hdrBlk := gnuSparseHeader("sparse.bin")
	buf.Write(hdrBlk[:])

	// Regular file appended immediately after via tar.Writer.
	tw := tar.NewWriter(&buf)
	r := randReader(t, 64)
	writeTAREntry(t, tw, &tar.Header{
		Name: "regular.txt", Mode: 0o644, ModTime: modTime, Format: tar.FormatGNU,
	}, 64, r)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	fh := tempFile(t)
	defer fh.Close()
	if _, err := fh.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	idx, err := archive.BuildShardIndex(fh, int64(buf.Len()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.Lookup("sparse.bin"); ok {
		t.Error(`"sparse.bin" must not be indexed (TypeGNUSparse)`)
	}
	entry, ok := idx.Lookup("regular.txt")
	if !ok {
		t.Fatal(`entry "regular.txt": missing from index`)
	}
	checkEntry(t, fh, "regular.txt", entry, r)
}

// Verify that a GNU long-name auxiliary entry
// (TypeGNULongName, emitted before the real header for names > 100 bytes) is
// fully consumed before Offset is recorded.
func TestShardBuildIndex_GNULongName(t *testing.T) {
	// 535 chars: forces two 512-byte data blocks for the TypeGNULongName entry,
	// not just one.  Single-block long names (> 100 chars) are covered by the
	// GNULongName scenario in the main table.
	longName := "deeply/nested/" + strings.Repeat("x", 512) + "/file.txt"
	r := randReader(t, 64)

	fh := tempFile(t)
	defer fh.Close()

	tw := tar.NewWriter(fh)
	writeTAREntry(t, tw, &tar.Header{Name: longName, Mode: 0o644, ModTime: modTime, Format: tar.FormatGNU}, 64, r)
	size := sealTAR(t, fh, tw)

	idx, err := archive.BuildShardIndex(fh, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := idx.Lookup(longName)
	if !ok {
		t.Fatalf("entry %q: missing from index", longName)
	}
	checkEntry(t, fh, longName, entry, r)
}

// Verify PAX-format long filenames (> 100 chars).
// PAX encodes the name as a path= attribute in the extension block - a different
// mechanism than GNU TypeGNULongName, but likewise adds an extra block before data.
func TestShardBuildIndex_PAXLongName(t *testing.T) {
	// 531 chars: forces the PAX path= attribute to spill across multiple 512-byte
	// extension blocks, not just one.
	longName := "deep/path/" + strings.Repeat("y", 512) + "/data.bin"
	r := randReader(t, 64)

	fh := tempFile(t)
	defer fh.Close()

	tw := tar.NewWriter(fh)
	writeTAREntry(t, tw, &tar.Header{Name: longName, Mode: 0o644, ModTime: modTime, Format: tar.FormatPAX}, 64, r)
	size := sealTAR(t, fh, tw)

	idx, err := archive.BuildShardIndex(fh, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := idx.Lookup(longName)
	if !ok {
		t.Fatalf("entry %q: missing from index", longName)
	}
	checkEntry(t, fh, longName, entry, r)
}

// Verify that an explicit PAXRecords entry (independent of filename length)
// still forces an extension block, and the resulting Offset correctly skips it.
func TestShardBuildIndex_PAXExplicitExtension(t *testing.T) {
	const (
		name = "pax_explicit.txt"
		size = 64 * cos.KiB
	)
	r := randReader(t, size)

	fh := tempFile(t)
	defer fh.Close()

	tw := tar.NewWriter(fh)
	writeTAREntry(t, tw, &tar.Header{
		Name: name, Mode: 0o644, ModTime: modTime, Format: tar.FormatPAX,
		PAXRecords: map[string]string{"comment": "forces PAX extension block"},
	}, size, r)
	sz := sealTAR(t, fh, tw)

	idx, err := archive.BuildShardIndex(fh, sz, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := idx.Lookup(name)
	if !ok {
		t.Fatalf("entry %q: missing from index", name)
	}
	checkEntry(t, fh, name, entry, r)
}

// Verify that PAX extension headers spanning four or more 512-byte blocks are
// fully consumed before the data Offset is recorded.
// A 4096-byte comment encodes to ~8 extension blocks; the data must follow.
func TestShardBuildIndex_PAXLargeExtension(t *testing.T) {
	const (
		name = "pax_large.bin"
		size = 128 * cos.KiB
	)
	r := randReader(t, 128*cos.KiB)

	fh := tempFile(t)
	defer fh.Close()

	tw := tar.NewWriter(fh)
	writeTAREntry(t, tw, &tar.Header{
		Name: name, Mode: 0o644, ModTime: modTime, Format: tar.FormatPAX,
		PAXRecords: map[string]string{"comment": strings.Repeat("x", 4096)},
	}, size, r)
	sz := sealTAR(t, fh, tw)

	idx, err := archive.BuildShardIndex(fh, sz, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := idx.Lookup(name)
	// Header must be past 8 extension blocks (8 × 512 = 4096 bytes).
	if !ok || entry.Offset < 8*archive.TarBlockSize {
		t.Fatalf("entry %q: expected Offset >= %d (large PAX extension), got %v", name, 8*archive.TarBlockSize, entry)
	}
	checkEntry(t, fh, name, entry, r)
}

// Write TAR entries for the named scenario into tw and returns
// a fileMap.  Each reader in the map was used to write the corresponding entry;
// Open() on it yields a fresh reader at position 0 for byte-exact comparison.
func buildScenario(t *testing.T, name string, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	switch name {
	case "MultipleFiles":
		return buildMultipleFiles(t, tw, format)
	case "SingleFile":
		return buildSingleFile(t, tw, format)
	case "ZeroSizeFile":
		return buildZeroSizeFile(t, tw, format)
	case "DuplicateNames":
		return buildDuplicateNames(t, tw, format)
	case "512ByteBoundaries":
		return build512ByteBoundaries(t, tw, format)
	case "BinaryContent":
		return buildBinaryContent(t, tw, format)
	case "SkipsNonRegular":
		return buildSkipsNonRegular(t, tw, format)
	default:
		t.Fatalf("unknown scenario %q", name)
		return nil
	}
}

func buildMultipleFiles(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	count := 10_000 + rng.IntN(90_000) // [10K, 100K)
	if testing.Short() {
		count = 1_000 + rng.IntN(1_000) // [1K, 2K)
	}
	m := make(fileMap, count)
	for i := range count {
		name := fmt.Sprintf("file_%06d.bin", i)
		size := randFileSize(rng)
		r := randReader(t, size)
		writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, size, r)
		m[name] = r
	}
	return m
}

func buildSingleFile(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	size := 64*cos.KiB + rng.Int64N(448*cos.KiB) // [64KB, 512KB)
	if !testing.Short() {
		size = cos.MiB + rng.Int64N(7*cos.MiB) // [1MB, 8MB)
	}
	r := randReader(t, size)
	writeTAREntry(t, tw, &tar.Header{Name: "only.txt", Mode: 0o644, ModTime: modTime, Format: format}, size, r)
	return fileMap{"only.txt": r}
}

// buildZeroSizeFile interleaves zero-size and regular files.
// Zero-size files write no data blocks; the next header must follow immediately -
// any off-by-one in padding shifts every subsequent entry's Offset.
func buildZeroSizeFile(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	count := 100 + rng.IntN(400) // [100, 500)
	if !testing.Short() {
		count = 1_000 + rng.IntN(4_000) // [1K, 5K)
	}
	m := make(fileMap, 2*count)
	for i := range count {
		zeroName := fmt.Sprintf("zero_%06d.txt", i)
		empty := readers.NewBytes([]byte{})
		writeTAREntry(t, tw, &tar.Header{Name: zeroName, Mode: 0o644, ModTime: modTime, Format: format}, 0, empty)
		m[zeroName] = empty

		afterName := fmt.Sprintf("file_%06d.bin", i)
		size := randFileSize(rng)
		r := randReader(t, size)
		writeTAREntry(t, tw, &tar.Header{Name: afterName, Mode: 0o644, ModTime: modTime, Format: format}, size, r)
		m[afterName] = r
	}
	return m
}

// buildDuplicateNames writes each name twice.
// TAR allows duplicate names (it is a plain sequential stream with no uniqueness
// constraint).  ReadOne returns the first match and stops, so the index must agree:
// first-wins, not last-wins.  This keeps the O(1) index path a drop-in replacement
// for the sequential scan - callers see identical bytes either way.
func buildDuplicateNames(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	count := 500 + rng.IntN(500) // [500, 1K) unique names
	if !testing.Short() {
		count = 5_000 + rng.IntN(5_000) // [5K, 10K) unique names
	}
	m := make(fileMap, count)
	for i := range count {
		name := fmt.Sprintf("dup_%06d.txt", i)
		sz1 := randFileSize(rng)
		sz2 := randFileSize(rng)
		r1 := randReader(t, sz1) // first occurrence - index must point here
		r2 := randReader(t, sz2) // second occurrence - must NOT be returned
		writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, sz1, r1)
		writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, sz2, r2)
		m[name] = r1
	}
	return m
}

// build512ByteBoundaries writes files at sizes that straddle 512-byte TAR block
// boundaries, repeated many times.  TAR pads each file's data to the next 512-byte
// multiple; a wrong padding calculation shifts every subsequent entry's Offset.
func build512ByteBoundaries(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	repeat := 50 + rng.IntN(50) // [50, 100) repetitions of each boundary size
	if !testing.Short() {
		repeat = 500 + rng.IntN(500) // [500, 1K) repetitions
	}
	sizes := []int64{1, 511, 512, 513, 1023, 1024, 1025}
	m := make(fileMap, len(sizes)*repeat)
	for rep := range repeat {
		for _, sz := range sizes {
			name := fmt.Sprintf("file_%04d_%06d.bin", sz, rep)
			r := randReader(t, sz)
			writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, sz, r)
			m[name] = r
		}
	}
	return m
}

func buildBinaryContent(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	count := 1_000 + rng.IntN(1_000) // [1K, 2K)
	if !testing.Short() {
		count = 10_000 + rng.IntN(90_000) // [10K, 100K)
	}
	m := make(fileMap, count)
	for i := range count {
		name := fmt.Sprintf("binary_%06d.bin", i)
		size := randFileSize(rng)
		r := randReader(t, size)
		writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, size, r)
		m[name] = r
	}
	return m
}

// buildSkipsNonRegular interleaves dirs and symlinks before each regular file.
// Only regular files should appear in the index; dirs and symlinks must be skipped.
func buildSkipsNonRegular(t *testing.T, tw *tar.Writer, format tar.Format) fileMap {
	t.Helper()
	rng := cos.NowRand()
	count := 500 + rng.IntN(500) // [500, 1K) regular files
	if !testing.Short() {
		count = 5_000 + rng.IntN(5_000) // [5K, 10K) regular files
	}
	m := make(fileMap, count)
	for i := range count {
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("dir_%06d/", i), Typeflag: tar.TypeDir, Mode: 0o755, ModTime: modTime, Format: format,
		}); err != nil {
			t.Fatal(err)
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("link_%06d.txt", i), Typeflag: tar.TypeSymlink, Linkname: "target.txt", ModTime: modTime, Format: format,
		}); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("regular_%06d.txt", i)
		size := 1 + rng.Int64N(512)
		r := randReader(t, size)
		writeTAREntry(t, tw, &tar.Header{Name: name, Mode: 0o644, ModTime: modTime, Format: format}, size, r)
		m[name] = r
	}
	return m
}

// low-level helpers

// tempFile creates a temp file inside t.TempDir() (auto-cleaned after the test).
func tempFile(t *testing.T) *os.File {
	t.Helper()
	fh, err := os.CreateTemp(t.TempDir(), "shard_*.tar")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	return fh
}

// sealTAR flushes and closes tw, then returns the size of fh.
func sealTAR(t *testing.T, fh *os.File, tw *tar.Writer) int64 {
	t.Helper()
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	info, err := fh.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return info.Size()
}

func randReader(t *testing.T, size int64) readers.Reader {
	t.Helper()
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: cos.ChecksumOneXxh})
	if err != nil {
		t.Fatalf("readers.New(size=%d): %v", size, err)
	}
	return r
}

// randFileSize returns a size drawn from either [1, 1K) or [1K, 4K) with equal
// probability, ensuring stress tests cover both small and medium file sizes.
func randFileSize(rng *rand.Rand) int64 {
	if rng.IntN(2) == 0 {
		return 1 + rng.Int64N(cos.KiB) // small [1, 1K)
	}
	return cos.KiB + rng.Int64N(3*cos.KiB) // medium [1K, 4K)
}

func writeTAREntry(t *testing.T, tw *tar.Writer, hdr *tar.Header, size int64, r io.Reader) {
	t.Helper()
	hdr.Size = size
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader %q: %v", hdr.Name, err)
	}
	if size > 0 {
		if _, err := io.Copy(tw, r); err != nil {
			t.Fatalf("Write %q: %v", hdr.Name, err)
		}
	}
}

// checkEntry verifies the index entry for name against the raw TAR bytes.
// Content is verified via xxhash: the hash precomputed during writing must match
// the hash of the bytes at [Offset, Offset+Size) in the TAR file.
func checkEntry(t *testing.T, raw io.ReaderAt, name string, entry archive.ShardIndexEntry, r readers.Reader) {
	t.Helper()
	// Offset points to the file's TAR header block - must be non-negative and 512-aligned.
	if entry.Offset < 0 {
		t.Fatalf("entry %q: negative offset %d", name, entry.Offset)
	}
	if entry.Offset%archive.TarBlockSize != 0 {
		t.Fatalf("entry %q: offset %d not aligned to TarBlockSize", name, entry.Offset)
	}
	if entry.Size < 0 {
		t.Fatalf("entry %q: negative size %d", name, entry.Size)
	}
	if entry.Size == 0 {
		return // zero-size file: no content to verify
	}
	section := io.NewSectionReader(raw, entry.DataOffset(), entry.Size)
	n, actual, err := cos.ChecksumReader(section, cos.ChecksumOneXxh)
	if err != nil {
		t.Fatalf("entry %q: CopyAndChecksum: %v", name, err)
	}
	if n != entry.Size {
		t.Fatalf("entry %q: read %d bytes, want %d", name, n, entry.Size)
	}
	if !actual.Equal(r.Cksum()) {
		t.Fatalf("entry %q: xxhash mismatch at offset %d size %d", name, entry.Offset, entry.Size)
	}
}

// Verify that SrcCksum and SrcSize survive
// a Pack => Unpack round-trip, and that IsStale returns the correct answer
// when compared against the original values.
func TestShardSrcMetadataRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		srcCksum *cos.Cksum
		srcSize  int64
	}{
		{"nil_cksum", nil, 0},
		{"none_cksum", cos.NewCksum(cos.ChecksumNone, ""), 0},
		{"xxhash_cksum", cos.NewCksum(cos.ChecksumOneXxh, "0123456789abcdef"), 1 << 20},
		{"md5_cksum", cos.NewCksum(cos.ChecksumMD5, "d41d8cd98f00b204e9800998ecf8427e"), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orig, err := archive.NewShardIndexTestOnly(tc.srcCksum, tc.srcSize,
				map[string]archive.ShardIndexEntry{"a.txt": {Offset: 0, Size: 512}})
			if err != nil {
				t.Fatal("NewShardIndexTestOnly:", err)
			}
			b := packShardIndex(t, orig)

			got := readShardIndex(t, b)

			// Entry must survive.
			if got.Len() != 1 {
				t.Fatalf("expected 1 entry, got %d", got.Len())
			}

			// SrcSize must match.
			if got.SrcSize() != tc.srcSize {
				t.Fatalf("SrcSize: got %d, want %d", got.SrcSize(), tc.srcSize)
			}

			// SrcCksum must match (nil/none inputs both serialize as "none").
			if tc.srcCksum == nil || cos.NoneC(tc.srcCksum) {
				if !cos.NoneC(got.SrcCksum()) {
					t.Fatalf("SrcCksum: got %v, want none", got.SrcCksum())
				}
			} else {
				if !got.SrcCksum().Equal(tc.srcCksum) {
					t.Fatalf("SrcCksum: got %v, want %v", got.SrcCksum(), tc.srcCksum)
				}
			}

			// IsStale must be false when compared against the same values.
			if got.IsStale(tc.srcCksum, tc.srcSize) {
				t.Fatal("IsStale: expected false for matching cksum+size")
			}
		})
	}
}

// Verify IsStale using a realistic re-upload scenario:
// build a real shard, stamp the index with its cksum/size, then "re-upload"
// (a new shard with different content) and confirm IsStale fires.
func TestShardIsStale(t *testing.T) {
	// Build the original shard and compute its checksum.
	fh1 := tempFile(t)
	defer fh1.Close()
	tw := tar.NewWriter(fh1)
	writeTAREntry(t, tw, &tar.Header{Name: "a.txt", Mode: 0o644, ModTime: modTime, Format: tar.FormatUSTAR}, 1024, randReader(t, 1024))
	size1 := sealTAR(t, fh1, tw)
	ck1 := shardCksum(t, fh1, size1)

	// Build the index, stamped with the original shard's metadata.
	idx, err := archive.BuildShardIndex(fh1, size1, ck1)
	if err != nil {
		t.Fatal("BuildShardIndex:", err)
	}

	b := packShardIndex(t, idx)
	loaded := readShardIndex(t, b)

	// Same shard - index is fresh.
	if loaded.IsStale(ck1, size1) {
		t.Fatal("IsStale: expected false for unchanged shard")
	}

	// Re-upload: a new shard with different content => different cksum and size.
	fh2 := tempFile(t)
	defer fh2.Close()
	tw2 := tar.NewWriter(fh2)
	writeTAREntry(t, tw2, &tar.Header{Name: "a.txt", Mode: 0o644, ModTime: modTime, Format: tar.FormatUSTAR}, 2048, randReader(t, 2048))
	size2 := sealTAR(t, fh2, tw2)
	ck2 := shardCksum(t, fh2, size2)

	// Re-uploaded shard => index is stale.
	if !loaded.IsStale(ck2, size2) {
		t.Fatal("IsStale: expected true after shard re-upload")
	}
}

// shardCksum computes the xxhash checksum of the first `size` bytes of fh.
func shardCksum(t *testing.T, fh *os.File, size int64) *cos.Cksum {
	t.Helper()
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		t.Fatal("Seek:", err)
	}
	_, ckh, err := cos.ChecksumReader(io.LimitReader(fh, size), cos.ChecksumOneXxh)
	if err != nil {
		t.Fatal("shardCksum:", err)
	}
	return ckh.Clone()
}

// Return a minimal valid 512-byte TAR header block with Typeflag=TypeGNUSparse and Size=0
// (no data blocks follow).
//
// Ref: https://www.gnu.org/software/tar/manual/html_node/Standard.html
//
//	https://www.gnu.org/software/tar/manual/html_section/Sparse-Formats.html
//
// A USTAR/GNU header block is exactly 512 bytes.  The layout used here:
//
//	[  0.. 99]  name       - filename, null-padded
//	[100..107]  mode       - octal string, e.g. "0000644\0"
//	[124..135]  size       - octal string, 11 digits + null; 0 here (no data)
//	[136..147]  mtime      - octal string, 11 digits + null
//	[148..155]  checksum   - sum of all header bytes (checksum field as spaces)
//	[156]       typeflag   - 'S' = TypeGNUSparse
//	[257..262]  magic      - "ustar  \0" (GNU variant: two spaces, not "00")
//
// All fields not listed above remain zero, which is valid for a size=0 entry.
func gnuSparseHeader(name string) [512]byte {
	var blk [512]byte

	copy(blk[0:], name)
	copy(blk[100:], fmt.Sprintf("%07o\x00", 0o644))           // mode
	copy(blk[124:], fmt.Sprintf("%011o\x00", 0))              // size = 0
	copy(blk[136:], fmt.Sprintf("%011o\x00", modTime.Unix())) // mtime
	blk[156] = tar.TypeGNUSparse                              // typeflag = 'S'
	copy(blk[257:], "ustar  \x00")                            // GNU magic (two spaces)

	// Checksum: arithmetic sum of every byte in the 512-byte block,
	// with the 8-byte checksum field itself treated as all spaces (0x20).
	// The result is written back as a 6-digit octal string followed by
	// null and space - the standard GNU/POSIX checksum encoding.
	var chk int
	for i, b := range &blk {
		if i >= 148 && i < 156 {
			chk += 32 // treat checksum field as spaces
		} else {
			chk += int(b)
		}
	}
	copy(blk[148:], fmt.Sprintf("%06o\x00 ", chk))

	return blk
}
