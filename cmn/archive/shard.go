// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	onexxh "github.com/OneOfOne/xxhash"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Binary layout of a packed ShardIndex:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│  PREAMBLE (11 bytes, fixed)                                             │
//	│  [0]      meta-version uint8   — shardIdxMetaver                        │
//	│  [1]      format       uint8   — 0 = TAR (shardIdxFmtTAR)               │
//	│  [2]      cksum type   uint8   — 1 = onexxh (shardIdxCksumXXH)          │
//	│  [3..10]  xxhash64     uint64  — onexxh.Checksum64S(payload, MLCG32)    │
//	├─────────────────────────────────────────────────────────────────────────┤
//	│  PAYLOAD (variable, covered by xxhash)                                  │
//	│  src_cksum_type_len  uvarint — byte length of LOM cksum type            │
//	│  src_cksum_type      []byte  — e.g. "xxhash" (UTF-8)                    │
//	│  src_cksum_val_len   uvarint — byte length of LOM cksum value           │
//	│  src_cksum_val       []byte  — hex-encoded value                        │
//	│  src_size            uvarint — LOM size in bytes                        │
//	│  count    uvarint              — number of entries                      │
//	│  for each entry:                                                        │
//	│    name_len  uvarint           — byte length of the name string         │
//	│    name      []byte            — UTF-8 file path                        │
//	│    offset    uvarint           — byte offset of the TAR header block    │
//	│    size      uvarint           — logical file size in bytes             │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// This indexer supports regular-file members from the common TAR variants handled by Go's stdlib,
// including long-name PAX/GNU cases. We intentionally skip sparse (tar.TypeGNUSparse)
// and non-regular entries (directories, links, device nodes, FIFOs).

const (
	shardIdxMetaver  = 1  // current meta-version
	shardIdxFmtTAR   = 0  // format: TAR
	shardIdxCksumXXH = 1  // checksum: onexxh.Checksum64S with cos.MLCG32 seed
	shardIdxPrefLen  = 11 // [1:ver | 1:fmt | 1:cksum-type | 8:xxhash64]

	// ShardIdxMinLen: preamble + four single-byte uvarints (lower bound, not exact).
	ShardIdxMinLen = shardIdxPrefLen + 4
)

type (
	ShardIndexEntry struct {
		// Offset is the byte offset of the file's 512-byte TAR header block within the archive.
		// File data begins immediately after: Offset + TarBlockSize.
		// Always a multiple of TarBlockSize; the first entry in a shard can be at offset 0.
		Offset int64

		// File size in bytes (as recorded in the TAR header).
		Size int64
	}
	ShardIndex struct {
		Entries map[string]ShardIndexEntry
		// SrcCksum and SrcSize are the LOM's checksum and size captured at index-build time.
		// to detect re-uploaded shards without reading the TAR content.
		// Set by the caller before passing the index to SaveShardIndex.
		SrcCksum *cos.Cksum
		SrcSize  int64
		raw      []byte // (GC)
	}
	// idxDecoder is a cursor for sequential decoding of the shard-index payload.
	idxDecoder struct {
		b   []byte
		off int
	}
)

// ErrShardIdxStale is returned by core.LoadShardIndex when the stored index was built
// from a prior version of the shard (checksum or size mismatch). The caller should rebuild.
var ErrShardIdxStale = errors.New("shard index: stale")

func _emitErr(format string, a ...any) error { return fmt.Errorf("shard index: "+format, a...) }

// BuildShardIndex performs one sequential scan of a TAR and returns an index
// mapping each regular file's name to its exact byte location within the archive.
func BuildShardIndex(r io.ReaderAt, size int64) (*ShardIndex, error) {
	// initial capacity: upper bound is size/TarBlockSize (all zero-size files, one header each);
	// lower bound of 8 avoids degenerate near-zero estimates for tiny archives.
	const (
		minCap = 8
		maxCap = 8 * 1024
	)
	initCap := cos.ClampInt(int(size/TarBlockSize), minCap, maxCap)

	var (
		sr  = io.NewSectionReader(r, 0, size)
		tr  = tar.NewReader(sr)
		idx = &ShardIndex{Entries: make(map[string]ShardIndexEntry, initCap)}
	)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return idx, nil
			}
			return nil, _emitErr("tar reader failure: %w", err)
		}
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			// regular file — index below
		case tar.TypeGNUSparse:
			continue // not indexed: logical size != physical; caller falls back to sequential scan
		default:
			continue // skip directories, symlinks, devices, etc.
		}
		if _, exists := idx.Entries[hdr.Name]; exists {
			continue // first-wins: matches ReadOne semantics
		}
		// After tr.Next() the section reader is positioned at the data start.
		// TAR guarantees all headers and data are aligned to TarBlockSize (512 bytes),
		// so dataOffset is always an exact multiple.
		dataOffset, _ := sr.Seek(0, io.SeekCurrent)
		debug.Assert(dataOffset&(TarBlockSize-1) == 0, dataOffset)

		idx.Entries[hdr.Name] = ShardIndexEntry{
			Offset: dataOffset - TarBlockSize, // points to the 512-byte file header, not the data
			Size:   hdr.Size,
		}
	}
}

////////////////
// ShardIndex //
////////////////

// Pack serializes the index into a compact binary format.
func (idx *ShardIndex) Pack() ([]byte, error) {
	if idx.SrcSize < 0 {
		return nil, _emitErr("negative src size %d", idx.SrcSize)
	}
	cksumTy, cksumVal := idx.SrcCksum.Get() // nil-safe; returns ("none", "") when nil

	// Pre-allocate a single buffer: preamble placeholder, then payload appended in-place.
	// Payload: src cksum type + value (each length-prefixed), src size, entry count, entries.
	total := shardIdxPrefLen +
		2*binary.MaxVarintLen64 + len(cksumTy) + len(cksumVal) + // src cksum type + value
		binary.MaxVarintLen64 + // src size
		binary.MaxVarintLen64 // entry count
	for name := range idx.Entries {
		// one uvarint for name length + the name itself + two uvarints for offset and size
		total += 3*binary.MaxVarintLen64 + len(name)
	}
	buf := make([]byte, shardIdxPrefLen, total)

	// Source metadata — allows LoadShardIndex to detect stale indexes.
	buf = binary.AppendUvarint(buf, uint64(len(cksumTy)))
	buf = append(buf, cksumTy...)
	buf = binary.AppendUvarint(buf, uint64(len(cksumVal)))
	buf = append(buf, cksumVal...)
	buf = binary.AppendUvarint(buf, uint64(idx.SrcSize))

	// Entry count and entries.
	buf = binary.AppendUvarint(buf, uint64(len(idx.Entries)))
	for name, e := range idx.Entries {
		if e.Offset < 0 {
			return nil, _emitErr("entry %q has negative offset %d", name, e.Offset)
		}
		if e.Size < 0 {
			return nil, _emitErr("entry %q has negative size %d", name, e.Size)
		}
		buf = binary.AppendUvarint(buf, uint64(len(name)))
		buf = append(buf, name...)
		buf = binary.AppendUvarint(buf, uint64(e.Offset))
		buf = binary.AppendUvarint(buf, uint64(e.Size))
	}

	// Checksum the payload, then fill in the preamble.
	h := onexxh.Checksum64S(buf[shardIdxPrefLen:], cos.MLCG32)
	buf[0] = shardIdxMetaver
	buf[1] = shardIdxFmtTAR
	buf[2] = shardIdxCksumXXH
	binary.BigEndian.PutUint64(buf[3:], h)

	return buf, nil
}

/////////////////////
// ShardIndexEntry //
/////////////////////

// DataOffset returns the byte offset of the file's data within the archive.
// Callers use this for direct random access: io.NewSectionReader(r, entry.DataOffset(), entry.Size).
func (e ShardIndexEntry) DataOffset() int64 { return e.Offset + TarBlockSize }

////////////////
// idxDecoder //
////////////////

// readU64 reads a length-prefixed (uvarint) uint64.
// The `field` argument is for error messages only.
func (d *idxDecoder) readU64(field string) (uint64, error) {
	v, n := binary.Uvarint(d.b[d.off:])
	if n <= 0 {
		return 0, _emitErr("failed to decode %s", field)
	}
	d.off += n
	return v, nil
}

// The `field` argument is for error messages only.
func (d *idxDecoder) readI64(field string) (int64, error) {
	v, err := d.readU64(field)
	if err != nil {
		return 0, err
	}
	if v > math.MaxInt64 {
		return 0, _emitErr("%s overflows int64", field)
	}
	return int64(v), nil
}

// readStr reads a length-prefixed (uvarint) string, copying bytes from the payload.
// The `field` argument is for error messages only.
func (d *idxDecoder) readStr(field string) (string, error) {
	slen, err := d.readU64(field + " length")
	if err != nil {
		return "", err
	}
	if slen > math.MaxInt {
		return "", _emitErr("%s length too large", field)
	}
	end := d.off + int(slen)
	if end > len(d.b) {
		return "", _emitErr("%s overruns buffer", field)
	}
	s := string(d.b[d.off:end])
	d.off = end
	return s, nil
}

// readName reads a length-prefixed entry name via cos.UnsafeS (zero-copy).
// Caller must retain idx.raw to keep the payload alive while the ShardIndex is in use.
func (d *idxDecoder) readName() (string, error) {
	nlen, err := d.readU64("name length")
	if err != nil {
		return "", err
	}
	if nlen > math.MaxInt {
		return "", _emitErr("name length too large")
	}
	end := d.off + int(nlen)
	if end > len(d.b) {
		return "", _emitErr("name overruns buffer")
	}
	name := cos.UnsafeS(d.b[d.off:end])
	d.off = end
	return name, nil
}

// Unpack deserializes a packed ShardIndex produced by Pack.
func (idx *ShardIndex) Unpack(b []byte) error {
	if len(b) < shardIdxPrefLen {
		return _emitErr("buffer underrun (%d bytes)", len(b))
	}
	if b[0] != shardIdxMetaver {
		return _emitErr("unsupported meta-version %d", b[0])
	}
	if b[1] != shardIdxFmtTAR {
		return _emitErr("unsupported format %d", b[1])
	}
	if b[2] != shardIdxCksumXXH {
		return _emitErr("unsupported checksum type %d", b[2])
	}

	storedHash := binary.BigEndian.Uint64(b[3:])
	payload := b[shardIdxPrefLen:]
	if h := onexxh.Checksum64S(payload, cos.MLCG32); h != storedHash {
		return _emitErr("checksum mismatch (stored %016x, computed %016x)", storedHash, h)
	}

	d := idxDecoder{b: payload}

	// Source metadata: cksum type, cksum value, size.
	cksumTy, err := d.readStr("src cksum type")
	if err != nil {
		return err
	}
	cksumVal, err := d.readStr("src cksum value")
	if err != nil {
		return err
	}
	srcSize, err := d.readI64("src size")
	if err != nil {
		return err
	}
	idx.SrcCksum = cos.NewCksum(cksumTy, cksumVal)
	idx.SrcSize = srcSize

	// Entry count and entries.
	count, err := d.readU64("entry count")
	if err != nil {
		return err
	}
	entries := make(map[string]ShardIndexEntry, count)
	for range count {
		name, err := d.readName()
		if err != nil {
			return err
		}
		offset, err := d.readI64("offset")
		if err != nil {
			return err
		}
		size, err := d.readI64("size")
		if err != nil {
			return err
		}
		entries[name] = ShardIndexEntry{Offset: offset, Size: size}
	}

	idx.Entries = entries
	idx.raw = d.b // retain for entry names
	return nil
}

// IsStale reports whether the index was built from a different version of the shard.
// Always checks size; also compares cksum when SrcCksum is set.
func (idx *ShardIndex) IsStale(cksum *cos.Cksum, size int64) bool {
	if !cos.NoneC(idx.SrcCksum) && !idx.SrcCksum.Equal(cksum) {
		return true
	}
	return idx.SrcSize != size
}
