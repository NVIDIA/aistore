// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"encoding/binary"
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
//	│  [0]      metaversion  uint8   — shardIdxMetaver                        │
//	│  [1]      format       uint8   — 0 = TAR (shardIdxFmtTAR)               │
//	│  [2]      cksum type   uint8   — 1 = onexxh (shardIdxCksumXXH)          │
//	│  [3..10]  xxhash64     uint64  — onexxh.Checksum64S(payload, MLCG32)    │
//	├─────────────────────────────────────────────────────────────────────────┤
//	│  PAYLOAD (variable, covered by xxhash)                                  │
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
	shardIdxMetaver  = 1  // current version of the shard index binary format
	shardIdxFmtTAR   = 0  // format: TAR
	shardIdxCksumXXH = 1  // checksum: onexxh.Checksum64S with cos.MLCG32 seed
	shardIdxPrefLen  = 11 // [1:ver | 1:fmt | 1:cksum-type | 8:xxhash64]
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
		raw     []byte // (GC)
	}
)

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
// binary.AppendUvarint grows the slice and appends the encoded bytes in one call.
func (idx *ShardIndex) Pack() ([]byte, error) {
	var payload []byte

	// 1. number of entries
	payload = binary.AppendUvarint(payload, uint64(len(idx.Entries)))

	// 2. for each entry: name length, name bytes, TAR header offset, file size
	for name, e := range idx.Entries {
		if e.Offset < 0 {
			return nil, _emitErr("entry %q has negative offset %d", name, e.Offset)
		}
		if e.Size < 0 {
			return nil, _emitErr("entry %q has negative size %d", name, e.Size)
		}
		payload = binary.AppendUvarint(payload, uint64(len(name)))
		payload = append(payload, name...)
		payload = binary.AppendUvarint(payload, uint64(e.Offset))
		payload = binary.AppendUvarint(payload, uint64(e.Size))
	}

	// 3. checksum the payload, then prepend the fixed preamble
	h := onexxh.Checksum64S(payload, cos.MLCG32)

	buf := make([]byte, shardIdxPrefLen+len(payload))
	buf[0] = shardIdxMetaver
	buf[1] = shardIdxFmtTAR
	buf[2] = shardIdxCksumXXH
	binary.BigEndian.PutUint64(buf[3:], h)
	copy(buf[shardIdxPrefLen:], payload)

	return buf, nil
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

	var (
		storedHash = binary.BigEndian.Uint64(b[3:])
		payload    = b[shardIdxPrefLen:]
	)
	if h := onexxh.Checksum64S(payload, cos.MLCG32); h != storedHash {
		return _emitErr("checksum mismatch (stored %016x, computed %016x)", storedHash, h)
	}

	count, nCount := binary.Uvarint(payload)
	if nCount <= 0 {
		return _emitErr("failed to decode entry count")
	}
	off := nCount

	entries := make(map[string]ShardIndexEntry, count)
	for range count {
		nameLen, nNameLen := binary.Uvarint(payload[off:])
		if nNameLen <= 0 {
			return _emitErr("failed to decode name length")
		}
		off += nNameLen
		if nameLen > math.MaxInt {
			return _emitErr("name length too large")
		}
		if off+int(nameLen) > len(payload) {
			return _emitErr("name overruns buffer (len %d)", nameLen)
		}
		// UnsafeS aliases payload; caller must NOT modify the input buffer while the ShardIndex is in use.
		name := cos.UnsafeS(payload[off : off+int(nameLen)])
		off += int(nameLen)

		offset, nOffset := binary.Uvarint(payload[off:])
		if nOffset <= 0 {
			return _emitErr("failed to decode offset for %q", name)
		}
		off += nOffset

		size, nSize := binary.Uvarint(payload[off:])
		if nSize <= 0 {
			return _emitErr("failed to decode size for %q", name)
		}
		off += nSize

		if offset > math.MaxInt64 {
			return _emitErr("offset %d overflows int64 for %q", offset, name)
		}
		if size > math.MaxInt64 {
			return _emitErr("size %d overflows int64 for %q", size, name)
		}
		entries[name] = ShardIndexEntry{
			Offset: int64(offset),
			Size:   int64(size),
		}
	}

	idx.Entries = entries
	idx.raw = payload // retain for entry names
	return nil
}

/////////////////////
// ShardIndexEntry //
/////////////////////

// DataOffset returns the byte offset of the file's data within the archive.
// Callers use this for direct random access: io.NewSectionReader(r, entry.DataOffset(), entry.Size).
func (e ShardIndexEntry) DataOffset() int64 { return e.Offset + TarBlockSize }
