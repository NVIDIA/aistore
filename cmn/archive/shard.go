// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	ShardIndexEntry struct {
		// Offset is the byte offset of the file's 512-byte TAR header block within the archive.
		// File data begins immediately after: Offset + TarBlockSize.
		// Always a multiple of TarBlockSize; the first entry in a shard can be at offset 0.
		Offset int64
		Size   int64 // logical file size in bytes (as recorded in the TAR header)
		// TODO: implement cos.Packer (Pack, PackedSize) and cos.Unpacker (Unpack)
	}
	ShardIndex struct {
		Entries map[string]*ShardIndexEntry
		// TODO: implement cos.Packer (Pack, PackedSize) and cos.Unpacker (Unpack)
	}
)

// TODO: add Save(path string) error — pack ShardIndex and write to file
// TODO: add LoadShardIndex(path string) (*ShardIndex, error) — read file and unpack

// DataOffset returns the byte offset of the file's data within the archive.
// Callers use this for direct random access: io.NewSectionReader(r, entry.DataOffset(), entry.Size).
func (e *ShardIndexEntry) DataOffset() int64 { return e.Offset + TarBlockSize }

// BuildShardIndex performs one sequential scan of a TAR and returns an index
// mapping each regular file's name to its exact byte location within the archive.
func BuildShardIndex(r io.ReaderAt, size int64) (*ShardIndex, error) {
	sr := io.NewSectionReader(r, 0, size)
	tr := tar.NewReader(sr)
	idx := &ShardIndex{Entries: make(map[string]*ShardIndexEntry)}
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return idx, nil
			}
			return nil, fmt.Errorf("BuildShardIndex: %w", err)
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
		debug.Assert(dataOffset%TarBlockSize == 0, dataOffset)
		idx.Entries[hdr.Name] = &ShardIndexEntry{
			Offset: dataOffset - TarBlockSize, // points to the 512-byte file header, not the data
			Size:   hdr.Size,
		}
	}
}
