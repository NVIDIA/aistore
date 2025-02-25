// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// common method to compress .tar via `aw` (archive writer)
func writeCompressedTar(s *Shard, tw *tar.Writer, aw io.Writer, loader ContentLoader, rdReader *tarRecordW) (written int64, _ error) {
	var needFlush bool
	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			switch obj.StoreType {
			case OffsetStoreType:
				if needFlush {
					// write directly to the underlying tar file, so flush everything written so far
					if err := tw.Flush(); err != nil {
						return written, err
					}
					needFlush = false
				}
				n, err := loader.Load(aw, rec, obj)
				written += n
				if err != nil {
					return written, err
				}
				// pad to 512
				diff := cos.CeilAlignI64(n, archive.TarBlockSize) - n
				debug.Assert(diff >= 0)
				if diff > 0 {
					npad, errP := aw.Write(padBuf[:diff])
					written += int64(npad)
					if errP != nil {
						return written, errP
					}
					debug.Assert(diff == int64(npad) && diff < archive.TarBlockSize, diff, npad)
				}
			case SGLStoreType, DiskStoreType:
				rdReader.reinit(tw, obj.Size, obj.MetadataSize)
				n, err := loader.Load(rdReader, rec, obj)
				written += n
				if err != nil {
					return written, err
				}
				needFlush = true
			default:
				debug.Assert(false, obj.StoreType)
			}
		}
	}
	return written, nil
}
