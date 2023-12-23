// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"compress/gzip"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/ct"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/glob"
)

type tgzRW struct {
	ext string
}

// interface guard
var _ RW = (*tgzRW)(nil)

func NewTargzRW(ext string) RW { return &tgzRW{ext: ext} }

func (*tgzRW) IsCompressed() bool   { return true }
func (*tgzRW) SupportsOffset() bool { return true }
func (*tgzRW) MetadataSize() int64  { return archive.TarBlockSize } // size of tar header with padding

// Extract reads the tarball f and extracts its metadata.
// Writes work tar
func (trw *tgzRW) Extract(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(trw.ext, r)
	if err != nil {
		return 0, 0, err
	}
	workFQN := fs.CSM.Gen(lom, ct.DsortFileType, "") // tarFQN
	wfh, err := cos.CreateFile(workFQN)
	if err != nil {
		return 0, 0, err
	}

	c := &rcbCtx{parent: trw, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk}
	c.tw = tar.NewWriter(wfh)
	buf, slab := glob.T.PageMM().AllocSize(lom.SizeBytes())
	c.buf = buf

	_, err = ar.Range("", c.xtar)
	slab.Free(buf)
	if err == nil {
		cos.Close(c.tw)
	} else {
		_ = c.tw.Close()
	}
	cos.Close(wfh)
	return c.extractedSize, c.extractedCount, err
}

// Create creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (*tgzRW) Create(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		gzw, _    = gzip.NewWriterLevel(tarball, gzip.BestSpeed)
		tw        = tar.NewWriter(gzw)
		rdReader  = newTarRecordDataReader()
	)

	defer func() {
		rdReader.free()
		cos.Close(tw)
		cos.Close(gzw)
	}()

	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			switch obj.StoreType {
			case OffsetStoreType:
				if needFlush {
					// We now will write directly to the tarball file so we need
					// to flush everything what we have written so far.
					if err := tw.Flush(); err != nil {
						return written, err
					}
					needFlush = false
				}
				if n, err = loader.Load(gzw, rec, obj); err != nil {
					return written + n, err
				}
				// pad to 512 bytes
				diff := cos.CeilAlignInt64(n, archive.TarBlockSize) - n
				if diff > 0 {
					if _, err = gzw.Write(padBuf[:diff]); err != nil {
						return written + n, err
					}
					n += diff
				}
				debug.Assert(diff >= 0 && diff < archive.TarBlockSize)
			case SGLStoreType, DiskStoreType:
				rdReader.reinit(tw, obj.Size, obj.MetadataSize)
				if n, err = loader.Load(rdReader, rec, obj); err != nil {
					return written + n, err
				}
				written += n
				needFlush = true
			default:
				debug.Assert(false, obj.StoreType)
			}

			written += n
		}
	}
	return written, nil
}
