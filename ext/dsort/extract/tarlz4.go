// Package extract provides ExtractShard and associated methods for dsort
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/ct"
	"github.com/NVIDIA/aistore/fs"
	"github.com/pierrec/lz4/v3"
)

type tarlz4ExtractCreator struct {
	t   cluster.Target
	ext string
}

// interface guard
var _ Creator = (*tarlz4ExtractCreator)(nil)

func NewTarlz4ExtractCreator(t cluster.Target) Creator {
	return &tarlz4ExtractCreator{t: t, ext: archive.ExtTarLz4}
}

// ExtractShard  the tarball f and extracts its metadata.
func (t *tarlz4ExtractCreator) ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(t.ext, r)
	if err != nil {
		return 0, 0, err
	}
	workFQN := fs.CSM.Gen(lom, ct.DSortFileType, "") // tarFQN
	wfh, err := cos.CreateFile(workFQN)
	if err != nil {
		return 0, 0, err
	}

	s := &rcbCtx{parent: t, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk}
	s.tw = tar.NewWriter(wfh)
	buf, slab := t.t.PageMM().AllocSize(lom.SizeBytes())

	_, err = ar.Range("", s.xtar)

	slab.Free(buf)
	cos.Close(s.tw)
	cos.Close(wfh)
	return s.extractedSize, s.extractedCount, err
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, lzw, then finally tarball.
func (t *tarlz4ExtractCreator) CreateShard(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		lzw       = lz4.NewWriter(tarball)
		tw        = tar.NewWriter(lzw)
		rdReader  = newTarRecordDataReader(t.t)
	)

	defer func() {
		rdReader.free()
		cos.Close(tw)
		cos.Close(lzw)
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

				if n, err = loader.Load(lzw, rec, obj); err != nil {
					return written + n, err
				}

				// pad to 512 bytes
				diff := cos.CeilAlignInt64(n, archive.TarBlockSize) - n
				if diff > 0 {
					if _, err = lzw.Write(padBuf[:diff]); err != nil {
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

func (*tarlz4ExtractCreator) UsingCompression() bool { return true }
func (*tarlz4ExtractCreator) SupportsOffset() bool   { return true }
func (*tarlz4ExtractCreator) MetadataSize() int64    { return archive.TarBlockSize } // size of tar header with padding
