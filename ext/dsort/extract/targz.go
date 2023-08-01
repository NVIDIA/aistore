// Package extract provides ExtractShard and associated methods for dsort
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package extract

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
)

type targzExtractCreator struct {
	t   cluster.Target
	ext string
}

// interface guard
var _ Creator = (*targzExtractCreator)(nil)

func NewTargzExtractCreator(t cluster.Target, ext string) Creator {
	return &targzExtractCreator{t: t, ext: ext}
}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *targzExtractCreator) ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
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
	s.buf = buf

	_, err = ar.Range("", s.xtar)

	slab.Free(buf)
	cos.Close(s.tw)
	cos.Close(wfh)
	return s.extractedSize, s.extractedCount, err
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *targzExtractCreator) CreateShard(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		gzw, _    = gzip.NewWriterLevel(tarball, gzip.BestSpeed)
		tw        = tar.NewWriter(gzw)
		rdReader  = newTarRecordDataReader(t.t)
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

func (*targzExtractCreator) UsingCompression() bool { return true }
func (*targzExtractCreator) SupportsOffset() bool   { return true }
func (*targzExtractCreator) MetadataSize() int64    { return archive.TarBlockSize } // size of tar header with padding
