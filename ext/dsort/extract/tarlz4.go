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
	"github.com/NVIDIA/aistore/ext/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/pierrec/lz4/v3"
)

type tarlz4ExtractCreator struct {
	t cluster.Target
}

// interface guard
var _ Creator = (*tarlz4ExtractCreator)(nil)

func NewTarlz4ExtractCreator(t cluster.Target) Creator {
	return &tarlz4ExtractCreator{t: t}
}

// ExtractShard  the tarball f and extracts its metadata.
func (t *tarlz4ExtractCreator) ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor,
	toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size    int64
		header  *tar.Header
		workFQN = fs.CSM.Gen(lom, filetype.DSortFileType, "") // tarFQN
	)

	lzr := lz4.NewReader(r)
	tr := tar.NewReader(lzr)

	// extract to .tar
	f, err := cos.CreateFile(workFQN)
	if err != nil {
		return 0, 0, err
	}
	tw := tar.NewWriter(f)
	defer func() {
		cos.Close(tw)
		cos.Close(f)
	}()

	buf, slab := t.t.PageMM().AllocSize(lom.SizeBytes())
	defer slab.Free(buf)

	offset := int64(0)
	for {
		header, err = tr.Next()
		if err == io.EOF {
			return extractedSize, extractedCount, nil
		} else if err != nil {
			return extractedSize, extractedCount, err
		}

		bmeta := cos.MustMarshal(header)

		if err := tw.WriteHeader(header); err != nil {
			return extractedSize, extractedCount, err
		}

		offset += t.MetadataSize()

		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Format == tar.FormatPAX {
			offset += t.MetadataSize()
			size := estimateXHeaderSize(header.PAXRecords)
			size = cos.CeilAlignInt64(size, archive.TarBlockSize)
			offset += size
		}

		data := cos.NewSizedReader(tr, header.Size)
		extractMethod := ExtractToMem
		if toDisk {
			extractMethod = ExtractToDisk
		}
		extractMethod.Set(ExtractToWriter)

		args := extractRecordArgs{
			shardName:     lom.ObjName,
			fileType:      filetype.DSortFileType,
			recordName:    header.Name,
			r:             data,
			w:             tw,
			metadata:      bmeta,
			extractMethod: extractMethod,
			offset:        offset,
			buf:           buf,
		}
		if size, err = extractor.ExtractRecordWithBuffer(args); err != nil {
			return extractedSize, extractedCount, err
		}

		extractedSize += size
		extractedCount++

		// .tar format pads all block to 512 bytes
		offset += cos.CeilAlignInt64(header.Size, archive.TarBlockSize)
	}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, lzw, then finally tarball.
func (t *tarlz4ExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
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

				if n, err = loadContent(lzw, rec, obj); err != nil {
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
				if n, err = loadContent(rdReader, rec, obj); err != nil {
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
