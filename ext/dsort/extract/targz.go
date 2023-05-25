// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/ext/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
)

// interface guard
var _ Creator = (*targzExtractCreator)(nil)

type targzExtractCreator struct {
	t cluster.Target
}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *targzExtractCreator) ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor,
	toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size    int64
		header  *tar.Header
		workFQN = fs.CSM.Gen(lom, filetype.DSortFileType, "") // tarFQN
	)

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return 0, 0, err
	}
	defer cos.Close(gzr)
	tr := tar.NewReader(gzr)

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
			// We can safely ignore this case because we do `MkdirAll` anyway
			// when we create files. And since dirs can appear after all the files
			// we must have this `MkdirAll` before files.
			continue
		} else if header.Format == tar.FormatPAX {
			// When dealing with `tar.FormatPAX` we also need to take into
			// consideration the `tar.TypeXHeader` that comes before the actual header.
			// Together it looks like this: [x-header][pax-records][pax-header][pax-file].
			// Since `tar.Reader` skips over this header and writes to `header.PAXRecords`
			// we need to manually adjust the offset, otherwise when using the
			// offset we will point to totally wrong location.

			// Add offset for `tar.TypeXHeader`.
			offset += t.MetadataSize()

			// Add offset for size of PAX records - there is no way of knowing
			// the size, so we must estimate it by ourselves...
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

func NewTargzExtractCreator(t cluster.Target) Creator {
	return &targzExtractCreator{t: t}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *targzExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
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

				if n, err = loadContent(gzw, rec, obj); err != nil {
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
				if n, err = loadContent(rdReader, rec, obj); err != nil {
					return written + n, err
				}
				written += n

				needFlush = true
			default:
				cos.AssertMsg(false, obj.StoreType)
			}

			written += n
		}
	}

	return written, nil
}

func (*targzExtractCreator) UsingCompression() bool { return true }
func (*targzExtractCreator) SupportsOffset() bool   { return true }
func (*targzExtractCreator) MetadataSize() int64    { return archive.TarBlockSize } // size of tar header with padding
