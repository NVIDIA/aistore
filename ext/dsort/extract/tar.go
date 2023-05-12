// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

var (
	// Predefined padding buffer (zero-initialized).
	padBuf [cos.TarBlockSize]byte

	// interface guard
	_ Creator = (*tarExtractCreator)(nil)
)

type (
	tarExtractCreator struct {
		t cluster.Target
	}

	// tarRecordDataReader is used for writing metadata as well as data to the buffer.
	tarRecordDataReader struct {
		slab *memsys.Slab

		metadataSize int64
		size         int64
		written      int64
		metadataBuf  []byte
		tarWriter    *tar.Writer
	}
)

func newTarRecordDataReader(t cluster.Target) *tarRecordDataReader {
	rd := &tarRecordDataReader{}
	rd.metadataBuf, rd.slab = t.ByteMM().Alloc()
	return rd
}

func (rd *tarRecordDataReader) reinit(tw *tar.Writer, size, metadataSize int64) {
	rd.tarWriter = tw
	rd.written = 0
	rd.size = size
	rd.metadataSize = metadataSize
}

func (rd *tarRecordDataReader) free() {
	rd.slab.Free(rd.metadataBuf)
}

func (rd *tarRecordDataReader) Write(p []byte) (int, error) {
	// Write header
	remainingMetadataSize := rd.metadataSize - rd.written
	if remainingMetadataSize > 0 {
		writeN := int64(len(p))
		if writeN < remainingMetadataSize {
			debug.Assert(int64(len(rd.metadataBuf))-rd.written >= writeN)
			copy(rd.metadataBuf[rd.written:], p)
			rd.written += writeN
			return len(p), nil
		}

		debug.Assert(int64(len(rd.metadataBuf))-rd.written >= remainingMetadataSize)
		copy(rd.metadataBuf[rd.written:], p[:remainingMetadataSize])
		rd.written += remainingMetadataSize
		p = p[remainingMetadataSize:]
		var header tar.Header
		if err := jsoniter.Unmarshal(rd.metadataBuf[:rd.metadataSize], &header); err != nil {
			return int(remainingMetadataSize), err
		}

		if err := rd.tarWriter.WriteHeader(&header); err != nil {
			return int(remainingMetadataSize), err
		}
	} else {
		remainingMetadataSize = 0
	}

	n, err := rd.tarWriter.Write(p)
	rd.written += int64(n)
	return n + int(remainingMetadataSize), err
}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *tarExtractCreator) ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor,
	toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size   int64
		header *tar.Header
		tr     = tar.NewReader(r)
	)

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
		offset += t.MetadataSize()

		if header.Typeflag == tar.TypeDir {
			// We can safely ignore this case because we do `MkdirAll` anyway
			// when we create files. And since dirs can appear after all the files
			// we must have this `MkdirAll` before files.
			continue
		}

		data := cos.NewSizedReader(tr, header.Size)
		extractMethod := ExtractToMem
		if toDisk {
			extractMethod = ExtractToDisk
		}
		args := extractRecordArgs{
			shardName:     lom.ObjName,
			fileType:      fs.ObjectType,
			recordName:    header.Name,
			r:             data,
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
		offset += cos.CeilAlignInt64(header.Size, cos.TarBlockSize)
	}
}

func NewTarExtractCreator(t cluster.Target) Creator {
	return &tarExtractCreator{t: t}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *tarExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		tw        = tar.NewWriter(tarball)
		rdReader  = newTarRecordDataReader(t.t)
	)

	defer func() {
		rdReader.free()
		cos.Close(tw)
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

				if n, err = loadContent(tarball, rec, obj); err != nil {
					return written + n, err
				}

				// pad to 512 bytes
				diff := cos.CeilAlignInt64(n, cos.TarBlockSize) - n
				if diff > 0 {
					if _, err = tarball.Write(padBuf[:diff]); err != nil {
						return written + n, err
					}
					n += diff
				}
				debug.Assert(diff >= 0 && diff < cos.TarBlockSize)
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

func (*tarExtractCreator) UsingCompression() bool { return false }
func (*tarExtractCreator) SupportsOffset() bool   { return true }
func (*tarExtractCreator) MetadataSize() int64    { return cos.TarBlockSize } // size of tar header with padding
