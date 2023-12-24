// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/zip"
	"io"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

type (
	zipRW struct {
		ext string
	}

	zipFileHeader struct {
		Name    string `json:"name"`
		Comment string `json:"comment"`
	}

	// zipRecordDataReader is used for writing metadata as well as data to the buffer.
	zipRecordDataReader struct {
		slab *memsys.Slab

		metadataSize int64
		size         int64
		written      int64
		metadataBuf  []byte
		header       zipFileHeader
		zipWriter    *zip.Writer

		writer io.Writer
	}
)

// interface guard
var _ RW = (*zipRW)(nil)

///////////
// zipRW //
///////////

func NewZipRW() RW { return &zipRW{ext: archive.ExtZip} }

func (*zipRW) IsCompressed() bool   { return true }
func (*zipRW) SupportsOffset() bool { return false }
func (*zipRW) MetadataSize() int64  { return 0 } // zip does not have header size

// Extract reads the tarball f and extracts its metadata.
func (zrw *zipRW) Extract(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(zrw.ext, r, lom.SizeBytes())
	if err != nil {
		return 0, 0, err
	}
	c := &rcbCtx{parent: zrw, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk}
	buf, slab := cluster.T.PageMM().AllocSize(lom.SizeBytes())
	c.buf = buf

	_, err = ar.Range("", c.xzip)

	slab.Free(buf)
	return c.extractedSize, c.extractedCount, err
}

// Create creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (*zipRW) Create(s *Shard, w io.Writer, loader ContentLoader) (written int64, err error) {
	var n int64
	zw := zip.NewWriter(w)
	defer cos.Close(zw)

	rdReader := newZipRecordDataReader()
	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			rdReader.reinit(zw, obj.Size, obj.MetadataSize)
			if n, err = loader.Load(rdReader, rec, obj); err != nil {
				return written + n, err
			}

			written += n
		}
	}
	rdReader.free()
	return written, nil
}

/////////////////////////
// zipRecordDataReader //
/////////////////////////

func newZipRecordDataReader() *zipRecordDataReader {
	rd := &zipRecordDataReader{}
	rd.metadataBuf, rd.slab = cluster.T.ByteMM().Alloc()
	return rd
}

func (rd *zipRecordDataReader) reinit(zw *zip.Writer, size, metadataSize int64) {
	rd.zipWriter = zw
	rd.written = 0
	rd.size = size
	rd.metadataSize = metadataSize
}

func (rd *zipRecordDataReader) free() {
	rd.slab.Free(rd.metadataBuf)
}

func (rd *zipRecordDataReader) Write(p []byte) (int, error) {
	// Read header and initialize file writer
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
		var metadata zipFileHeader
		if err := jsoniter.Unmarshal(rd.metadataBuf[:rd.metadataSize], &metadata); err != nil {
			return int(remainingMetadataSize), err
		}

		rd.header = metadata
		writer, err := rd.zipWriter.Create(rd.header.Name)
		if err != nil {
			return int(remainingMetadataSize), err
		}
		if err := rd.zipWriter.SetComment(rd.header.Comment); err != nil {
			return int(remainingMetadataSize), err
		}
		rd.writer = writer
	} else {
		remainingMetadataSize = 0
	}

	n, err := rd.writer.Write(p)
	rd.written += int64(n)
	return n + int(remainingMetadataSize), err
}
