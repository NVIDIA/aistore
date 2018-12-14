/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/zip"
	"io"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	jsoniter "github.com/json-iterator/go"
)

var (
	_ ExtractCreator = &zipExtractCreator{}
)

type (
	zipExtractCreator struct{}

	zipFileHeader struct {
		Name    string `json:"name"`
		Comment string `json:"comment"`
	}

	// zipRecordDataReader is used for writing metadata as well as data to the buffer.
	zipRecordDataReader struct {
		slab *memsys.Slab2

		metadataSize int64
		size         int64
		written      int64
		metadataBuf  []byte
		header       zipFileHeader
		zipWriter    *zip.Writer

		writer io.Writer
	}
)

func newZipRecordDataReader() *zipRecordDataReader {
	rd := &zipRecordDataReader{}
	rd.metadataBuf, rd.slab = mem.AllocFromSlab2(cmn.KiB)
	return rd
}

func (rd *zipRecordDataReader) reinit(zw *zip.Writer, size int64, metadataSize int64) {
	rd.grow(metadataSize)
	rd.zipWriter = zw
	rd.written = 0
	rd.size = size
	rd.metadataSize = metadataSize
}

func (rd *zipRecordDataReader) grow(size int64) {
	if int64(len(rd.metadataBuf)) < size {
		rd.slab.Free(rd.metadataBuf)
		rd.metadataBuf, rd.slab = mem.AllocFromSlab2(size)
	}
}

func (rd *zipRecordDataReader) free() {
	rd.slab.Free(rd.metadataBuf)
}

func (rd *zipRecordDataReader) Write(p []byte) (int, error) {
	// Read header and initialize file writer
	remainingMetadataSize := rd.metadataSize - rd.written
	if remainingMetadataSize > 0 {
		if int64(len(p)) < remainingMetadataSize {
			copy(rd.metadataBuf[rd.written:], p)
			rd.written += int64(len(p))
			return len(p), nil
		}

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

// ExtractShard reads the tarball f and extracts its metadata.
func (z *zipExtractCreator) ExtractShard(fqn string, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		zr   *zip.Reader
		size int64
	)

	if zr, err = zip.NewReader(r, r.Size()); err != nil {
		return extractedSize, extractedCount, err
	}

	buf, slab := mem.AllocFromSlab2(cmn.MiB)
	defer slab.Free(buf)
	for _, f := range zr.File {
		header := f.FileHeader
		metadata := zipFileHeader{
			Name:    header.Name,
			Comment: header.Comment,
		}

		bmeta, err := jsoniter.Marshal(metadata)
		if err != nil {
			return extractedSize, extractedCount, err
		}

		if f.FileInfo().IsDir() {
			// We can safely ignore this case because we do `MkdirAll` anyway
			// when we create files. And since dirs can appear after all the files
			// we must have this `MkdirAll` before files.
			continue
		} else {
			file, err := f.Open()
			if err != nil {
				return extractedSize, extractedCount, err
			}

			data := cmn.NewSizedReader(file, int64(header.UncompressedSize64))
			if size, err = extractor.ExtractRecordWithBuffer(fqn, header.Name, data, bmeta, toDisk, buf); err != nil {
				file.Close()
				return extractedSize, extractedCount, err
			}
			file.Close()
		}

		extractedSize += size
		extractedCount++
	}

	return extractedSize, extractedCount, nil
}

func NewZipExtractCreator() *zipExtractCreator {
	return &zipExtractCreator{}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (z *zipExtractCreator) CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var n int64
	zw := zip.NewWriter(w)
	defer zw.Close()

	rdReader := newZipRecordDataReader()
	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			rdReader.reinit(zw, obj.Size, obj.MetadataSize)
			if n, err = loadContent(rdReader, rec, obj); err != nil {
				return written + n, err
			}

			written += n
		}
	}
	rdReader.free()
	return written, nil
}

func (z *zipExtractCreator) UsingCompression() bool {
	return true
}

func (z *zipExtractCreator) MetadataSize() int64 {
	return 0 // zip does not have header size
}
