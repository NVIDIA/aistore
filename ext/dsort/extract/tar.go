// Package extract provides Extract(shard), Create(shard), and associated methods for dsort
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/tar"
	"io"
	"strconv"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

type (
	tarRW struct {
		t   cluster.Target
		ext string
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

var (
	// Predefined padding buffer (zero-initialized).
	padBuf [archive.TarBlockSize]byte

	// interface guard
	_ Creator = (*tarRW)(nil)
)

func NewTarRW(t cluster.Target) Creator {
	return &tarRW{t: t, ext: archive.ExtTar}
}

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

// Extract reads the tarball f and extracts its metadata.
func (t *tarRW) Extract(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(t.ext, r)
	if err != nil {
		return 0, 0, err
	}
	s := &rcbCtx{parent: t, tw: nil, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk}
	buf, slab := t.t.PageMM().AllocSize(lom.SizeBytes())
	s.buf = buf

	_, err = ar.Range("", s.xtar)

	slab.Free(buf)
	return s.extractedSize, s.extractedCount, err
}

// Create creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *tarRW) Create(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
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

				if n, err = loader.Load(tarball, rec, obj); err != nil {
					return written + n, err
				}

				// pad to 512 bytes
				diff := cos.CeilAlignInt64(n, archive.TarBlockSize) - n
				if diff > 0 {
					if _, err = tarball.Write(padBuf[:diff]); err != nil {
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

func (*tarRW) UsingCompression() bool { return false }
func (*tarRW) SupportsOffset() bool   { return true }
func (*tarRW) MetadataSize() int64    { return archive.TarBlockSize } // size of tar header with padding

// NOTE: Mostly taken from `tar.formatPAXRecord`.
func estimateXHeaderSize(paxRecords map[string]string) int64 {
	totalSize := 0
	for k, v := range paxRecords {
		const padding = 3 // Extra padding for ' ', '=', and '\n'
		size := len(k) + len(v) + padding
		size += len(strconv.Itoa(size))
		record := strconv.Itoa(size) + " " + k + "=" + v + "\n"

		// Final adjustment if adding size field increased the record size.
		if len(record) != size {
			record = strconv.Itoa(len(record)) + " " + k + "=" + v + "\n"
		}

		totalSize += len(record)
	}
	return int64(totalSize)
}
