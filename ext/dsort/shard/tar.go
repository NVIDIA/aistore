//go:build sharding

// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"io"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/memsys"

	jsoniter "github.com/json-iterator/go"
)

type (
	tarRW struct {
		ext string
	}
	tarRecordW struct {
		tarWriter    *tar.Writer
		slab         *memsys.Slab
		metadataSize int64
		size         int64
		written      int64
		metadataBuf  []byte
	}
)

// interface guard
var _ RW = (*tarRW)(nil)

////////////////
// tarRecordW //
////////////////

func newTarRecordDataReader() *tarRecordW {
	rd := &tarRecordW{}
	rd.metadataBuf, rd.slab = core.T.ByteMM().Alloc()
	return rd
}

func (rd *tarRecordW) reinit(tw *tar.Writer, size, metadataSize int64) {
	rd.tarWriter = tw
	rd.written = 0
	rd.size = size
	rd.metadataSize = metadataSize
}

func (rd *tarRecordW) free() {
	rd.slab.Free(rd.metadataBuf)
}

func (rd *tarRecordW) Write(p []byte) (int, error) {
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

///////////
// tarRW //
///////////

func NewTarRW() RW { return &tarRW{ext: archive.ExtTar} }

func (*tarRW) IsCompressed() bool   { return false }
func (*tarRW) SupportsOffset() bool { return true }
func (*tarRW) MetadataSize() int64  { return archive.TarBlockSize } // size of tar header with padding

func (trw *tarRW) Extract(lom *core.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error) {
	ar, err := archive.NewReader(trw.ext, r)
	if err != nil {
		return 0, 0, err
	}
	c := &rcbCtx{parent: trw, tw: nil, extractor: extractor, shardName: lom.ObjName, toDisk: toDisk, fromTar: true}
	buf, slab := core.T.PageMM().AllocSize(lom.Lsize())
	c.buf = buf

	err = ar.ReadUntil(c, cos.EmptyMatchAll, "")

	slab.Free(buf)
	return c.extractedSize, c.extractedCount, err
}

// Note that the order of closing must be trw, gzw, then finally tarball.
func (*tarRW) Create(s *Shard, tarball io.Writer, loader ContentLoader) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		tw        = tar.NewWriter(tarball)
		rdReader  = newTarRecordDataReader()
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
				diff := cos.CeilAlignI64(n, archive.TarBlockSize) - n
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

// mostly follows `tar.formatPAXRecord`
func estimateXHeaderSize(paxRecords map[string]string) int64 {
	const padding = 3 // Extra padding for ' ', '=', and '\n'
	totalSize := 0
	for k, v := range paxRecords {
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
