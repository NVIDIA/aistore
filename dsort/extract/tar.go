// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/tar"
	"io"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	tarBlockSize = 512 // Size of each block in a tar stream
)

var (
	_ ExtractCreator = &tarExtractCreator{}
)

type tarExtractCreator struct{}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *tarExtractCreator) ExtractShard(fqn fs.ParsedFQN, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size   int64
		tr     = tar.NewReader(r)
		header *tar.Header
	)

	var slabSize int64 = memsys.MaxSlabSize
	if r.Size() < cmn.MiB {
		slabSize = 128 * cmn.KiB
	}

	slab, err := mem.GetSlab2(slabSize)
	cmn.AssertNoErr(err)
	buf := slab.Alloc()
	defer slab.Free(buf)

	offset := int64(0)
	for {
		header, err = tr.Next()
		if err == io.EOF {
			return extractedSize, extractedCount, nil
		} else if err != nil {
			return extractedSize, extractedCount, err
		}

		offset += t.MetadataSize()

		if header.Typeflag == tar.TypeDir {
			// We can safely ignore this case because we do `MkdirAll` anyway
			// when we create files. And since dirs can appear after all the files
			// we must have this `MkdirAll` before files.
			continue
		} else if header.Typeflag == tar.TypeReg {
			data := cmn.NewSizedReader(tr, header.Size)
			if size, err = extractor.ExtractRecordWithBuffer(t, fqn, header.Name, data, nil, toDisk, offset, buf); err != nil {
				return extractedSize, extractedCount, err
			}
		} else {
			glog.Warningf("Unrecognized header typeflag in tar: %s", string(header.Typeflag))
			continue
		}

		extractedSize += size
		extractedCount++

		// .tar format pads all block to 512 bytes
		offset += paddedSize(header.Size)
	}
}

func NewTarExtractCreator() ExtractCreator {
	return &tarExtractCreator{}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *tarExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var (
		n      int64
		padBuf = make([]byte, tarBlockSize)
	)

	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			if n, err = loadContent(tarball, rec, obj); err != nil {
				return written + n, err
			}

			// pad to 512 bytes
			diff := paddedSize(n) - n
			if diff > 0 {
				if _, err = tarball.Write(padBuf[:diff]); err != nil {
					return written + n, err
				}
				n += diff
			}

			written += n
		}
	}
	return written, nil
}

func (t *tarExtractCreator) UsingCompression() bool {
	return false
}

func (t *tarExtractCreator) SupportsOffset() bool {
	return true
}

func (t *tarExtractCreator) MetadataSize() int64 {
	return tarBlockSize // size of tar header with padding
}

// Calculates padded value to 512 bytes
func paddedSize(offset int64) int64 {
	return offset + (-offset & (tarBlockSize - 1))
}
