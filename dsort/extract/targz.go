// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"archive/tar"
	"compress/gzip"
	"io"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

var (
	_ ExtractCreator = &targzExtractCreator{}
)

type targzExtractCreator struct{}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *targzExtractCreator) ExtractShard(shardName string, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size   int64
		header *tar.Header
	)

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return 0, 0, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	var slabSize int64 = memsys.MaxSlabSize
	if r.Size() < cmn.MiB {
		slabSize = 128 * cmn.KiB
	}

	slab, err := mem.GetSlab2(slabSize)
	cmn.AssertNoErr(err)
	buf := slab.Alloc()
	defer slab.Free(buf)

	for {
		header, err = tr.Next()
		if err == io.EOF {
			return extractedSize, extractedCount, nil
		} else if err != nil {
			return extractedSize, extractedCount, err
		}

		metadata := newTarFileHeader(header)
		bmeta, err := jsoniter.Marshal(metadata)
		if err != nil {
			return extractedSize, extractedCount, err
		}

		if header.Typeflag == tar.TypeDir {
			// We can safely ignore this case because we do `MkdirAll` anyway
			// when we create files. And since dirs can appear after all the files
			// we must have this `MkdirAll` before files.
			continue
		} else if header.Typeflag == tar.TypeReg {
			data := cmn.NewSizedReader(tr, header.Size)
			if size, err = extractor.ExtractRecordWithBuffer(shardName, header.Name, data, bmeta, toDisk, 0, buf); err != nil {
				return extractedSize, extractedCount, err
			}
		} else {
			glog.Warningf("Unrecognized header typeflag in tar: %s", string(header.Typeflag))
			continue
		}

		extractedSize += size
		extractedCount++
	}
}

func NewTargzExtractCreator() ExtractCreator {
	return &targzExtractCreator{}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *targzExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var (
		n        int64
		gzw      = gzip.NewWriter(tarball)
		tw       = tar.NewWriter(gzw)
		rdReader = newTarRecordDataReader()
	)

	defer func() {
		rdReader.free()
		tw.Close()
		gzw.Close()
	}()

	for _, rec := range s.Records.All() {
		for _, obj := range rec.Objects {
			rdReader.reinit(tw, obj.Size, obj.MetadataSize)
			if n, err = loadContent(rdReader, rec, obj); err != nil {
				return written + n, err
			}

			written += n
		}
	}
	return written, nil
}

func (t *targzExtractCreator) UsingCompression() bool {
	return true
}

func (t *targzExtractCreator) SupportsOffset() bool {
	return false
}

func (t *targzExtractCreator) MetadataSize() int64 {
	return tarBlockSize // size of tar header with padding
}
