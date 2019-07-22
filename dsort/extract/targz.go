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
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	_ ExtractCreator = &targzExtractCreator{}
)

type targzExtractCreator struct{}

// ExtractShard reads the tarball f and extracts its metadata.
func (t *targzExtractCreator) ExtractShard(fqn fs.ParsedFQN, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (extractedSize int64, extractedCount int, err error) {
	var (
		size   int64
		header *tar.Header

		// tarFQN
		workFQN = fs.CSM.GenContentParsedFQN(fqn, filetype.DSortFileType, "")
	)

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return 0, 0, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	// extract to .tar
	f, err := cmn.CreateFile(workFQN)
	if err != nil {
		return 0, 0, err
	}
	tw := tar.NewWriter(f)
	defer func() {
		tw.Close()
		f.Close()
	}()

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

		metadata := newTarFileHeader(header)
		bmeta := cmn.MustMarshal(metadata)

		if err := tw.WriteHeader(header); err != nil {
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

			var extractMethod cmn.Bits = ExtractToMem
			if toDisk {
				extractMethod = ExtractToDisk
			}
			extractMethod.Set(ExtractToWriter)

			args := extractRecordArgs{
				shardName:     fqn.Objname,
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

func NewTargzExtractCreator() ExtractCreator {
	return &targzExtractCreator{}
}

// CreateShard creates a new shard locally based on the Shard.
// Note that the order of closing must be trw, gzw, then finally tarball.
func (t *targzExtractCreator) CreateShard(s *Shard, tarball io.Writer, loadContent LoadContentFunc) (written int64, err error) {
	var (
		n         int64
		needFlush bool
		padBuf    = make([]byte, tarBlockSize)
		gzw, _    = gzip.NewWriterLevel(tarball, gzip.BestSpeed)
		tw        = tar.NewWriter(gzw)
		rdReader  = newTarRecordDataReader()
	)

	defer func() {
		rdReader.free()
		tw.Close()
		gzw.Close()
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
				diff := paddedSize(n) - n
				if diff > 0 {
					if _, err = gzw.Write(padBuf[:diff]); err != nil {
						return written + n, err
					}
					n += diff
				}
				cmn.Dassert(diff >= 0 && diff < 512, pkgName)
			case SGLStoreType, DiskStoreType:
				rdReader.reinit(tw, obj.Size, obj.MetadataSize)
				if n, err = loadContent(rdReader, rec, obj); err != nil {
					return written + n, err
				}
				written += n

				needFlush = true
			default:
				cmn.AssertMsg(false, obj.StoreType)
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
	return true
}

func (t *targzExtractCreator) MetadataSize() int64 {
	return tarBlockSize // size of tar header with padding
}
