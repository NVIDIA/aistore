// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"archive/tar"
	"archive/zip"

	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

// interface guard
var _ archive.ArchRCB = (*rcbCtx)(nil)

// `archive.Reader` rcb context and callback; uses `extractor` to extract
type rcbCtx struct {
	extractor      RecordExtractor
	parent         RW
	tw             *tar.Writer
	shardName      string
	buf            []byte
	offset         int64
	extractedSize  int64
	extractedCount int
	toDisk         bool
	fromTar        bool
}

// implements archive.ArchRCB callback
func (c *rcbCtx) Call(_ string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error) {
	if c.fromTar {
		return c.xtar("", reader, hdr)
	}
	return c.xzip("", reader, hdr)
}

// handles .tar, .targz, and .tarlz4 - anything and everything that has tar headers
func (c *rcbCtx) xtar(_ string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error) {
	header, ok := hdr.(*tar.Header)
	debug.Assert(ok)

	bmeta := cos.MustMarshal(header)
	c.offset += c.parent.MetadataSize()
	if header.Format == tar.FormatPAX {
		// When dealing with `tar.FormatPAX` we also need to take into
		// consideration the `tar.TypeXHeader` that comes before the actual header.
		// Together it looks like this: [x-header][pax-records][pax-header][pax-file].
		// Since `tar.Reader` skips over this header and writes to `header.PAXRecords`
		// we need to manually adjust the offset, otherwise when using the
		// offset we will point to totally wrong location.

		// Add offset for `tar.TypeXHeader`.
		c.offset += c.parent.MetadataSize()
		sz := estimateXHeaderSize(header.PAXRecords)
		sz = cos.CeilAlignI64(sz, archive.TarBlockSize)

		// Add offset for size of PAX records - there is no way of knowing
		// the size, so we must estimate it by ourselves...
		c.offset += sz
	}
	args := extractRecordArgs{
		shardName:  c.shardName,
		recordName: header.Name,
		r:          reader,
		metadata:   bmeta,
		offset:     c.offset,
		buf:        c.buf,
	}
	args.extractMethod = ExtractToMem
	if c.toDisk {
		args.extractMethod = ExtractToDisk
	}
	if c.tw == nil {
		// tar (and zip - below)
		args.fileType = fs.ObjCT
	} else {
		// tar.gz and tar.lz4
		if err := c.tw.WriteHeader(header); err != nil {
			return true, err
		}
		args.fileType = fs.DsortFileCT
		args.extractMethod.Set(ExtractToWriter)
		args.w = c.tw
	}

	size, err := c.extractor.RecordWithBuffer(&args)
	reader.Close()
	if err != nil {
		return true /*stop*/, err
	}
	debug.Assert(size > 0)
	c.extractedSize += size
	c.extractedCount++
	c.offset += cos.CeilAlignI64(header.Size, archive.TarBlockSize) // .tar padding
	return false, nil
}

// handles .zip
func (c *rcbCtx) xzip(_ string, reader cos.ReadCloseSizer, hdr any) (bool /*stop*/, error) {
	header, ok := hdr.(*zip.FileHeader)
	debug.Assert(ok)

	metadata := zipFileHeader{
		Name:    header.Name,
		Comment: header.Comment,
	}
	bmeta := cos.MustMarshal(metadata)
	args := extractRecordArgs{
		shardName:  c.shardName,
		recordName: header.Name,
		r:          reader,
		metadata:   bmeta,
		buf:        c.buf,
	}
	args.extractMethod = ExtractToMem
	if c.toDisk {
		args.extractMethod = ExtractToDisk
	}
	args.fileType = fs.ObjCT

	size, err := c.extractor.RecordWithBuffer(&args)
	if err == nil {
		c.extractedSize += size
		c.extractedCount++
	}
	reader.Close()
	return err != nil /*stop*/, err
}

// common method to extract compressed tar using `ar` (archive reader)
func (c *rcbCtx) extract(lom *core.LOM, ar archive.Reader) error {
	workFQN := lom.GenFQN(fs.DsortFileCT) // tarFQN
	wfh, err := cos.CreateFile(workFQN)
	if err != nil {
		return err
	}

	c.tw = tar.NewWriter(wfh)
	buf, slab := core.T.PageMM().AllocSize(lom.Lsize())
	c.buf = buf

	err = ar.ReadUntil(c, cos.EmptyMatchAll, "")
	slab.Free(buf)
	if err == nil {
		cos.Close(c.tw)
	} else {
		_ = c.tw.Close()
	}
	cos.Close(wfh)
	return err
}
