// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// write remote ranges into manifest chunks: parallel cold GET (ais) and blob download (xact/xs)
// - one instance per goroutine; reuses its hash state across chunks
// - per-chunk CRC32C (when checksumming) to combine at completion without rereading

type ChunkWriter struct {
	lom      *LOM
	manifest *Ufest
	fh       *os.File
	cksum    cos.CksumHash
	checksum bool
	fsync    bool
}

func (cw *ChunkWriter) Init(lom *LOM, manifest *Ufest) {
	cw.lom, cw.manifest = lom, manifest
	cw.checksum = lom.CksumType() != cos.ChecksumNone
	cw.fsync = lom.IsFeatureSet(feat.FsyncPUT)
	if cw.checksum {
		cw.cksum.Init(cos.ChecksumCRC32C)
	}
}

// copy reader into chunk `part` and add it to the manifest
// - size > 0: require exactly that many bytes
// - extra writers (e.g., SGL for streaming) receive the same bytes
// - on error, remove the chunk file
func (cw *ChunkWriter) WriteChunk(part int, reader io.Reader, size int64, buf []byte, extra ...io.Writer) (int64, error) {
	chunk, err := cw.manifest.NewChunk(part, cw.lom)
	if err != nil {
		return 0, err
	}
	cw.fh, err = cw.lom.CreatePart(chunk.Path())
	if err != nil {
		return 0, err
	}
	var writer io.Writer = cw.fh
	if cw.checksum {
		cw.cksum.H.Reset()
		writer = cw
	}
	if len(extra) > 0 {
		writer = cos.NewWriterMulti(append([]io.Writer{writer}, extra...)...)
	}
	written, err := cos.CopyBuffer(writer, reader, buf)
	if err == nil && size > 0 && written != size {
		err = fmt.Errorf("%s: chunk %d: expected %d bytes, read %d", cw.lom.Cname(), part, size, written)
	}
	if err == nil && cw.fsync {
		err = cw.fh.Sync()
	}
	if closeErr := cw.fh.Close(); err == nil {
		err = closeErr
	}
	cw.fh = nil
	if err == nil {
		if cw.checksum {
			cw.cksum.Finalize()
			chunk.SetCksum(cos.NewCksum(cos.ChecksumCRC32C, cw.cksum.Value()))
		}
		err = cw.manifest.Add(chunk, written, int64(part))
	}
	if err != nil {
		if nerr := cos.RemoveFile(chunk.Path()); nerr != nil {
			nlog.Errorln("nested error removing chunk:", nerr)
		}
		return 0, err
	}
	return written, nil
}

// file and checksum in one pass
func (cw *ChunkWriter) Write(p []byte) (int, error) {
	n, err := cw.fh.Write(p)
	if n > 0 {
		cw.cksum.H.Write(p[:n])
	}
	return n, err
}
