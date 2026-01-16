// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	onexxh "github.com/OneOfOne/xxhash"
	"github.com/pierrec/lz4/v4"
)

const (
	lz4BufferSize = lz4.Block64Kb
)

func Encode(ws cos.WriterAt, v any, opts Options) error {
	var (
		zw  *lz4.Writer
		h   hash.Hash
		w   io.Writer = ws
		off int
	)
	//
	// 1. header
	//
	if opts.Signature {
		var (
			prefix [prefLen]byte
			flags  uint32
		)
		copy(prefix[:], signature) // [ 0 - 63 ]
		l := len(signature)
		debug.Assert(l < cos.SizeofI64)
		prefix[l] = Metaver // current jsp version
		off += cos.SizeofI64

		binary.BigEndian.PutUint32(prefix[off:], opts.Metaver) // [ 64 - 95 ]
		off += cos.SizeofI32

		if opts.Compress { // [ 96 - 127 ]
			flags |= 1 << 0
		}
		if opts.Checksum {
			flags |= 1 << 1
		}
		binary.BigEndian.PutUint32(prefix[off:], flags)
		off += cos.SizeofI32

		if _, err := w.Write(prefix[:]); err != nil {
			return err
		}
		debug.Assert(off == prefLen)
	}
	if opts.Checksum {
		var cksum [cos.SizeXXHash64]byte
		if _, err := w.Write(cksum[:]); err != nil { // reserve for checksum
			return err
		}
	}
	if opts.Compress {
		zw = lz4.NewWriter(w)
		errN := zw.Apply(lz4.BlockSizeOption(lz4BufferSize))
		debug.AssertNoErr(errN)
		w = zw
	}
	if opts.Checksum {
		h = onexxh.New64()
		cos.Assert(h.Size() == cos.SizeXXHash64)
		w = io.MultiWriter(h, w)
	}

	//
	// 2. data
	//
	var (
		encoder      = cos.JSON.NewEncoder(w)
		errEn, errCl error
	)
	if opts.Indent {
		encoder.SetIndent("", "  ")
	}
	errEn = encoder.Encode(v)
	if zw != nil {
		errCl = zw.Close()
	}
	if errEn != nil {
		return errEn
	}
	if errCl != nil {
		return errCl
	}

	//
	// 3. checksum
	//
	if opts.Checksum {
		if _, err := ws.WriteAt(h.Sum(nil), int64(off)); err != nil {
			return err
		}
	}
	return nil
}

func Decode(r io.Reader, v any, opts Options, tag string) (*cos.Cksum, error) {
	if opts.Signature {
		var (
			prefix  [prefLen]byte
			metaVer uint32
		)
		if _, err := r.Read(prefix[:]); err != nil {
			return nil, err
		}
		l := len(signature)
		debug.Assert(l < cos.SizeofI64)
		if signature != string(prefix[:l]) {
			return nil, &ErrBadSignature{tag, string(prefix[:l]), signature}
		}
		jspVer := prefix[l]
		if jspVer != Metaver {
			return nil, newErrVersion("jsp", uint32(jspVer), Metaver)
		}
		metaVer = binary.BigEndian.Uint32(prefix[cos.SizeofI64:])
		if metaVer != opts.Metaver {
			if opts.OldMetaverOk == 0 || metaVer > opts.Metaver || metaVer < opts.OldMetaverOk {
				// _not_ backward compatible
				return nil, newErrVersion(tag, metaVer, opts.Metaver)
			}
			// backward compatible
			erw := newErrVersion(tag, metaVer, opts.Metaver, opts.OldMetaverOk)
			nlog.Warningln(erw)
		}
		flags := binary.BigEndian.Uint32(prefix[cos.SizeofI64+cos.SizeofI32:])
		opts.Compress = flags&(1<<0) != 0
		opts.Checksum = flags&(1<<1) != 0
	}

	if opts.Checksum {
		return _decksum(r, v, opts, tag)
	}
	if opts.Compress {
		r = lz4.NewReader(r)
	}
	if err := cos.JSON.NewDecoder(r).Decode(v); err != nil {
		return nil, err
	}

	return nil, nil
}

func _decksum(r io.Reader, v any, opts Options, tag string) (*cos.Cksum, error) {
	var cksum [cos.SizeXXHash64]byte
	if _, err := r.Read(cksum[:]); err != nil {
		return nil, err
	}

	expectedCksum := binary.BigEndian.Uint64(cksum[:])
	if opts.Compress {
		r = lz4.NewReader(r)
	}

	var (
		h  = onexxh.New64()
		rr = io.TeeReader(r, h)
	)
	if err := cos.JSON.NewDecoder(rr).Decode(v); err != nil {
		return nil, err
	}

	// We have already parsed `v` but there is still the possibility that `\n` remains
	// not read. Read it to include into the final checksum.
	b, err := cos.ReadAllN(rr, cos.ContentLengthUnknown)
	if err != nil {
		return nil, err
	}
	debug.Assert(len(b) == 0 || (len(b) == 1 && b[0] == '\n'), b)

	actual := h.Sum(nil)
	actualCksum := binary.BigEndian.Uint64(actual)
	if expectedCksum != actualCksum {
		return nil, cos.NewErrMetaCksum(expectedCksum, actualCksum, tag)
	}

	return cos.NewCksum(cos.ChecksumOneXxh, hex.EncodeToString(actual)), nil
}
