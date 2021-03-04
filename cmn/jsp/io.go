// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"io/ioutil"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pierrec/lz4/v3"
)

const (
	sizeXXHash64  = cmn.SizeofI64
	lz4BufferSize = 64 << 10
)

func EncodeSGL(v interface{}, opts cmn.Jopts) *memsys.SGL {
	// NOTE: `32 * cmn.KiB` value was estimated by deploying cluster with
	//  32 targets and 32 proxies and creating 100 buckets.
	sgl := memsys.DefaultPageMM().NewSGL(32 * cmn.KiB)
	err := Encode(sgl, v, opts)
	cmn.AssertNoErr(err)
	return sgl
}

func Encode(ws cmn.WriterAt, v interface{}, opts cmn.Jopts) (err error) {
	var (
		h       hash.Hash
		w       io.Writer = ws
		encoder *jsoniter.Encoder
		off     int
	)
	debug.Assert(opts.Signature || opts.Metaver == 0)
	if opts.Signature {
		var (
			prefix [prefLen]byte
			flags  uint32
		)
		copy(prefix[:], signature) // [ 0 - 63 ]
		l := len(signature)
		debug.Assert(l < cmn.SizeofI64)
		prefix[l] = cmn.MetaverJSP // current jsp version
		off += cmn.SizeofI64

		binary.BigEndian.PutUint32(prefix[off:], opts.Metaver) // [ 64 - 95 ]
		off += cmn.SizeofI32

		if opts.Compress { // [ 96 - 127 ]
			flags |= 1 << 0
		}
		if opts.Checksum {
			flags |= 1 << 1
		}
		binary.BigEndian.PutUint32(prefix[off:], flags)
		off += cmn.SizeofI32

		w.Write(prefix[:])
		debug.Assert(off == prefLen)
	}
	if opts.Checksum {
		var cksum [sizeXXHash64]byte
		w.Write(cksum[:]) // reserve for checksum
	}
	if opts.Compress {
		zw := lz4.NewWriter(w)
		zw.BlockMaxSize = lz4BufferSize
		w = zw
		defer zw.Close()
	}
	if opts.Checksum {
		h = xxhash.New64()
		cmn.Assert(h.Size() == sizeXXHash64)
		w = io.MultiWriter(h, w)
	}
	if opts.Local {
		encoder = cmn.JSONLocal.NewEncoder(w)
	} else {
		encoder = cmn.JSON.NewEncoder(w)
	}
	if opts.Indent {
		encoder.SetIndent("", "  ")
	}
	if err = encoder.Encode(v); err != nil {
		return
	}
	if opts.Checksum {
		if _, err := ws.WriteAt(h.Sum(nil), int64(off)); err != nil {
			return err
		}
	}
	return
}

func Decode(reader io.ReadCloser, v interface{}, opts cmn.Jopts, tag string) (checksum *cmn.Cksum, err error) {
	var (
		r             io.Reader = reader
		expectedCksum uint64
		h             hash.Hash
		jspVer        byte
	)
	defer cmn.Close(reader)
	if opts.Signature {
		var (
			prefix  [prefLen]byte
			metaVer uint32
		)
		if _, err = r.Read(prefix[:]); err != nil {
			return
		}
		l := len(signature)
		debug.Assert(l < cmn.SizeofI64)
		if signature != string(prefix[:l]) {
			err = &ErrBadSignature{tag, string(prefix[:l]), signature}
			return
		}
		jspVer = prefix[l]
		if jspVer != cmn.MetaverJSP {
			err = newErrVersion("jsp", uint32(jspVer), cmn.MetaverJSP, 2)
			// NOTE: start jsp backward compatibility
			if _, ok := err.(*ErrCompatibleVersion); ok {
				glog.Errorf("%v - skipping meta-version check", err)
				err = nil
				goto skip
			}
			// NOTE: end jsp backward compatibility
			return
		}
		metaVer = binary.BigEndian.Uint32(prefix[cmn.SizeofI64:])
		if metaVer != opts.Metaver {
			err = newErrVersion(tag, metaVer, opts.Metaver)
			return
		}
	skip:
		flags := binary.BigEndian.Uint32(prefix[cmn.SizeofI64+cmn.SizeofI32:])
		opts.Compress = flags&(1<<0) != 0
		opts.Checksum = flags&(1<<1) != 0
	}
	if opts.Checksum {
		var cksum [sizeXXHash64]byte
		if _, err = r.Read(cksum[:]); err != nil {
			return
		}
		expectedCksum = binary.BigEndian.Uint64(cksum[:])
	}
	if opts.Compress {
		zr := lz4.NewReader(r)
		zr.BlockMaxSize = lz4BufferSize
		r = zr
	}
	if opts.Checksum {
		h = xxhash.New64()
		r = io.TeeReader(r, h)
	}
	if err = cmn.JSON.NewDecoder(r).Decode(v); err != nil {
		return
	}
	if opts.Checksum {
		// We have already parsed `v` but there is still the possibility that `\n` remains
		// not read. Therefore, we read it to include it into the final checksum.
		var b []byte
		if b, err = ioutil.ReadAll(r); err != nil {
			return
		}
		// To be sure that this is exactly the case...
		debug.Assert(len(b) == 0 || (len(b) == 1 && b[0] == '\n'), b)

		actual := h.Sum(nil)
		actualCksum := binary.BigEndian.Uint64(actual)
		if expectedCksum != actualCksum {
			err = cmn.NewBadMetaCksumError(expectedCksum, actualCksum, tag)
			return
		}
		checksum = cmn.NewCksum(cmn.ChecksumXXHash, hex.EncodeToString(actual))
	}
	return
}
