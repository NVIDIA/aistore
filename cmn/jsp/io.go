// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/ioutil"

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

func EncodeSGL(v interface{}, opts Options) *memsys.SGL {
	// NOTE: `32 * cmn.KiB` value was estimated by deploying cluster with
	//  32 targets and 32 proxies and creating 100 buckets.
	sgl := memsys.DefaultPageMM().NewSGL(32 * cmn.KiB)
	err := Encode(sgl, v, opts)
	cmn.AssertNoErr(err)
	return sgl
}

func Encode(ws cmn.WriterAt, v interface{}, opts Options) (err error) {
	var (
		cksumOffset int64
		h           hash.Hash
		w           io.Writer = ws
	)
	if opts.Signature {
		var prefix [prefLen]byte

		// 1st 64-bit word
		copy(prefix[:], signature)
		l := len(signature)
		cmn.Assert(l < prefLen/2)
		prefix[l] = v2

		// 2nd 64-bit word
		var packingInfo uint64
		if opts.Compression {
			packingInfo |= 1 << 0
		}
		if opts.Checksum {
			packingInfo |= 1 << 1
		}
		binary.BigEndian.PutUint64(prefix[cmn.SizeofI64:], packingInfo)

		w.Write(prefix[:])

		cksumOffset = int64(prefLen)
	}
	if opts.Checksum {
		// Reserve place for a checksum.
		var cksum [sizeXXHash64]byte
		w.Write(cksum[:])
	}
	if opts.Compression {
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

	encoder := jsoniter.NewEncoder(w)
	if opts.Indent {
		encoder.SetIndent("", "  ")
	}
	if err = encoder.Encode(v); err != nil {
		return
	}
	if opts.Checksum {
		if _, err := ws.WriteAt(h.Sum(nil), cksumOffset); err != nil {
			return err
		}
	}
	return
}

func Decode(reader io.ReadCloser, v interface{}, opts Options, tag string) error {
	var (
		r             io.Reader = reader
		expectedCksum uint64
		h             hash.Hash
		version       byte
	)
	defer cmn.Close(reader)
	if opts.Signature {
		var prefix [prefLen]byte
		if _, err := r.Read(prefix[:]); err != nil {
			return err
		}
		l := len(signature)
		if signature != string(prefix[:l]) {
			return fmt.Errorf("bad signature %q: %v", tag, string(prefix[:l]))
		}
		version = prefix[l]
		if version != v2 {
			return fmt.Errorf("unsupported version %q: %x", tag, version)
		}

		packingInfo := binary.BigEndian.Uint64(prefix[cmn.SizeofI64:])
		opts.Compression = packingInfo&(1<<0) != 0
		opts.Checksum = packingInfo&(1<<1) != 0
	}
	if opts.Checksum {
		var cksum [sizeXXHash64]byte
		if _, err := r.Read(cksum[:]); err != nil {
			return err
		}
		expectedCksum = binary.BigEndian.Uint64(cksum[:])
	}
	if opts.Compression {
		zr := lz4.NewReader(r)
		zr.BlockMaxSize = lz4BufferSize
		r = zr
	}
	if opts.Checksum {
		h = xxhash.New64()
		r = io.TeeReader(r, h)
	}
	decoder := jsoniter.NewDecoder(r)
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if opts.Checksum {
		// We have already parsed `v` but there is still the possibility that `\n` remains
		// not read. Therefore, we read it to include it into the final checksum.
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		// To be sure that this is exactly the case...
		debug.Assert(len(b) == 0 || (len(b) == 1 && b[0] == '\n'), b)

		actual := h.Sum(nil)
		actualCksum := binary.BigEndian.Uint64(actual)
		if expectedCksum != actualCksum {
			return cmn.NewBadMetaCksumError(expectedCksum, actualCksum, tag)
		}
	}
	return nil
}
