// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pierrec/lz4/v3"
)

func EncodeBuf(v interface{}, opts Options) []byte {
	buf := &bytes.Buffer{}
	err := Encode(buf, v, opts)
	cmn.AssertNoErr(err)
	return buf.Bytes()
}

func Encode(writer io.Writer, v interface{}, opts Options) (err error) {
	var (
		zw      *lz4.Writer
		encoder *jsoniter.Encoder
		h       hash.Hash
		mw      io.Writer
		prefix  [prefLen]byte
		buf     = &bytes.Buffer{}
	)
	if opts.Checksum {
		h = xxhash.New64()
	}

	if opts.Compression {
		if opts.Checksum {
			mw = io.MultiWriter(h, buf)
			zw = lz4.NewWriter(mw)
		} else {
			zw = lz4.NewWriter(buf)
		}
		encoder = jsoniter.NewEncoder(zw)
	} else {
		if opts.Checksum {
			mw = io.MultiWriter(h, buf)
			encoder = jsoniter.NewEncoder(mw)
		} else {
			encoder = jsoniter.NewEncoder(buf)
		}
	}
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(v); err != nil {
		return
	}
	if opts.Compression {
		_ = zw.Close()
	}
	if opts.Signature {
		// 1st 64-bit word
		copy(prefix[:], signature)
		l := len(signature)
		cmn.Assert(l < prefLen/2)
		prefix[l] = version

		// 2nd 64-bit word as of version == 1
		var packingInfo uint64
		if opts.Compression {
			packingInfo |= 1 << 0
		}
		if opts.Checksum {
			packingInfo |= 1 << 1
		}
		binary.BigEndian.PutUint64(prefix[cmn.SizeofI64:], packingInfo)

		// write prefix
		writer.Write(prefix[:])
	}
	if opts.Checksum {
		if _, err = writer.Write(h.Sum(nil)); err != nil {
			return
		}
	}
	if _, err = writer.Write(buf.Bytes()); err != nil {
		return
	}
	return
}

func Decode(reader io.ReadCloser, v interface{}, opts Options, tag string) error {
	var (
		decoder *jsoniter.Decoder
		zr      *lz4.Reader
		h       hash.Hash
		hsum    [8]byte
		prefix  [prefLen]byte
		hsize   int
		err     error
	)
	defer reader.Close()
	if opts.Signature {
		if _, err = reader.Read(prefix[:]); err != nil {
			return err
		}
		l := len(signature)
		if signature != string(prefix[:l]) {
			return fmt.Errorf("bad signature %q: %v", tag, prefix[:l])
		}
		if version != prefix[l] {
			return fmt.Errorf("unsupported version %q: %v", tag, prefix[l])
		}
		packingInfo := binary.BigEndian.Uint64(prefix[cmn.SizeofI64:])
		opts.Compression = packingInfo&(1<<0) != 0
		opts.Checksum = packingInfo&(1<<1) != 0
	}
	var b io.Reader
	if opts.Checksum {
		var n int
		buf := &bytes.Buffer{}
		h = xxhash.New64()
		hsize = h.Size()
		cmn.Assert(hsize <= len(hsum))
		if n, err = reader.Read(hsum[:hsize]); err == nil && n != hsize {
			err = fmt.Errorf("failed reading checksum %q (%d, %d)", tag, n, h.Size())
		}
		if err != nil {
			return err
		}
		if _, err = cmn.ReceiveAndChecksum(buf, reader, nil, h); err != nil {
			return err
		}
		expected, actual := binary.BigEndian.Uint64(hsum[:]), binary.BigEndian.Uint64(h.Sum(nil))
		if expected != actual {
			return cmn.NewBadMetaCksumError(expected, actual, tag)
		}
		b = bytes.NewReader(buf.Bytes())
	} else {
		b = reader
	}
	if opts.Compression {
		zr = lz4.NewReader(b)
		decoder = jsoniter.NewDecoder(zr)
	} else {
		decoder = jsoniter.NewDecoder(b)
	}
	return decoder.Decode(v)
}
