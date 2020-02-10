// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures to/from disk with optional checksumming and compression.
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
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pierrec/lz4/v3"
)

const (
	signature = "aistore" // file signature
	version   = 1         // version of *this* packing format
	//                                            0 -------------- 63   64 --------------- 127
	prefLen = 2 * cmn.SizeofI64 // 128bit prefix [ signature, version | flags and packing info ]
)

type Options struct {
	Compression bool // lz4 when version == 1
	Checksum    bool // xxhash when version == 1
	Signature   bool // when true, write 128bit prefix (of the layout shown above) at offset zero
}

//
// version 1 shortcuts to specify the most commonly used options
//
func Plain() Options  { return Options{} }                 // plain text
func CCSign() Options { return Options{true, true, true} } // compress, checksum, and sign

//////////////////
// main methods //
//////////////////

func Save(path string, v interface{}, opts Options) (err error) {
	var (
		zw      *lz4.Writer
		encoder *jsoniter.Encoder
		h       hash.Hash
		mw      io.Writer
		prefix  [prefLen]byte
		off, l  int
		file    *os.File
		tmp     = path + ".tmp." + cmn.GenTie()
	)
	if file, err = cmn.CreateFile(tmp); err != nil {
		return
	}
	defer func() {
		_ = file.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	if opts.Signature {
		off = prefLen
	}
	if opts.Checksum {
		h = xxhash.New64()
		off += h.Size()
	}
	if off > 0 {
		if _, err = file.Seek(int64(off), io.SeekStart); err != nil {
			return
		}
	}
	if opts.Compression {
		if opts.Checksum {
			mw = io.MultiWriter(h, file)
			zw = lz4.NewWriter(mw)
		} else {
			zw = lz4.NewWriter(file)
		}
		encoder = jsoniter.NewEncoder(zw)
	} else {
		if opts.Checksum {
			mw = io.MultiWriter(h, file)
			encoder = jsoniter.NewEncoder(mw)
		} else {
			encoder = jsoniter.NewEncoder(file)
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
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			return
		}
		// 1st 64-bit word
		copy(prefix[:], signature)
		l = len(signature)
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
		file.Write(prefix[:])
	}
	if opts.Checksum {
		if !opts.Signature {
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				return
			}
		}
		file.Write(h.Sum(nil))
	}
	if err = file.Close(); err != nil {
		return
	}
	err = os.Rename(tmp, path)
	return
}

func Load(path string, v interface{}, opts Options) error {
	var (
		decoder    *jsoniter.Decoder
		zr         *lz4.Reader
		h          hash.Hash
		hsum       [8]byte
		prefix     [prefLen]byte
		hsize, off int
		file, err  = os.Open(path)
	)
	if err != nil {
		return err
	}
	defer file.Close()
	if opts.Signature {
		if _, err = file.Read(prefix[:]); err != nil {
			return err
		}
		l := len(signature)
		if signature != string(prefix[:l]) {
			return fmt.Errorf("bad signature %q: %v", path, prefix[:l])
		}
		if version != prefix[l] {
			return fmt.Errorf("unsupported version %q: %v", path, prefix[l])
		}
		packingInfo := binary.BigEndian.Uint64(prefix[cmn.SizeofI64:])
		opts.Compression = packingInfo&(1<<0) != 0
		opts.Checksum = packingInfo&(1<<1) != 0
		off = prefLen
	}
	if opts.Checksum {
		var n int
		h = xxhash.New64()
		hsize = h.Size()
		cmn.Assert(hsize <= len(hsum))
		if n, err = file.Read(hsum[:hsize]); err == nil && n != hsize {
			err = fmt.Errorf("failed reading checksum %q (%d, %d)", path, n, h.Size())
		}
		if err != nil {
			return err
		}
		if _, err = cmn.ReceiveAndChecksum(ioutil.Discard, file, nil, h); err != nil {
			return err
		}
		expected, actual := binary.BigEndian.Uint64(hsum[:]), binary.BigEndian.Uint64(h.Sum(nil))
		if expected != actual {
			return cmn.NewBadMetaCksumError(expected, actual, path)
		}
		off += hsize
		if _, err = file.Seek(int64(off), io.SeekStart); err != nil {
			return err
		}
	}
	if opts.Compression {
		zr = lz4.NewReader(file)
		decoder = jsoniter.NewDecoder(zr)
	} else {
		decoder = jsoniter.NewDecoder(file)
	}
	return decoder.Decode(v)
}
