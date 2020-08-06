// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	v1 = 1 + iota
	v2
	vlatest = v2
)

const (
	signature = "aistore" // file signature
	//                                            0 -------------- 63   64 --------------- 127
	prefLen = 2 * cmn.SizeofI64 // 128bit prefix [ signature, version | flags and packing info ]
)

type Options struct {
	Compression bool // lz4 when [version == 1 || version == 2]
	Checksum    bool // xxhash when [version == 1 || version == 2]
	Signature   bool // when true, write 128bit prefix (of the layout shown above) at offset zero

	Indent bool // Determines if the JSON should be indented. Useful for CLI config.
}

func Plain() Options { return Options{} }
func CCSign() Options {
	return Options{Compression: true, Checksum: true, Signature: true, Indent: false}
}

//////////////////
// main methods //
//////////////////

func Save(path string, v interface{}, opts Options) (err error) {
	var (
		file *os.File
		tmp  = path + ".tmp." + cmn.GenTie()
	)
	if file, err = cmn.CreateFile(tmp); err != nil {
		return
	}
	defer func() {
		if err != nil {
			debug.AssertNoErr(os.Remove(tmp))
		}
	}()
	if err = Encode(file, v, opts); err != nil {
		debug.AssertNoErr(file.Close())
		return
	}
	if err = file.Close(); err != nil {
		return
	}
	err = os.Rename(tmp, path)
	return
}

func Load(path string, v interface{}, opts Options) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return Decode(file, v, opts, path)
}
