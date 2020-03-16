// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"os"

	"github.com/NVIDIA/aistore/cmn"
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
		file *os.File
		tmp  = path + ".tmp." + cmn.GenTie()
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
	if err = Encode(file, v, opts); err != nil {
		return
	}
	if err = file.Close(); err != nil {
		return
	}
	err = os.Rename(tmp, path)
	return
}

func Load(path string, v interface{}, opts Options) error {
	var (
		file, err = os.Open(path)
	)
	if err != nil {
		return err
	}
	defer file.Close()
	return Decode(file, v, opts, path)
}
