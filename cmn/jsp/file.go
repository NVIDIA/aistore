// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"errors"
	"flag"
	"os"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	v2 = 2 // supported version
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
	Local  bool // when true, use JSON local extension
}

func Plain() Options      { return Options{} }
func PlainLocal() Options { return Options{Local: true} }
func CCSignLocal() Options {
	opts := CCSign()
	opts.Local = true
	return opts
}

func CCSign() Options {
	return Options{Compression: true, Checksum: true, Signature: true, Indent: false}
}

func CksumSign() Options {
	return Options{Checksum: true, Signature: true}
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
			errRm := os.Remove(tmp)
			debug.AssertNoErr(errRm)
		}
	}()
	if err = Encode(file, v, opts); err != nil {
		cmn.Close(file)
		return
	}
	if err = file.Close(); err != nil {
		return
	}
	err = os.Rename(tmp, path)
	return
}

func Load(path string, v interface{}, opts Options) (checksum *cmn.Cksum, err error) {
	var file *os.File
	file, err = os.Open(path)
	if err != nil {
		return
	}
	checksum, err = Decode(file, v, opts, path)
	if err != nil && errors.Is(err, &cmn.ErrBadCksum{}) {
		if errRm := os.Remove(path); errRm == nil {
			if flag.Parsed() {
				glog.Errorf("bad checksum: removing %s", path)
			}
		} else if flag.Parsed() {
			glog.Errorf("bad checksum: failed to remove %s: %v", path, errRm)
		}
		return
	}
	return
}
