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
	signature = "aistore" // file signature
	//                              0 ---------------- 63  64 ------ 95 | 96 ------ 127
	prefLen = 2 * cmn.SizeofI64 // [ signature | jsp ver | meta version |   bit flags  ]
)

//////////////////
// main methods //
//////////////////

func SaveMeta(filepath string, meta cmn.GetJopts) error {
	return Save(filepath, meta, meta.GetJopts())
}

func Save(filepath string, v interface{}, opts cmn.Jopts) (err error) {
	var (
		file *os.File
		tmp  = filepath + ".tmp." + cmn.GenTie()
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
	err = os.Rename(tmp, filepath)
	return
}

func LoadMeta(filepath string, meta cmn.GetJopts) (*cmn.Cksum, error) {
	return Load(filepath, meta, meta.GetJopts())
}

func Load(filepath string, v interface{}, opts cmn.Jopts) (checksum *cmn.Cksum, err error) {
	var file *os.File
	file, err = os.Open(filepath)
	if err != nil {
		return
	}
	checksum, err = Decode(file, v, opts, filepath)
	if err != nil && errors.Is(err, &cmn.ErrBadCksum{}) {
		if errRm := os.Remove(filepath); errRm == nil {
			if flag.Parsed() {
				glog.Errorf("bad checksum: removing %s", filepath)
			}
		} else if flag.Parsed() {
			glog.Errorf("bad checksum: failed to remove %s: %v", filepath, errRm)
		}
		return
	}
	return
}
