// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"errors"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

const (
	signature = "aistore" // file signature
	//                              0 ---------------- 63  64 ------ 95 | 96 ------ 127
	prefLen = 2 * cos.SizeofI64 // [ signature | jsp ver | meta version |   bit flags  ]
)

// current JSP version
const (
	Metaver = 3
)

//////////////////
// main methods //
//////////////////

func SaveMeta(filepath string, meta Opts, wto io.WriterTo) error {
	return Save(filepath, meta, meta.JspOpts(), wto)
}

func Save(filepath string, v any, opts Options, wto io.WriterTo) (err error) {
	var (
		file *os.File
		tmp  = filepath + ".tmp." + cos.GenTie()
	)
	if file, err = cos.CreateFile(tmp); err != nil {
		return
	}
	defer func() {
		if err == nil {
			return
		}
		if nestedErr := cos.RemoveFile(tmp); nestedErr != nil {
			nlog.Errorf("Nested (%v): failed to remove %s, err: %v", err, tmp, nestedErr)
		}
	}()
	if wto != nil {
		_, err = wto.WriteTo(file)
	} else {
		debug.Assert(v != nil)
		err = Encode(file, v, opts)
	}
	if err != nil {
		nlog.Errorf("Failed to encode %s: %v", filepath, err)
		cos.Close(file)
		return
	}
	if err = cos.FlushClose(file); err != nil {
		nlog.Errorf("Failed to flush and close %s: %v", tmp, err)
		return
	}
	err = os.Rename(tmp, filepath)
	return
}

func LoadMeta(filepath string, meta Opts) (*cos.Cksum, error) {
	return Load(filepath, meta, meta.JspOpts())
}

func Load(filepath string, v any, opts Options) (checksum *cos.Cksum, err error) {
	var file *os.File
	file, err = os.Open(filepath)
	if err != nil {
		return
	}
	checksum, err = Decode(file, v, opts, filepath)
	if err == nil {
		return
	}
	if errors.Is(err, &cos.ErrBadCksum{}) {
		if errRm := os.Remove(filepath); errRm == nil {
			cos.Errorf("jsp: %v, removed %q", err, filepath)
		} else {
			cos.Errorf("jsp: %v, failed to remove %q: %v", err, filepath, errRm)
		}
	}
	return
}
