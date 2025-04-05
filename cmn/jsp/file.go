// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

import (
	"errors"
	"fmt"
	"os"
	"strings"

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

func SaveMeta(filepath string, meta Opts, wto cos.WriterTo2) error {
	return Save(filepath, meta, meta.JspOpts(), wto)
}

func Save(filepath string, v any, opts Options, wto cos.WriterTo2) (err error) {
	var (
		file *os.File
		tmp  = filepath + ".tmp." + cos.GenTie()
	)
	if file, err = cos.CreateFile(tmp); err != nil {
		return err
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
		err = wto.WriteTo2(file)
	} else {
		debug.Assert(v != nil)
		err = Encode(file, v, opts)
	}
	if err != nil {
		nlog.Errorf("Failed to encode %s: %v", filepath, err)
		cos.Close(file)
		return err
	}
	if err = cos.FlushClose(file); err != nil {
		nlog.Errorf("Failed to flush and close %s: %v", tmp, err)
		return err
	}
	err = os.Rename(tmp, filepath)
	return err
}

func LoadMeta(filepath string, meta Opts) (*cos.Cksum, error) {
	return Load(filepath, meta, meta.JspOpts())
}

func _tag(filepath string, v any) (tag string) {
	tag = fmt.Sprintf("%T", v)
	if i := strings.LastIndex(tag, "."); i > 0 {
		tag = tag[i+1:]
	}
	return tag + " at " + filepath
}

func Load(filepath string, v any, opts Options) (checksum *cos.Cksum, err error) {
	var fh *os.File
	fh, err = os.Open(filepath)
	if err != nil {
		return
	}
	checksum, err = Decode(fh, v, opts, _tag(filepath, v))
	cos.Close(fh)
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
