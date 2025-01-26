// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"io"
	"math"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type rocCb struct {
	roc           cos.ROCS
	cb            func(int, error)
	readBytes     int // bytes read since last `Open`.
	reportedBytes int // vs reopen
}

// interface guard
var (
	_ cos.ReadOpenCloser = (*rocCb)(nil)
	_ io.Seeker          = (*rocCb)(nil)
)

///////////
// rocCb //
///////////

func newRocCb(roc cos.ROCS, readCb func(int, error), reportedBytes int) *rocCb {
	return &rocCb{
		roc:           roc,
		cb:            readCb,
		reportedBytes: reportedBytes,
	}
}

func (r *rocCb) Read(p []byte) (n int, err error) {
	n, err = r.roc.Read(p)
	debug.Assert(r.readBytes < math.MaxInt-n)
	r.readBytes += n
	if delta := r.readBytes - r.reportedBytes; delta > 0 {
		r.cb(delta, err)
		r.reportedBytes += delta
	}
	return n, err
}

func (r *rocCb) Open() (cos.ReadOpenCloser, error) {
	roc2, err := r.roc.OpenDup()
	if err != nil {
		return nil, err
	}
	return newRocCb(roc2, r.cb, r.reportedBytes), nil
}

func (r *rocCb) Close() error { return r.roc.Close() }

func (r *rocCb) Seek(offset int64, whence int) (int64, error) {
	return r.roc.Seek(offset, whence)
}
