// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"math"
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/urfave/cli"
)

const (
	stdInOut = "-" // STDIN (for `ais put`), STDOUT (for `ais put`)
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

//
// handle destination path
//

func discardOutput(outf string) bool {
	return outf == "/dev/null" || outf == "dev/null" || outf == "dev/nil"
}

// notice returned "variability":
// - (error | errUserCancel)
// - (io.Discard | os.Stdout | wfh)
func createDstFile(c *cli.Context, outf string, allowStdout bool) (w io.Writer, wfh *os.File, err error) {
	discard := discardOutput(outf)
	switch {
	case discard:
		return io.Discard, nil, nil
	case outf == stdInOut:
		if !allowStdout {
			return nil, nil, incorrectUsageMsg(c, "destination STDOUT is not permitted for this operation")
		}
		return os.Stdout, nil, nil
	}

	if finfo, err := os.Stat(outf); err == nil {
		if finfo.IsDir() {
			return nil, nil, fmt.Errorf("destination %q is a directory", outf)
		}
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("destination %q already exists", outf)
			if !confirm(c, "Proceed to overwrite?", warn) {
				return nil, nil, errUserCancel
			}
		}
	}
	wfh, err = cos.CreateFile(outf)
	if err != nil {
		return nil, nil, err
	}
	w = wfh

	return w, wfh, nil
}
