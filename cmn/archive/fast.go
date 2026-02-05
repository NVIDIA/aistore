// Package archive: write, read, copy, append, list primitives
// across all supported formats
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package archive

import (
	"archive/tar"
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Fast Append -------------------------------------------------------
// Standard library does not support appending to tgz, zip, and such;
// for TAR there is an optimizing workaround not requiring a full copy.
//
// Execution:
// OpensTAR and use its reader's Next() to skip to the position
// right _after_ the last file in the TAR (padding bytes including).
//
// Background:
// TAR file is padded with one or more 512-byte blocks of zero bytes.
// The blocks must be overwritten, otherwise newly added files won't be
// accessible. Different TAR formats (such as `ustar`, `pax` and `GNU`)
// write different number of zero blocks.
// --------------------------------------------------------------------

func OpenTarForAppend(cname, workFQN string) (rwfh *os.File, tarFormat tar.Format, offset int64, err error) {
	if rwfh, err = os.OpenFile(workFQN, os.O_RDWR, cos.PermRWR); err != nil {
		return
	}
	if tarFormat, offset, err = _seekTarEnd(cname, rwfh); err != nil {
		rwfh.Close() // always close on err
	}
	return
}

func _seekTarEnd(cname string, fh *os.File) (tarFormat tar.Format, offset int64, err error) {
	var (
		twr     = tar.NewReader(fh)
		size    int64
		pos     = int64(-1)
		unknown bool
	)
	for {
		var hdr *tar.Header
		hdr, err = twr.Next()
		if err != nil {
			if err != io.EOF {
				return tarFormat, 0, err // invalid TAR format
			}
			// EOF
			if pos < 0 {
				return tarFormat, 0, ErrTarIsEmpty // still ok
			}
			break
		}
		if pos < 0 {
			tarFormat = hdr.Format
		} else if !unknown { // once unknown remains unknown
			if tarFormat != hdr.Format {
				tarFormat = tar.FormatUnknown
				unknown = true
			}
		}
		pos, err = fh.Seek(0, io.SeekCurrent)
		if err != nil {
			debug.AssertNoErr(err) // unlikely
			return tarFormat, -1, err
		}
		size = hdr.Size
	}
	if pos == 0 {
		return tarFormat, 0, fmt.Errorf("failed to seek end of the TAR %s", cname)
	}

	padded := cos.CeilAlignI64(size, TarBlockSize)
	offset, err = fh.Seek(pos+padded, io.SeekStart)
	debug.AssertNoErr(err)
	debug.Assert(offset > 0)

	return tarFormat, offset, err
}
