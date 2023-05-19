// Package archive
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

// OpenTarForAppend opens a TAR and uses tar's reader Next() to skip
// to the position right _after_ the last file in the TAR
// (padding bytes including). Background:
//
// TAR file is padded with one or more 512-byte blocks of zero bytes.
// The blocks must be overwritten, otherwise newly added files won't be
// accessible. Different TAR formats (such as `ustar`, `pax` and `GNU`)
// write different number of zero blocks.
func OpenTarSeekEnd(cname, workFQN string) (rwfh *os.File, err error) {
	if rwfh, err = os.OpenFile(workFQN, os.O_RDWR, cos.PermRWR); err != nil {
		return
	}
	if err = _seekTarEnd(cname, rwfh); err != nil {
		rwfh.Close() // always close on err
	}
	return
}

func _seekTarEnd(cname string, fh *os.File) error {
	var (
		size int64
		pos  = int64(-1)
		twr  = tar.NewReader(fh)
	)
	for {
		hdr, err := twr.Next()
		if err != nil {
			if err != io.EOF {
				return err // invalid TAR format
			}
			// EOF
			if pos < 0 {
				return ErrTarIsEmpty
			}
			break
		}
		pos, err = fh.Seek(0, io.SeekCurrent)
		if err != nil {
			debug.AssertNoErr(err) // unlikely
			return err
		}
		size = hdr.Size
	}
	if pos == 0 {
		return fmt.Errorf("failed to seek end of the TAR %s", cname)
	}
	padded := cos.CeilAlignInt64(size, TarBlockSize)
	_, err := fh.Seek(pos+padded, io.SeekStart)
	return err
}
