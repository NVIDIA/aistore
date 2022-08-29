// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"path/filepath"
	"runtime/debug"

	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

const separator = string(filepath.Separator)

///////////
// TYPES //
///////////

type (
	// EntryLookupResult is a struct returned as a result of
	// directory entry lookup.
	EntryLookupResult struct {
		Entry  *fuseutil.Dirent
		Object *ais.Object
	}
)

// NoEntry checks if lookup operation found an entry.
func (res EntryLookupResult) NoEntry() bool {
	return res.Entry == nil
}

// IsDir checks if an entry maps to a directory.
func (res EntryLookupResult) IsDir() bool {
	cos.Assert(res.Entry != nil)
	return res.Entry.Type == fuseutil.DT_Directory
}

// NoInode checks if inode number is known for an entry.
func (res EntryLookupResult) NoInode() bool {
	cos.Assert(res.Entry != nil)
	return res.Entry.Inode == invalidInodeID
}

/////////////
// LOGGING //
/////////////

func (fs *aisfs) logf(fmt string, v ...interface{}) {
	fs.errLog.Printf(fmt, v...)
}

func (fs *aisfs) fatalf(fmt string, v ...interface{}) {
	errFmt := "FATAL: " + fmt +
		"\n*** CONNECTION LOST, BUT THE FILE SYSTEM REMAINS MOUNTED ON %s ***\n" +
		"CALL STACK ---> %s\n"
	v = append(v, fs.cfg.MountPath, debug.Stack())
	fs.errLog.Fatalf(errFmt, v...)
}

func (fs *aisfs) handleIOError(err error) error {
	var ioerr *ais.ErrIO
	if errors.As(err, &ioerr) {
		fs.logf("%v", err)
		return fuse.EIO
	}
	return err
}
