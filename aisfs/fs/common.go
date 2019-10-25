// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"errors"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/NVIDIA/aistore/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
)

const separator = string(filepath.Separator)

//////////
// TYPES
//////////

// EntryLookupResult is a struct returned as a result of
// directory entry lookup.
type EntryLookupResult struct {
	Entry  *fuseutil.Dirent
	Object *ais.Object
}

// NoEntry checks if lookup operation found an entry.
func (res EntryLookupResult) NoEntry() bool {
	return res.Entry == nil
}

// IsDir checks if an entry maps to a directory.
// Assumes that res.Entry != nil.
func (res EntryLookupResult) IsDir() bool {
	return res.Entry.Type == fuseutil.DT_Directory
}

// NoInode checks if inode number is known for an entry.
// Assumes that res.Entry != nil.
func (res EntryLookupResult) NoInode() bool {
	return res.Entry.Inode == invalidInodeID
}

///////////
// LOGGING
///////////

func (fs *aisfs) logf(fmt string, v ...interface{}) {
	fs.errLog.Printf(fmt, v...)
}

func (fs *aisfs) fatalf(fmt string, v ...interface{}) {
	errFmt := "FATAL: " + fmt +
		"\n*** CONNECTION LOST, BUT THE FILE SYSTEM REMAINS MOUNTED ON %s ***\n" +
		"CALL STACK ---> %s\n"
	v = append(v, fs.mountPath, debug.Stack())
	fs.errLog.Fatalf(errFmt, v...)
}

func (fs *aisfs) handleIOError(err error) error {
	var ioerr *ais.IOError
	if errors.As(err, &ioerr) {
		fs.logf("%v", err)
		return fuse.EIO
	}
	return err
}

///////////
// HELPERS
///////////

func isDirName(taggedName string) bool {
	return strings.HasSuffix(taggedName, separator)
}

func direntNameAndType(taggedName string) (string, fuseutil.DirentType) {
	if isDirName(taggedName) {
		// Separator is removed because directory entry names cannot contain
		// a path separator.
		return strings.TrimSuffix(taggedName, separator), fuseutil.DT_Directory
	}
	return taggedName, fuseutil.DT_File
}

func getTaggedNames(objNames []string, parentPath string) (taggedNames []string) {
	seen := make(cmn.StringSet)

	for _, name := range objNames {
		// Remove parent path prefix from object name.
		name = strings.TrimPrefix(name, parentPath)

		// Resolve conflicts between tagged names that can represent both
		// a file and a directory with the same name (e.g. a/b and a/b/)
		// by giving priority to file names.
		if split := strings.Index(name, separator); split != -1 {
			// Directory name
			name = name[:split+1]

			conflictingFile := strings.TrimSuffix(name, separator)
			if seen.Contains(conflictingFile) {
				continue
			}
		} else {
			// File name
			conflictingDir := name + separator
			delete(seen, conflictingDir)
		}

		// Remove duplicated names.
		if seen.Contains(name) {
			continue
		}

		seen.Add(name)
	}

	for name := range seen {
		taggedNames = append(taggedNames, name)
	}

	return
}
