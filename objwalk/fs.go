// Package objwalk provides core functionality for reading the list of a bucket objects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type (
	// used to traverse local filesystem and collect objects info
	allfinfos struct {
		t            cluster.Target
		smap         *cluster.Smap
		objs         []*cmn.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *cmn.SelectMsg
		lastFilePath string
		bucket       string
		fileCount    int
		rootLength   int
		limit        int

		needSize    bool
		needAtime   bool
		needCksum   bool
		needVersion bool
		needStatus  bool
		needCopies  bool
	}
)

// Checks if the directory should be processed by cache list call
// Does checks:
//  - Object name must start with prefix (if it is set)
//  - Object name is not in early processed directories by the previous call:
//    paging support
func (ci *allfinfos) processDir(fqn string) error {
	if len(fqn) <= ci.rootLength {
		return nil
	}

	// every directory has to either:
	// - start with prefix (for levels higher than prefix: prefix="ab", directory="abcd/def")
	// - or include prefix (for levels deeper than prefix: prefix="a/", directory="a/b/")
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(ci.prefix, relname) && !strings.HasPrefix(relname, ci.prefix) {
		return filepath.SkipDir
	}

	// When markerDir = "b/c/d/" we should skip directories: "a/", "b/a/",
	// "b/b/" etc. but should not skip entire "b/" or "b/c/" since it is our
	// parent which we want to traverse (see that: "b/" < "b/c/d/").
	if ci.markerDir != "" && relname < ci.markerDir && !strings.HasPrefix(ci.markerDir, relname) {
		return filepath.SkipDir
	}

	return nil
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
func (ci *allfinfos) lsObject(lom *cluster.LOM, objStatus uint16) error {
	relname := lom.FQN[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	if ci.marker != "" && relname <= ci.marker {
		return nil
	}

	if _ = lom.Load(); !lom.Exists() {
		return nil
	}

	// add the obj to the page
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:   relname,
		Atime:  "",
		Flags:  objStatus | cmn.EntryIsCached,
		Copies: 1,
	}
	if ci.needAtime {
		fileInfo.Atime = cmn.FormatTime(lom.Atime(), ci.msg.TimeFormat)
	}
	if ci.needCksum && lom.Cksum() != nil {
		_, storedCksum := lom.Cksum().Get()
		fileInfo.Checksum = storedCksum
	}
	if ci.needVersion {
		fileInfo.Version = lom.Version()
	}
	if ci.needCopies {
		fileInfo.Copies = int16(lom.NumCopies())
	}
	fileInfo.Size = lom.Size()
	ci.objs = append(ci.objs, fileInfo)
	ci.lastFilePath = lom.FQN
	return nil
}

// fast alternative of generic listwalk: do not fetch any object information
// Returns all objects if the `msg.PageSize` was not specified. But the result
// may have 'ghost' or duplicated  objects.
func (ci *allfinfos) listwalkfFast(fqn string, de fs.DirEntry) error {
	if ci.fileCount >= ci.limit {
		return filepath.SkipDir
	}
	if de.IsDir() {
		return ci.processDir(fqn)
	}

	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	if ci.marker != "" && relname <= ci.marker {
		return nil
	}
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:  relname,
		Flags: cmn.ObjStatusOK,
	}
	if ci.needSize {
		fi, err := os.Stat(fqn)
		if err == nil {
			fileInfo.Size = fi.Size()
		}
	}

	ci.objs = append(ci.objs, fileInfo)
	return nil
}

func (ci *allfinfos) listwalkf(fqn string, de fs.DirEntry) error {
	if ci.fileCount >= ci.limit {
		return filepath.SkipDir
	}
	if de.IsDir() {
		return ci.processDir(fqn)
	}

	var (
		objStatus uint16 = cmn.ObjStatusOK
	)
	lom := &cluster.LOM{T: ci.t, FQN: fqn}
	if err := lom.Init("", ""); err != nil {
		return err
	}
	if err := lom.Load(); err != nil {
		return err
	}
	if !lom.Exists() {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	if !lom.IsHRW() {
		objStatus = cmn.ObjStatusMoved
	} else {
		si, err := cluster.HrwTarget(lom.Bck(), lom.Objname, ci.smap)
		if err != nil {
			return err
		}
		if ci.t.Snode().DaemonID != si.DaemonID {
			objStatus = cmn.ObjStatusMoved
		}
	}
	return ci.lsObject(lom, objStatus)
}
