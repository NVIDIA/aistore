// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"bytes"
	"io/ioutil"
	"path"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/ais"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Ensure interface satisfaction.
var _ Inode = &DirectoryInode{}

type DirectoryInode struct {
	baseInode

	parent *DirectoryInode
	bucket *ais.Bucket

	entries []fuseutil.Dirent
}

func NewDirectoryInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, path string, parent *DirectoryInode, bucket *ais.Bucket) Inode {
	return &DirectoryInode{
		baseInode: newBaseInode(id, attrs, path),
		parent:    parent,
		bucket:    bucket,
	}
}

func (dir *DirectoryInode) Parent() Inode {
	return dir.parent
}

func (dir *DirectoryInode) IsDir() bool {
	return true
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) UpdateAttributes(req *AttrUpdateReq) fuseops.InodeAttributes {
	attrs := dir.Attributes()
	if req.Mode != nil {
		attrs.Mode = *req.Mode
		attrs.Ctime = time.Now()
		dir.SetAttributes(attrs)
	}
	return attrs
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) NewFileEntry(entryName string, id fuseops.InodeID, size int64) {
	entryName = path.Join(dir.Path(), entryName)
	nsCache.add(entryFileTy, dtAttrs{id: id, path: entryName, size: size})

	// TODO: improve caching entries for `ReadEntries`
	dir.entries = nil
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) ForgetFile(entryName string) {
	entryName = path.Join(dir.Path(), entryName)
	nsCache.remove(entryName)

	// TODO: improve caching entries for `ReadEntries`
	dir.entries = nil
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) NewDirEntry(entryName string, id fuseops.InodeID) {
	entryName = path.Join(dir.Path(), entryName) + separator
	nsCache.add(entryDirTy, dtAttrs{id: id, path: entryName})

	// TODO: improve caching entries for `ReadEntries`
	dir.entries = nil
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) ForgetDir(entryName string) {
	entryName = path.Join(dir.Path(), entryName) + separator
	nsCache.remove(entryName)

	// TODO: improve caching entries for `ReadEntries`
	dir.entries = nil
}

func (dir *DirectoryInode) InvalidateInode(entryName string, isDir bool) {
	entryName = path.Join(dir.Path(), entryName)
	ty := entryFileTy
	if isDir {
		entryName += separator
		ty = entryDirTy
	}
	nsCache.add(ty, dtAttrs{id: invalidInodeID, path: entryName})
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) LinkNewFile(fileName string) (*ais.Object, error) {
	obj := ais.NewObject(fileName, dir.bucket)
	err := obj.Put(cmn.NopOpener(ioutil.NopCloser(bytes.NewReader([]byte{}))))
	if err != nil {
		obj = nil
	}
	return obj, err
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) ReadEntries() (entries []fuseutil.Dirent, err error) {
	// Traverse files and subdirectories of dir read from the bucket.
	exists, _, entry := nsCache.exists(dir.Path())
	if !exists {
		return nil, fuse.ENOENT
	}

	if dir.entries != nil {
		return dir.entries, nil
	}

	var offset fuseops.DirOffset = 1
	for _, child := range entry.ListChildren() {
		dir.entries = append(dir.entries, fuseutil.Dirent{
			Inode:  child.ID(),
			Offset: offset,
			Name:   path.Base(child.Name()),
			Type:   fuseutil.DirentType(child.Ty()),
		})
		offset++
	}
	return dir.entries, nil
}

// LOCKS(dir)
func (dir *DirectoryInode) UnlinkEntry(entryName string) error {
	objName := path.Join(dir.Path(), entryName)
	if err := dir.bucket.DeleteObject(objName); err != nil {
		return err
	}

	dir.Lock()
	dir.ForgetFile(entryName)
	dir.Unlock()
	return nil
}

func (dir *DirectoryInode) LookupEntry(entryName string) (res EntryLookupResult) {
	var (
		exists       bool
		objEntryName = path.Join(dir.Path(), entryName)
		dirEntryName = objEntryName + separator
	)

	// First check for directories
	exists, res, _ = nsCache.exists(dirEntryName)
	if exists {
		return res
	}

	_, res, _ = nsCache.exists(objEntryName)
	return res
}
