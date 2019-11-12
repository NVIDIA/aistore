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
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Ensure interface satisfaction.
var _ Inode = &DirectoryInode{}

type DirectoryInode struct {
	baseInode

	parent           *DirectoryInode
	entries          map[string]fuseops.InodeID
	localDirectories map[string]fuseops.InodeID
	bucket           *ais.Bucket
}

func NewDirectoryInode(id fuseops.InodeID, attrs fuseops.InodeAttributes, path string, parent *DirectoryInode, bucket *ais.Bucket) Inode {
	return &DirectoryInode{
		baseInode:        newBaseInode(id, attrs, path),
		parent:           parent,
		bucket:           bucket,
		entries:          make(map[string]fuseops.InodeID),
		localDirectories: make(map[string]fuseops.InodeID),
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
func (dir *DirectoryInode) NewEntry(entryName string, id fuseops.InodeID) {
	dir.entries[entryName] = id
}

// REQUIRES_LOCK(dir)
// Note: Ignores non-existent entryName. This is because ForgetEntry
// is called when an inode is being destroyed, but may also be called
// before that (e.g. Unlink, RmDir), deleting an entry earlier.
func (dir *DirectoryInode) ForgetEntry(entryName string) {
	delete(dir.entries, entryName)
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
func (dir *DirectoryInode) LinkLocalSubdir(entryName string, id fuseops.InodeID) {
	dir.localDirectories[entryName] = id
}

// LOCKS(dir)
func (dir *DirectoryInode) TryDeleteLocalSubdirEntry(entryName string) (ok bool) {
	dir.Lock()
	_, ok = dir.localDirectories[entryName]
	delete(dir.localDirectories, entryName)
	dir.ForgetEntry(entryName)
	dir.Unlock()
	return
}

// REQUIRES_READ_LOCK(dir)
func (dir *DirectoryInode) ReadEntries() (entries []fuseutil.Dirent, err error) {
	var offset fuseops.DirOffset = 1

	// List objects from the bucket that belong to this directory.
	// (i.e. list object names starting with dir.Path())
	objectNames, err := dir.bucket.ListObjectNames(dir.Path())
	if err != nil {
		return
	}

	// Traverse files and subdirectories of dir read from the bucket.
	for _, taggedName := range getTaggedNames(objectNames, dir.Path()) {
		var en = fuseutil.Dirent{
			Inode:  invalidInodeID,
			Offset: offset,
		}
		en.Name, en.Type = direntNameAndType(taggedName)

		if en.Type == fuseutil.DT_Directory {
			dir.RUnlock()
			dir.TryDeleteLocalSubdirEntry(en.Name)
			dir.RLock()
		}

		entries = append(entries, en)
		offset++
	}

	// Add remaining local directories.
	for entryName, id := range dir.localDirectories {
		var en = fuseutil.Dirent{
			Inode:  id,
			Offset: offset,
			Name:   entryName,
			Type:   fuseutil.DT_Directory,
		}

		entries = append(entries, en)
		offset++
	}

	return
}

// LOCKS(dir)
func (dir *DirectoryInode) UnlinkEntry(entryName string) error {
	objName := path.Join(dir.Path(), entryName)
	if err := dir.bucket.DeleteObject(objName); err != nil {
		return err
	}
	dir.Lock()
	dir.ForgetEntry(entryName)
	dir.Unlock()
	return nil
}

// READ_LOCKS(dir)
func (dir *DirectoryInode) LookupEntry(entryName string) (res EntryLookupResult, err error) {
	// Maybe it's an empty (local) directory.
	dir.RLock()
	id, ok := dir.localDirectories[entryName]
	dir.RUnlock()
	if ok {
		res.Entry = &fuseutil.Dirent{
			Inode: id,
			Name:  entryName,
			Type:  fuseutil.DT_Directory,
		}
		return
	}

	// Fast path, assume this is an object: dir.Name()/name
	object, err := dir.bucket.HeadObject(path.Join(dir.Path(), entryName))
	if object != nil {
		res.Object = object
		inodeID := invalidInodeID
		dir.RLock()
		if id, ok := dir.entries[entryName]; ok {
			inodeID = id
		}
		dir.RUnlock()
		res.Entry = &fuseutil.Dirent{
			Inode: inodeID,
			Name:  entryName,
			Type:  fuseutil.DT_File,
		}
		return
	}

	// If there is at least one object starting with dir.Name()/name/
	// then name is also a (non-empty) directory.
	prefix := path.Join(dir.Path(), entryName) + separator
	exists, err := dir.bucket.HasObjectWithPrefix(prefix)
	if err != nil {
		return
	}
	if exists {
		inodeID := invalidInodeID
		dir.RLock()
		if id, ok := dir.entries[entryName]; ok {
			inodeID = id
		}
		dir.RUnlock()
		res.Entry = &fuseutil.Dirent{
			Inode: inodeID,
			Name:  entryName,
			Type:  fuseutil.DT_Directory,
		}
		return
	}
	// res.Entry == nil
	return
}
