// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path"

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
func (dir *DirectoryInode) NewEntry(name string, id fuseops.InodeID) {
	dir.entries[name] = id
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) LinkEmptyFile(fileName string) (*ais.Object, error) {
	return dir.bucket.NewEmptyObject(fileName)
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) LinkLocalSubdir(entryName string, id fuseops.InodeID) {
	dir.localDirectories[entryName] = id
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) TryDeleteLocalSubdirEntry(entryName string) (ok bool) {
	_, ok = dir.localDirectories[entryName]
	delete(dir.localDirectories, entryName)
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
			dir.TryDeleteLocalSubdirEntry(en.Name)
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

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) UnlinkEntry(entryName string) error {
	objName := path.Join(dir.Path(), entryName)
	if err := dir.bucket.DeleteObject(objName); err != nil {
		return err
	}

	delete(dir.entries, entryName)
	return nil
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) LookupEntry(entryName string) (res EntryLookupResult, err error) {
	// Maybe it's an empty (local) directory.
	if id, ok := dir.localDirectories[entryName]; ok {
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
		if id, ok := dir.entries[entryName]; ok {
			inodeID = id
		}
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
		if id, ok := dir.entries[entryName]; ok {
			inodeID = id
		}
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
