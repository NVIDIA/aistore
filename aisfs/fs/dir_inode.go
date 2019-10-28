// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path"

	"github.com/NVIDIA/aistore/aisfs/ais"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

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
	return res.Entry.Inode == dummyInodeID
}

type DirectoryInode struct {
	baseInode
	parent           *DirectoryInode
	children         map[string]fuseops.InodeID
	localDirectories map[string]fuseops.InodeID
	bucket           *ais.Bucket
}

// Ensure interface satisfaction.
var _ Inode = &DirectoryInode{}

func NewDirectoryInode(id fuseops.InodeID, name string, attrs fuseops.InodeAttributes, parent *DirectoryInode, bucket *ais.Bucket) Inode {
	return &DirectoryInode{
		baseInode:        newBaseInode(id, attrs, name),
		parent:           parent,
		bucket:           bucket,
		children:         make(map[string]fuseops.InodeID),
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
func (dir *DirectoryInode) NewEmptyDirEntry(name string, id fuseops.InodeID) {
	dir.NewEntry(name, id)
	dir.localDirectories[name] = id
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) NewEntry(name string, id fuseops.InodeID) {
	dir.children[name] = id
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) UnlinkEntry(name string) error {
	if err := dir.bucket.DeleteObject(name); err != nil {
		return err
	}

	delete(dir.children, name)
	return nil
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) ReadEntries() (entries []fuseutil.Dirent, err error) {
	var (
		offset fuseops.DirOffset = 1
	)

	// List objects from the bucket that belong to this directory.
	// (i.e. list object names starting with dir.Name())
	objectNames, err := dir.bucket.ListObjectNames(dir.Name())
	if err != nil {
		return
	}

	relativeNames := trimObjectNames(objectNames, dir.Name())

	for _, rname := range relativeNames {
		var en = fuseutil.Dirent{
			Inode:  dummyInodeID,
			Offset: offset,
		}
		en.Name, en.Type = direntNameAndType(rname)

		entries = append(entries, en)

		offset++
	}

	for name, id := range dir.localDirectories {
		var en = fuseutil.Dirent{
			Inode:  id,
			Offset: offset,
			Name:   name,
			Type:   fuseutil.DT_Directory,
		}

		entries = append(entries, en)

		offset++
	}

	return
}

// REQUIRES_LOCK(dir)
func (dir *DirectoryInode) LookupEntry(name string) (res EntryLookupResult, err error) {
	var (
		object  *ais.Object
		objects []*ais.Object
	)

	// First, we try to find a directory
	prefix := name + separator
	if dir.ID() != fuseops.RootInodeID {
		prefix = path.Join(dir.Name(), separator)
	}

	// Maybe it's an empty (local) directory.
	if id, ok := dir.localDirectories[name]; ok {
		res.Entry = &fuseutil.Dirent{
			Inode: id,
			Name:  name,
			Type:  fuseutil.DT_Directory,
		}
		return
	}

	// Fast path, assume this is an object: dir.Name()/name
	object, err = dir.bucket.HeadObject(path.Join(dir.Name(), name))
	if object != nil {
		res.Object = object
		inodeID := dummyInodeID
		if id, ok := dir.children[name]; ok {
			inodeID = id
		}
		res.Entry = &fuseutil.Dirent{
			Inode: inodeID,
			Name:  name,
			Type:  fuseutil.DT_File,
		}
		return
	}

	// If there is at least one object starting with dir.Name()/name/
	// then name is also a (non-empty) directory.
	objects, err = dir.bucket.ListObjects(prefix, 1)
	if err != nil {
		return
	}
	if len(objects) > 0 {
		inodeID := dummyInodeID
		if id, ok := dir.children[name]; ok {
			inodeID = id
		}
		res.Entry = &fuseutil.Dirent{
			Inode: inodeID,
			Name:  name,
			Type:  fuseutil.DT_Directory,
		}
	}
	return
}
