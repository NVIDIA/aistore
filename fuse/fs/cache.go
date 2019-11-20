// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/ais"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Theory of operation
//
// Cache is tree structure that keeps all information about current files and
// directories. It works like an oracle - it knows whether the file/directory
// exists or not. The cache should be updated/refreshed once in while
// (TODO: now it is not) to keep everything in sync with the AIS state.
//
// Cache is herachical structure built on recursive map where the key is current
// name of file or directory and the value is `cacheEntry`. Directories' entries
// have again map with their children. To get a certain nested entry eg: "a/b/c",
// we need to visit "a/" directory, then "b/" directory and check for "c" file.
// See `splitEntryName` function to check how arms are produced.
//
// The reason for creating the cache was to eliminate unnecessary calls to the
// AIS proxies eg. HEAD request to check if object exists or if given path
// is directory or not (this requires doing ListObjects with prefix what is
// super expensive).
//
// TODO: Refresh the cache once in a while.
// TODO: Refreshing the cache should remove entries that are not present in the objects.

const (
	entryFileTy = entryType(fuseutil.DT_File)
	entryDirTy  = entryType(fuseutil.DT_Directory)
)

var (
	// interface guard
	_ cacheEntry = &fileEntry{}
	_ cacheEntry = &dirEntry{}
)

type (
	// Data type attributes (either for file or directory)
	dtAttrs struct {
		id   fuseops.InodeID
		path string
		size int64
	}

	entryType uint8

	cacheEntry interface {
		Ty() entryType
		updateAttrs(dta dtAttrs)
		ID() fuseops.InodeID
		Name() string

		// Valid only for files
		Object() *ais.Object

		// Valid only for directories
		ListChildren() []cacheEntry
	}

	fileEntry struct {
		id     fuseops.InodeID
		object *ais.Object
	}

	dirEntry struct {
		id       fuseops.InodeID
		name     string
		children map[string]cacheEntry
	}

	namespaceCache struct {
		mu         sync.RWMutex
		root       *dirEntry
		rootDirent *fuseutil.Dirent // contains (constant/cached) dirent for root

		bck *ais.Bucket
	}
)

func (e *fileEntry) Ty() entryType              { return entryFileTy }
func (e *fileEntry) updateAttrs(dta dtAttrs)    { e.id, e.object.Size = dta.id, dta.size }
func (e *fileEntry) ID() fuseops.InodeID        { return e.id }
func (e *fileEntry) Name() string               { return e.object.Name }
func (e *fileEntry) Object() *ais.Object        { return e.object }
func (e *fileEntry) ListChildren() []cacheEntry { panic(e) }

func (e *dirEntry) Ty() entryType           { return entryDirTy }
func (e *dirEntry) updateAttrs(dta dtAttrs) { e.id = dta.id }
func (e *dirEntry) ID() fuseops.InodeID     { return e.id }
func (e *dirEntry) Name() string            { return e.name }
func (e *dirEntry) Object() *ais.Object     { return nil }
func (e *dirEntry) ListChildren() []cacheEntry {
	entries := make([]cacheEntry, 0, len(e.children))
	for _, entry := range e.children {
		entries = append(entries, entry)
	}
	return entries
}

func newNsCache(bck *ais.Bucket) (*namespaceCache, error) {
	c := &namespaceCache{
		bck: bck,
	}
	c.root = c.newDirEntry(dtAttrs{id: fuseops.RootInodeID, path: ""})
	c.rootDirent = &fuseutil.Dirent{
		Inode: c.root.ID(),
		Name:  c.root.Name(),
		Type:  fuseutil.DirentType(c.root.Ty()),
	}

	err := c.refresh("")
	return c, err
}

func (c *namespaceCache) newFileEntry(dta dtAttrs) cacheEntry {
	return &fileEntry{
		id:     dta.id,
		object: ais.NewObject(dta.path, c.bck, dta.size),
	}
}

func (c *namespaceCache) newDirEntry(dta dtAttrs) *dirEntry {
	return &dirEntry{
		id:       dta.id,
		name:     dta.path,
		children: make(map[string]cacheEntry),
	}
}

func (c *namespaceCache) refresh(prefix string) error {
	objs, err := c.bck.ListObjects(prefix)
	if err != nil {
		return err
	}

	// TODO: remove entries that are not present in `names`
	for _, obj := range objs {
		c.add(entryFileTy, dtAttrs{
			id:   invalidInodeID,
			path: obj.Name,
			size: obj.Size,
		})
	}
	return nil
}

func (c *namespaceCache) add(ty entryType, dta dtAttrs) {
	root := c.root
	arms := splitEntryName(dta.path)

	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, arm := range arms[:len(arms)-1] {
		if oldDir, ok := root.children[arm]; !ok {
			dir := c.newDirEntry(dtAttrs{id: invalidInodeID, path: path.Join(arms[:idx+1]...) + separator})
			root.children[arm] = dir
			root = dir
		} else {
			root = oldDir.(*dirEntry)
		}
	}

	var (
		entry cacheEntry
		arm   = arms[len(arms)-1]
	)

	// If entry already exists, just set `id`
	if _, exists := root.children[arm]; exists {
		root.children[arm].updateAttrs(dta)
		return
	}

	switch ty {
	case entryFileTy:
		entry = c.newFileEntry(dta)
	case entryDirTy:
		cmn.Assert(strings.HasSuffix(arm, separator))
		cmn.Assert(strings.HasSuffix(dta.path, separator))
		entry = c.newDirEntry(dta)
	default:
		panic(ty)
	}
	root.children[arm] = entry
}

func (c *namespaceCache) remove(p string) {
	arms := splitEntryName(p)
	root := c.root

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, arm := range arms[:len(arms)-1] {
		oldDir, ok := root.children[arm]
		if !ok {
			return
		}
		root = oldDir.(*dirEntry)
	}

	delete(root.children, arms[len(arms)-1])
}

func (c *namespaceCache) exists(p string) (exists bool, res EntryLookupResult, entry cacheEntry) {
	root := c.root
	if p == "" {
		res.Entry = c.rootDirent
		return true, res, root
	}

	arms := splitEntryName(p)
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, arm := range arms[:len(arms)-1] {
		oldDir, ok := root.children[arm]
		if !ok {
			return false, res, entry
		}
		root = oldDir.(*dirEntry)
	}

	e, exists := root.children[arms[len(arms)-1]]
	if !exists {
		return false, res, entry
	}
	res.Object = e.Object()
	res.Entry = &fuseutil.Dirent{
		Inode: e.ID(),
		Name:  e.Name(),
		Type:  fuseutil.DirentType(e.Ty()),
	}
	return true, res, e
}

// splitEntryName splits the POSIX name into hierarchical arms. Each directory
// finishes with slash ("/"). Example: "a/b/c" will be split into: ["a/","b/","c"]
// whereas "a/b/c/" will be split into: ["a/", "b/", "c/"].
func splitEntryName(name string) []string {
	cmn.AssertMsg(name != "", name)

	arms := make([]string, 0, strings.Count(name, separator))
	for {
		idx := strings.Index(name, separator)
		if idx == -1 {
			arms = append(arms, name)
			break
		}

		arms = append(arms, name[:idx+1])
		name = name[idx+1:]
		if name == "" {
			break
		}
	}
	return arms
}
