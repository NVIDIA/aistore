// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"path"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/OneOfOne/xxhash"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

// Theory of operation
//
// Cache is a flat structure that keeps all information about current files and
// directories. It works like an oracle - it knows whether the file/directory
// exists or not. The cache updates/refreshes once in while to keep everything
// in sync with the AIS state.
//
// The cache itself is built on top of N sync.Maps which we access by hash of
// the key (name/path). The values in the maps are `cacheEntry`. When we
// insert `/a/b/c` file we also need to insert information about parent
// directories: `/a/` and `/a/b/`.
//
// The reason for creating the cache was to eliminate unnecessary calls to the
// AIS proxies eg. HEAD request to check if object exists or if given path
// is directory or not (this requires doing ListObjects what is expensive and
// can take 1-2sec just to check if the single directory exists).

type (
	// Data type attributes (either for file or directory)
	dtAttrs struct {
		id   fuseops.InodeID
		path string
		obj  *ais.Object
	}

	namespaceCache struct {
		m atomic.Pointer // keeps `*cmn.MultiSyncMap` type

		root       *dirEntry
		rootDirent *fuseutil.Dirent // contains (constant/cached) dirent for root

		bck ais.Bucket

		cfg *ServerConfig
	}
)

func newNsCache(bck ais.Bucket, cfg *ServerConfig) *namespaceCache {
	c := &namespaceCache{
		bck: bck,
		cfg: cfg,
	}
	c.m.Store(unsafe.Pointer(&cos.MultiSyncMap{}))
	c.root = c.newDirEntry(dtAttrs{id: fuseops.RootInodeID, path: ""})
	c.rootDirent = &fuseutil.Dirent{
		Inode: c.root.ID(),
		Name:  c.root.Name(),
		Type:  fuseutil.DirentType(c.root.Ty()),
	}

	return c
}

func (c *namespaceCache) newFileEntry(dta dtAttrs) nsEntry {
	// Allow `nil` object only for `invalidInodeID`
	cos.Assert(dta.obj != nil || dta.id == invalidInodeID)
	if dta.obj == nil {
		dta.obj = ais.NewObject(dta.path, c.bck, 0)
	}
	return &fileEntry{
		id:     dta.id,
		object: dta.obj,
	}
}

func (*namespaceCache) newDirEntry(dta dtAttrs) *dirEntry {
	return &dirEntry{
		id:   dta.id,
		name: dta.path,
	}
}

func (c *namespaceCache) refresh() (bool, error) {
	var (
		hasAllObjects = true

		newCache = &namespaceCache{
			bck: c.bck,
		}
	)
	newCache.m.Store(unsafe.Pointer(&cos.MultiSyncMap{}))

	var (
		objs      []*ais.Object
		err       error
		nextToken string
	)
	for {
		objs, nextToken, err = c.bck.ListObjects("", nextToken, 50_000)
		if err != nil {
			return false, err
		}

		for _, obj := range objs {
			_, entry, exists := c.lookup(obj.Name)
			id := invalidInodeID
			if exists {
				id = entry.ID()
			}

			newCache.add(entryFileTy, dtAttrs{
				id:   id,
				path: obj.Name,
				obj:  obj,
			})
		}

		if nextToken == "" {
			break
		}

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		memLimit := c.cfg.MemoryLimit.Load()
		if memLimit > 0 && mem.HeapAlloc > memLimit {
			hasAllObjects = false
			break
		}
	}

	c.m.Store(newCache.m.Load())
	return hasAllObjects, nil
}

func (c *namespaceCache) add(ty entryType, dta dtAttrs) {
	var entry nsEntry

	arms := splitEntryName(dta.path)
	for idx := 0; idx < len(arms)-1; idx++ {
		dir := c.newDirEntry(dtAttrs{id: invalidInodeID, path: path.Join(arms[:idx+1]...) + separator})
		m := c.getCache(dir.name)
		m.LoadOrStore(dir.name, dir)
	}

	switch ty {
	case entryFileTy:
		cos.Assert(!strings.HasSuffix(dta.path, separator))
		entry = c.newFileEntry(dta)
	case entryDirTy:
		cos.Assert(strings.HasSuffix(dta.path, separator))
		entry = c.newDirEntry(dta)
	default:
		panic(ty)
	}

	m := c.getCache(dta.path)
	m.Store(dta.path, entry)
}

func (c *namespaceCache) remove(p string) {
	// Fast path for file
	if !strings.HasSuffix(p, separator) {
		m := c.getCache(p)
		m.Delete(p)
		return
	}

	// Slow path for directory - we also need to remove all entries with prefix `p`.
	wg := &sync.WaitGroup{}
	for i := 0; i < cos.MultiSyncMapCount; i++ {
		wg.Add(1)
		go func(i int) {
			m := c.getCacheByIdx(i)
			m.Range(func(k, v any) bool {
				name := k.(string)
				if strings.HasPrefix(name, p) {
					m.Delete(k)
				}
				return true
			})
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (c *namespaceCache) lookup(p string) (res EntryLookupResult, entry nsEntry, exists bool) {
	root := c.root
	if p == "" {
		res.Entry = c.rootDirent
		return res, root, true
	}

	m := c.getCache(p)
	v, exists := m.Load(p)
	if !exists {
		return res, entry, false
	}
	e := v.(nsEntry)
	res.Object = e.Object()
	res.Entry = &fuseutil.Dirent{
		Inode: e.ID(),
		Name:  e.Name(),
		Type:  fuseutil.DirentType(e.Ty()),
	}
	return res, e, true
}

func (c *namespaceCache) listEntries(p string, cb func(nsEntry)) {
	var (
		mtx sync.Mutex
		wg  = &sync.WaitGroup{}
	)
	for i := 0; i < cos.MultiSyncMapCount; i++ {
		wg.Add(1)
		go func(i int) {
			m := c.getCacheByIdx(i)
			m.Range(func(k, v any) bool {
				name := k.(string)
				if !strings.HasPrefix(name, p) {
					return true
				}

				name = name[len(p):]
				if name == "" { // directory itself
					return true
				}

				idx := strings.Index(name, separator)
				// Either name does not contain separator (file) or contains
				// it at the end of the name (directory).
				if idx == -1 || idx == (len(name)-1) {
					mtx.Lock()
					cb(v.(nsEntry))
					mtx.Unlock()
				}
				return true
			})
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (c *namespaceCache) getCache(p string) *sync.Map {
	h := xxhash.ChecksumString32(p)
	return (*cos.MultiSyncMap)(c.m.Load()).GetByHash(h)
}

func (c *namespaceCache) getCacheByIdx(i int) *sync.Map {
	return (*cos.MultiSyncMap)(c.m.Load()).Get(i)
}

// splitEntryName splits the POSIX name into hierarchical arms. Each directory
// finishes with slash ("/"). Example: "a/b/c" will be split into: ["a/","b/","c"]
// whereas "a/b/c/" will be split into: ["a/", "b/", "c/"].
func splitEntryName(name string) []string {
	cos.AssertMsg(name != "", name)
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
