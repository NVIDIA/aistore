// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"log"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fuse/ais"
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
	}

	fileEntry struct {
		id     fuseops.InodeID
		object *ais.Object
	}

	dirEntry struct {
		id   fuseops.InodeID
		name string
	}

	namespaceCache struct {
		m atomic.Pointer // keeps `*cmn.MultiSyncMap` type

		root       *dirEntry
		rootDirent *fuseutil.Dirent // contains (constant/cached) dirent for root

		bck *ais.Bucket

		cfg *ServerConfig

		// Determines if the cache was able to read whole namespace into memory.
		// In case we do have all objects in memory we can enable some of the
		// performance improvements.
		containsAllObjects bool
	}
)

func (e *fileEntry) Ty() entryType { return entryFileTy }
func (e *fileEntry) updateAttrs(dta dtAttrs) {
	// NOTE: Updating invalid inode with another invalid inode is equivalent
	// to invalidating invalid entry and should be considered a bug.
	cmn.Assert(e.id == invalidInodeID || dta.id == invalidInodeID)
	e.id = dta.id
	e.object.Size = dta.size
}
func (e *fileEntry) ID() fuseops.InodeID { return e.id }
func (e *fileEntry) Name() string        { return e.object.Name }
func (e *fileEntry) Object() *ais.Object { return e.object }

func (e *dirEntry) Ty() entryType { return entryDirTy }
func (e *dirEntry) updateAttrs(dta dtAttrs) {
	// NOTE: Updating invalid inode with another invalid inode is equivalent
	// to invalidating invalid entry and should be considered a bug.
	cmn.Assert(e.id == invalidInodeID || dta.id == invalidInodeID)
	e.id = dta.id
}
func (e *dirEntry) ID() fuseops.InodeID { return e.id }
func (e *dirEntry) Name() string        { return e.name }
func (e *dirEntry) Object() *ais.Object { return nil }

func newNsCache(bck *ais.Bucket, logger *log.Logger, cfg *ServerConfig) (*namespaceCache, error) {
	c := &namespaceCache{
		bck: bck,
		cfg: cfg,
	}
	c.m.Store(unsafe.Pointer(&cmn.MultiSyncMap{}))
	c.root = c.newDirEntry(dtAttrs{id: fuseops.RootInodeID, path: ""})
	c.rootDirent = &fuseutil.Dirent{
		Inode: c.root.ID(),
		Name:  c.root.Name(),
		Type:  fuseutil.DirentType(c.root.Ty()),
	}

	err := c.refresh()

	if cfg.SyncInterval > 0 {
		go func() {
			for {
				time.Sleep(cfg.SyncInterval)
				logger.Printf("syncing with AIS...")
				if err := c.refresh(); err != nil {
					logger.Printf("failed to sync, err: %v", err)
				} else {
					logger.Printf("syncing has finished successfully")
				}
			}
		}()
	}

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
		id:   dta.id,
		name: dta.path,
	}
}

func (c *namespaceCache) refresh() error {
	c.containsAllObjects = true
	newCache := &namespaceCache{
		bck: c.bck,
	}
	newCache.m.Store(unsafe.Pointer(&cmn.MultiSyncMap{}))

	var (
		objs       []*ais.Object
		err        error
		pageMarker string
	)
	for {
		objs, pageMarker, err = c.bck.ListObjects("", pageMarker, 50_000)
		if err != nil {
			return err
		}

		for _, obj := range objs {
			exists, _, entry := c.exists(obj.Name)
			id := invalidInodeID
			if exists {
				id = entry.ID()
			}

			newCache.add(entryFileTy, dtAttrs{
				id:   id,
				path: obj.Name,
				size: obj.Size,
			})
		}

		if pageMarker == "" {
			break
		}

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.HeapAlloc > c.cfg.MemoryLimit {
			c.containsAllObjects = false
			break
		}
	}

	c.m.Store(newCache.m.Load())
	return nil
}

func (c *namespaceCache) add(ty entryType, dta dtAttrs) {
	var (
		entry cacheEntry
	)

	arms := splitEntryName(dta.path)
	for idx := 0; idx < len(arms)-1; idx++ {
		dir := c.newDirEntry(dtAttrs{id: invalidInodeID, path: path.Join(arms[:idx+1]...) + separator})
		m := c.getCache(dir.name)
		m.LoadOrStore(dir.name, dir)
	}

	switch ty {
	case entryFileTy:
		cmn.Assert(!strings.HasSuffix(dta.path, separator))
		entry = c.newFileEntry(dta)
	case entryDirTy:
		cmn.Assert(strings.HasSuffix(dta.path, separator))
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
	for i := 0; i < cmn.MultiSyncMapCount; i++ {
		wg.Add(1)
		go func(i int) {
			m := c.getCacheByIdx(i)
			m.Range(func(k, v interface{}) bool {
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

func (c *namespaceCache) exists(p string) (exists bool, res EntryLookupResult, entry cacheEntry) {
	root := c.root
	if p == "" {
		res.Entry = c.rootDirent
		return true, res, root
	}

	m := c.getCache(p)
	v, exists := m.Load(p)
	if !exists {
		return false, res, entry
	}
	e := v.(cacheEntry)
	res.Object = e.Object()
	res.Entry = &fuseutil.Dirent{
		Inode: e.ID(),
		Name:  e.Name(),
		Type:  fuseutil.DirentType(e.Ty()),
	}
	return true, res, e
}

func (c *namespaceCache) listEntries(p string, cb func(cacheEntry)) {
	var (
		mtx sync.Mutex
		wg  = &sync.WaitGroup{}
	)
	for i := 0; i < cmn.MultiSyncMapCount; i++ {
		wg.Add(1)
		go func(i int) {
			m := c.getCacheByIdx(i)
			m.Range(func(k, v interface{}) bool {
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
					cb(v.(cacheEntry))
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
	return (*cmn.MultiSyncMap)(c.m.Load()).GetByHash(h)
}

func (c *namespaceCache) getCacheByIdx(i int) *sync.Map {
	return (*cmn.MultiSyncMap)(c.m.Load()).Get(i)
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
