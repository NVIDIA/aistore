// Package fs implements an AIStore file system.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"log"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmd/aisfs/ais"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

const (
	// Determines the number of objects that are listed
	listObjsPageSize = 10_000
)

const (
	entryFileTy = entryType(fuseutil.DT_File)
	entryDirTy  = entryType(fuseutil.DT_Directory)
)

// interface guard
var (
	_ nsEntry = (*fileEntry)(nil)
	_ nsEntry = (*dirEntry)(nil)
)

type (
	entryType uint8

	nsEntry interface {
		Ty() entryType
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

	// Namespace is abstraction over whole bucket namespace.
	namespace struct {
		bck   ais.Bucket
		cfg   *ServerConfig
		cache *namespaceCache

		// Determines if the cache was able to read whole namespace into memory.
		// In case we do have all objects in memory we can enable some of the
		// performance improvements.
		cacheHasAllObjects atomic.Bool
	}
)

func (*fileEntry) Ty() entryType         { return entryFileTy }
func (e *fileEntry) ID() fuseops.InodeID { return e.id }
func (e *fileEntry) Name() string        { return e.object.Name }
func (e *fileEntry) Object() *ais.Object { return e.object }

func (*dirEntry) Ty() entryType         { return entryDirTy }
func (e *dirEntry) ID() fuseops.InodeID { return e.id }
func (e *dirEntry) Name() string        { return e.name }
func (*dirEntry) Object() *ais.Object   { return nil }

func newNamespace(bck ais.Bucket, logger *log.Logger, cfg *ServerConfig) (*namespace, error) {
	nsCache := newNsCache(bck, cfg)

	hasAllObjects, err := nsCache.refresh()
	if err != nil {
		return nil, err
	}

	ns := &namespace{
		bck:   bck,
		cfg:   cfg,
		cache: nsCache,
	}
	ns.cacheHasAllObjects.Store(hasAllObjects)

	if cfg.SyncInterval.Load() == 0 {
		return ns, nil
	}

	go func() {
		for {
			interval := cfg.SyncInterval.Load()
			if interval == 0 {
				// Someone disabled the syncing.
				return
			}

			time.Sleep(interval)
			logger.Printf("syncing with AIS...")
			if hasAllObjects, err := ns.cache.refresh(); err != nil {
				logger.Printf("failed to sync, err: %v", err)
			} else {
				ns.cacheHasAllObjects.Store(hasAllObjects)
				logger.Printf("syncing has finished successfully")
			}
		}
	}()
	return ns, nil
}

func (ns *namespace) add(ty entryType, dta dtAttrs) {
	if !ns.cacheHasAllObjects.Load() {
		// TODO: maybe we have enough memory to store this given entry - we should
		// check that first and then decide.
		return
	}

	ns.cache.add(ty, dta)
}

func (ns *namespace) remove(p string) {
	ns.cache.remove(p)
}

func (ns *namespace) lookup(p string) (res EntryLookupResult, exists bool) {
	if ns.cacheHasAllObjects.Load() {
		res, _, exists = ns.cache.lookup(p)
		return
	}

	res, _, exists = ns.cache.lookup(p)
	if exists {
		return
	}

	p = strings.TrimLeft(p, separator)
	// If asking for directory, we need to check if any objects with such prefix
	// exists.
	if strings.HasSuffix(p, separator) {
		objs, _, err := ns.bck.ListObjects(p, "", 1)
		if err != nil || len(objs) == 0 {
			return res, false
		}
		return res, true
	}

	// If it does not exist we are not sure if it actually does not exist or
	// just was not cached.
	obj, exists, err := ns.bck.HeadObject(p)
	if err != nil || !exists {
		return res, false
	}

	res.Object = obj
	return res, true
}

func (ns *namespace) listEntries(p string, cb func(nsEntry)) {
	if ns.cacheHasAllObjects.Load() {
		ns.cache.listEntries(p, cb)
		return
	}

	p = strings.TrimLeft(p, separator)
	token := ""
	for {
		objs, nextToken, err := ns.bck.ListObjects(p, token, listObjsPageSize)
		if err != nil || len(objs) == 0 {
			break
		}
		for _, obj := range objs {
			name := obj.Name[len(p):]
			idx := strings.Index(name, separator)
			// Either name does not contain separator (file) or contains
			// separator which means is nested and we should print it as directory.
			if idx == -1 {
				cb(ns.cache.newFileEntry(dtAttrs{
					id:  invalidInodeID,
					obj: obj,
				}))
			} else {
				cb(ns.cache.newDirEntry(dtAttrs{
					id:   invalidInodeID,
					path: obj.Name[:len(p)+idx+1],
				}))
			}
		}

		if nextToken == "" {
			break
		}
		token = nextToken
	}
}
