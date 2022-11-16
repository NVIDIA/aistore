// Package dloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dloader

import (
	"errors"
	"path"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/kvdb"
)

const (
	downloaderErrors     = "errors"
	downloaderTasks      = "tasks"
	downloaderCollection = "downloads"

	// Number of errors stored in memory. When the number of errors exceeds
	// this number, then all errors will be flushed to disk
	errCacheSize = 100

	// Number of tasks stored in memory. When the number of tasks exceeds
	// this number, then all errors will be flushed to disk
	taskInfoCacheSize = 1000
)

var errJobNotFound = errors.New("job not found")

type downloaderDB struct {
	mtx    sync.RWMutex
	driver kvdb.Driver

	errCache      map[string][]TaskErrInfo // memory cache for errors, see: errCacheSize
	taskInfoCache map[string][]TaskDlInfo  // memory cache for tasks, see: taskInfoCacheSize
}

func newDownloadDB(driver kvdb.Driver) *downloaderDB {
	return &downloaderDB{
		driver:        driver,
		errCache:      make(map[string][]TaskErrInfo, 10),
		taskInfoCache: make(map[string][]TaskDlInfo, 10),
	}
}

func (db *downloaderDB) errors(id string) (errors []TaskErrInfo, err error) {
	key := path.Join(downloaderErrors, id)
	if err := db.driver.Get(downloaderCollection, key, &errors); err != nil {
		if !kvdb.IsErrNotFound(err) {
			glog.Error(err)
			return nil, err
		}
		// If there was nothing in DB, return only values in the cache
		return db.errCache[id], nil
	}

	errors = append(errors, db.errCache[id]...)
	return
}

func (db *downloaderDB) getErrors(id string) (errors []TaskErrInfo, err error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return db.errors(id)
}

func (db *downloaderDB) persistError(id, objName, errMsg string) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	errInfo := TaskErrInfo{Name: objName, Err: errMsg}
	if len(db.errCache[id]) < errCacheSize { // if possible store error in cache
		db.errCache[id] = append(db.errCache[id], errInfo)
		return
	}

	errMsgs, err := db.errors(id) // it will also append errors from cache
	if err != nil {
		glog.Error(err)
		return
	}
	errMsgs = append(errMsgs, errInfo)

	key := path.Join(downloaderErrors, id)
	if err := db.driver.Set(downloaderCollection, key, errMsgs); err != nil {
		glog.Error(err)
		return
	}

	db.errCache[id] = db.errCache[id][:0] // clear cache
}

func (db *downloaderDB) tasks(id string) (tasks []TaskDlInfo, err error) {
	key := path.Join(downloaderTasks, id)
	if err := db.driver.Get(downloaderCollection, key, &tasks); err != nil {
		if !kvdb.IsErrNotFound(err) {
			glog.Error(err)
			return nil, err
		}
		// If there was nothing in DB, return empty list
		return db.taskInfoCache[id], nil
	}
	tasks = append(tasks, db.taskInfoCache[id]...)
	return
}

func (db *downloaderDB) persistTaskInfo(id string, task TaskDlInfo) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if len(db.taskInfoCache[id]) < taskInfoCacheSize { // if possible store task in cache
		db.taskInfoCache[id] = append(db.taskInfoCache[id], task)
		return nil
	}

	persistedTasks, err := db.tasks(id) // it will also append tasks from cache
	if err != nil {
		return err
	}
	persistedTasks = append(persistedTasks, task)

	key := path.Join(downloaderTasks, id)
	if err := db.driver.Set(downloaderCollection, key, persistedTasks); err != nil {
		glog.Error(err)
		return err
	}

	db.taskInfoCache[id] = db.taskInfoCache[id][:0] // clear cache
	return nil
}

func (db *downloaderDB) getTasks(id string) (tasks []TaskDlInfo, err error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	return db.tasks(id)
}

// flushes caches into the disk
func (db *downloaderDB) flush(id string) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if len(db.errCache[id]) > 0 {
		errMsgs, err := db.errors(id) // it will also append errors from cache
		if err != nil {
			return err
		}

		key := path.Join(downloaderErrors, id)
		if err := db.driver.Set(downloaderCollection, key, errMsgs); err != nil {
			glog.Error(err)
			return err
		}

		db.errCache[id] = db.errCache[id][:0] // clear cache
	}

	if len(db.taskInfoCache[id]) > 0 {
		persistedTasks, err := db.tasks(id) // it will also append tasks from cache
		if err != nil {
			return err
		}

		key := path.Join(downloaderTasks, id)
		if err := db.driver.Set(downloaderCollection, key, persistedTasks); err != nil {
			glog.Error(err)
			return err
		}

		db.taskInfoCache[id] = db.taskInfoCache[id][:0] // clear cache
	}
	return nil
}

func (db *downloaderDB) delete(id string) {
	db.mtx.Lock()
	key := path.Join(downloaderErrors, id)
	db.driver.Delete(downloaderCollection, key)
	key = path.Join(downloaderTasks, id)
	db.driver.Delete(downloaderCollection, key)
	db.mtx.Unlock()
}
