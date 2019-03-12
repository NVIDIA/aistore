/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/nanobox-io/golang-scribble"
)

const (
	persistDownloaderJobsPath = "downloader_jobs.db" // base name to persist downloader jobs' file
	downloaderCollection      = "jobs"
)

var (
	errJobNotFound = errors.New("job not found")
)

type downloaderDB struct {
	driver *scribble.Driver
	mtx    sync.Mutex
}

func newDownloadDB() (*downloaderDB, error) {
	config := cmn.GCO.Get()
	driver, err := scribble.New(filepath.Join(config.Confdir, persistDownloaderJobsPath), nil)
	if err != nil {
		return nil, err
	}

	return &downloaderDB{driver: driver}, nil
}

func (db *downloaderDB) getJob(id string) (body *cmn.DlBody, err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if err := db.driver.Read(downloaderCollection, id, &body); err != nil {
		if !os.IsNotExist(err) {
			glog.Error(err)
			return nil, err
		}
		return nil, errJobNotFound
	}
	return
}

func (db *downloaderDB) setJob(id string, body *cmn.DlBody) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if err := db.driver.Write(downloaderCollection, id, body); err != nil {
		glog.Error(err)
		return err
	}
	return nil
}

func (db *downloaderDB) delJob(id string) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if err := db.driver.Delete(downloaderCollection, id); err != nil {
		if !os.IsNotExist(err) {
			glog.Error(err)
			return err
		}
		return errJobNotFound
	}
	return nil
}
