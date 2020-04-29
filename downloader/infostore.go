// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
)

var (
	// global downloader info store
	dlStore     *infoStore
	dlStoreOnce sync.Once
)

type (
	infoStore struct {
		*downloaderDB

		// FIXME: jobInfo is stored only in memory, should be persisted at some point
		// in case, for instance, of target's powercycle
		jobInfo map[string]*downloadJobInfo
		sync.RWMutex
	}
)

func initInfoStore() {
	dlStoreOnce.Do(func() {
		var err error
		dlStore, err = newInfoStore()
		if err != nil {
			cmn.ExitLogf("%v", err)
		}
	})
}

func newInfoStore() (*infoStore, error) {
	db, err := newDownloadDB()
	if err != nil {
		return nil, err
	}

	is := &infoStore{
		downloaderDB: db,
		jobInfo:      make(map[string]*downloadJobInfo),
	}
	hk.Housekeeper.Register("downloader", is.housekeep, hk.DayInterval)
	return is, nil
}

func (is *infoStore) getJob(id string) (*downloadJobInfo, error) {
	is.RLock()
	defer is.RUnlock()

	if ji, ok := is.jobInfo[id]; ok {
		return ji, nil
	}

	return nil, fmt.Errorf("download job with id %q has not been found", id)
}

func (is *infoStore) getList(descRegex *regexp.Regexp) []*downloadJobInfo {
	jobsInfo := make([]*downloadJobInfo, 0)

	is.RLock()
	for _, dji := range is.jobInfo {
		if descRegex == nil || descRegex.MatchString(dji.Description) {
			jobsInfo = append(jobsInfo, dji)
		}
	}
	is.RUnlock()

	return jobsInfo
}

func (is *infoStore) setJob(id string, job DlJob) {
	jInfo := &downloadJobInfo{
		ID:          job.ID(),
		Total:       job.Len(),
		Description: job.Description(),
		StartedTime: time.Now(),
	}

	is.Lock()
	is.jobInfo[id] = jInfo
	is.Unlock()
}

func (is *infoStore) incFinished(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.FinishedCnt.Inc()
	return nil
}

func (is *infoStore) incScheduled(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.ScheduledCnt.Inc()
	return nil
}

func (is *infoStore) incErrorCnt(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.ErrorCnt.Inc()
	return nil
}

func (is *infoStore) setAllDispatched(id string, dispatched bool) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.AllDispatched.Store(dispatched)
	return nil
}

func (is *infoStore) markFinished(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.FinishedTime.Store(time.Now())
	return nil
}

func (is *infoStore) setAborted(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		return err
	}

	jInfo.Aborted.Store(true)
	return nil
}

func (is *infoStore) delJob(id string) {
	delete(is.jobInfo, id)
	is.downloaderDB.delete(id)
}

func (is *infoStore) housekeep() time.Duration {
	const interval = hk.DayInterval

	is.Lock()
	for id, jInfo := range is.jobInfo {
		if time.Since(jInfo.FinishedTime.Load()) > interval {
			is.delJob(id)
		}
	}
	is.Unlock()

	return interval
}
