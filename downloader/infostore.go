// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"regexp"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/hk"
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

func initInfoStore(db dbdriver.Driver) {
	dlStoreOnce.Do(func() {
		dlStore = newInfoStore(db)
	})
}

func newInfoStore(driver dbdriver.Driver) *infoStore {
	db := newDownloadDB(driver)
	is := &infoStore{
		downloaderDB: db,
		jobInfo:      make(map[string]*downloadJobInfo),
	}
	hk.Reg("downloader", is.housekeep, hk.DayInterval)
	return is
}

func (is *infoStore) getJob(id string) (*downloadJobInfo, error) {
	is.RLock()
	defer is.RUnlock()

	if ji, ok := is.jobInfo[id]; ok {
		return ji, nil
	}
	return nil, errJobNotFound
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

func (is *infoStore) incFinished(id string) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.FinishedCnt.Inc()
}

func (is *infoStore) incSkipped(id string) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.SkippedCnt.Inc()
	jInfo.FinishedCnt.Inc()
}

func (is *infoStore) incScheduled(id string) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.ScheduledCnt.Inc()
}

func (is *infoStore) incErrorCnt(id string) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.ErrorCnt.Inc()
}

func (is *infoStore) setAllDispatched(id string, dispatched bool) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.AllDispatched.Store(dispatched)
}

func (is *infoStore) markFinished(id string) error {
	jInfo, err := is.getJob(id)
	if err != nil {
		debug.AssertNoErr(err)
		return err
	}
	jInfo.FinishedTime.Store(time.Now())
	return jInfo.valid()
}

func (is *infoStore) setAborted(id string) {
	jInfo, err := is.getJob(id)
	debug.AssertNoErr(err)
	jInfo.Aborted.Store(true)
	// NOTE: Don't set `FinishedTime` yet as we are not fully done.
	//       The job now can be removed but there's no guarantee
	//       that all tasks have been stopped and all resources were freed.
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
