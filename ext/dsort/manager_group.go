//go:build dsort

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"path"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/hk"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	dsortCollection = "dsort"
	managersKey     = "managers"
)

// managerGroup abstracts multiple dsort managers into single struct.
type managerGroup struct {
	mtx      sync.Mutex // Synchronizes reading managers field and db access
	managers map[string]*Manager
	db       kvdb.Driver
}

func newManagerGroup(db kvdb.Driver) *managerGroup {
	mg := &managerGroup{
		managers: make(map[string]*Manager, 1),
		db:       db,
	}
	return mg
}

// Add new, non-initialized manager with given managerUUID to manager group.
// Returned manager is locked, it's caller responsibility to unlock it.
// Returns error when manager with specified managerUUID already exists.
func (mg *managerGroup) Add(managerUUID string) (*Manager, error) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	if _, exists := mg.managers[managerUUID]; exists {
		return nil, errors.Errorf("job %q already exists", managerUUID)
	}
	manager := &Manager{
		ManagerUUID: managerUUID,
		mg:          mg,
	}
	mg.managers[managerUUID] = manager
	manager.lock()
	return manager, nil
}

func (mg *managerGroup) List(descRegex *regexp.Regexp, onlyActive bool) []JobInfo {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	jobsInfos := make([]JobInfo, 0, len(mg.managers))
	for _, v := range mg.managers {
		if descRegex != nil && !descRegex.MatchString(v.Metrics.Description) {
			continue
		}
		job := v.Metrics.toJobInfo(v.ManagerUUID, v.Pars)
		if onlyActive && job.IsFinished() {
			continue
		}
		jobsInfos = append(jobsInfos, job)
	}

	// Always check persistent db
	records, code, err := mg.db.GetAll(dsortCollection, managersKey)
	if err != nil {
		if !cos.IsErrNotFound(err) {
			nlog.Errorln(err, code)
		}
		return jobsInfos
	}
	for _, r := range records {
		var m Manager
		if err := jsoniter.Unmarshal([]byte(r), &m); err != nil {
			nlog.Errorln(err)
			continue
		}
		if descRegex == nil || descRegex.MatchString(m.Metrics.Description) {
			job := m.Metrics.toJobInfo(m.ManagerUUID, m.Pars)
			if onlyActive && job.IsFinished() {
				continue
			}
			jobsInfos = append(jobsInfos, job)
		}
	}
	sort.Slice(jobsInfos, func(i, j int) bool {
		return jobsInfos[i].ID < jobsInfos[j].ID
	})

	return jobsInfos
}

// Get gets manager with given mangerUUID. When manager with given uuid does not
// exist and user requested persisted lookup, it looks for it in persistent
// storage and returns it if found. Returns false if does not exist, true
// otherwise.
func (mg *managerGroup) Get(managerUUID string, inclArchived bool) (*Manager, bool) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	manager, exists := mg.managers[managerUUID]
	if !exists && inclArchived {
		key := path.Join(managersKey, managerUUID)
		if code, err := mg.db.Get(dsortCollection, key, &manager); err != nil {
			if !cos.IsErrNotFound(err) {
				nlog.Errorln(err, code)
			}
			return nil, false
		}
		exists = true
	}
	return manager, exists
}

// Remove the managerUUID from history. Used for reducing clutter. Fails if process hasn't been cleaned up.
func (mg *managerGroup) Remove(managerUUID string) error {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	if manager, ok := mg.managers[managerUUID]; ok && !manager.Metrics.Archived.Load() {
		return errors.Errorf("%s process %s still in progress and cannot be removed", apc.ActDsort, managerUUID)
	} else if ok {
		delete(mg.managers, managerUUID)
	}

	key := path.Join(managersKey, managerUUID)
	_, _ = mg.db.Delete(dsortCollection, key) // Delete only returns err when record does not exist, which should be ignored
	return nil
}

// persist removes manager from manager group (memory) and moves all information
// about it to persistent storage (file). This operation allows for later access
// of old managers (including managers' metrics).
//
// When error occurs during moving manager to persistent storage, manager is not
// removed from memory.
func (mg *managerGroup) persist(managerUUID string) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	manager, exists := mg.managers[managerUUID]
	if !exists {
		return
	}

	manager.Metrics.Archived.Store(true)
	key := path.Join(managersKey, managerUUID)
	if code, err := mg.db.Set(dsortCollection, key, manager); err != nil {
		nlog.Errorln(err, code)
		return
	}
	delete(mg.managers, managerUUID)
}

func (mg *managerGroup) housekeep(int64) time.Duration {
	const (
		retryInterval   = time.Hour // retry interval in case error occurred
		regularInterval = hk.DayInterval
	)

	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	records, code, err := mg.db.GetAll(dsortCollection, managersKey)
	if err != nil {
		if cos.IsErrNotFound(err) {
			return regularInterval
		}
		nlog.Errorln(err, code)
		return retryInterval
	}

	for _, r := range records {
		var m Manager
		if err := jsoniter.Unmarshal([]byte(r), &m); err != nil {
			nlog.Errorln(err)
			return retryInterval
		}
		if time.Since(m.Metrics.Extraction.End) > regularInterval {
			key := path.Join(managersKey, m.ManagerUUID)
			_, _ = mg.db.Delete(dsortCollection, key)
		}
	}

	return regularInterval
}
