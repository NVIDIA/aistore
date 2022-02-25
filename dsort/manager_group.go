// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"os"
	"path"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/hk"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

const (
	dsortCollection = "dsort"
	managersKey     = "managers"
)

var Managers *ManagerGroup

// ManagerGroup abstracts multiple dsort managers into single struct.
type ManagerGroup struct {
	mtx      sync.Mutex // Synchronizes reading managers field and db access
	managers map[string]*Manager
	db       dbdriver.Driver
}

func InitManagers(db dbdriver.Driver) {
	Managers = NewManagerGroup(db, false)
}

// NewManagerGroup returns new, initialized manager group.
func NewManagerGroup(db dbdriver.Driver, skipHk bool) *ManagerGroup {
	mg := &ManagerGroup{
		managers: make(map[string]*Manager, 1),
		db:       db,
	}
	if !skipHk {
		hk.Reg(apc.DSortNameLowercase, mg.housekeep, hk.DayInterval)
	}
	return mg
}

// Add new, non-initialized manager with given managerUUID to manager group.
// Returned manager is locked, it's caller responsibility to unlock it.
// Returns error when manager with specified managerUUID already exists.
func (mg *ManagerGroup) Add(managerUUID string) (*Manager, error) {
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

func (mg *ManagerGroup) List(descRegex *regexp.Regexp) []JobInfo {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	jobsInfos := make([]JobInfo, 0, len(mg.managers))
	for _, v := range mg.managers {
		if descRegex == nil || descRegex.MatchString(v.Metrics.Description) {
			jobsInfos = append(jobsInfos, v.Metrics.ToJobInfo(v.ManagerUUID))
		}
	}

	// Always check persistent db for now
	records, err := mg.db.GetAll(dsortCollection, managersKey)
	if err != nil {
		glog.Error(err)
		return jobsInfos
	}
	for _, r := range records {
		var m Manager
		if err := jsoniter.Unmarshal([]byte(r), &m); err != nil {
			glog.Error(err)
			continue
		}
		if descRegex == nil || descRegex.MatchString(m.Metrics.Description) {
			jobsInfos = append(jobsInfos, m.Metrics.ToJobInfo(m.ManagerUUID))
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
func (mg *ManagerGroup) Get(managerUUID string, ap ...bool) (*Manager, bool) {
	var allowPersisted bool

	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	if len(ap) > 0 {
		allowPersisted = ap[0]
	}

	manager, exists := mg.managers[managerUUID]
	if !exists && allowPersisted {
		key := path.Join(managersKey, managerUUID)
		if err := mg.db.Get(dsortCollection, key, &manager); err != nil {
			if !os.IsNotExist(err) {
				glog.Error(err)
			}
			return nil, false
		}
		exists = true
	}
	return manager, exists
}

// Remove the managerUUID from history. Used for reducing clutter. Fails if process hasn't been cleaned up.
func (mg *ManagerGroup) Remove(managerUUID string) error {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	if manager, ok := mg.managers[managerUUID]; ok && !manager.Metrics.Archived.Load() {
		return errors.Errorf("%s process %s still in progress and cannot be removed", apc.DSortName, managerUUID)
	} else if ok {
		delete(mg.managers, managerUUID)
	}

	key := path.Join(managersKey, managerUUID)
	_ = mg.db.Delete(dsortCollection, key) // Delete only returns err when record does not exist, which should be ignored
	return nil
}

// persist removes manager from manager group (memory) and moves all information
// about it to persistent storage (file). This operation allows for later access
// of old managers (including managers' metrics).
//
// When error occurs during moving manager to persistent storage, manager is not
// removed from memory.
func (mg *ManagerGroup) persist(managerUUID string) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	manager, exists := mg.managers[managerUUID]
	if !exists {
		return
	}

	manager.Metrics.Archived.Store(true)
	key := path.Join(managersKey, managerUUID)
	if err := mg.db.Set(dsortCollection, key, manager); err != nil {
		glog.Error(err)
		return
	}
	delete(mg.managers, managerUUID)
}

func (mg *ManagerGroup) AbortAll(err error) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	for _, manager := range mg.managers {
		manager.abort(err)
	}
}

func (mg *ManagerGroup) housekeep() time.Duration {
	const (
		retryInterval   = time.Hour // retry interval in case error occurred
		regularInterval = hk.DayInterval
	)

	mg.mtx.Lock()
	defer mg.mtx.Unlock()

	records, err := mg.db.GetAll(dsortCollection, managersKey)
	if err != nil {
		if dbdriver.IsErrNotFound(err) {
			return regularInterval
		}

		glog.Error(err)
		return retryInterval
	}

	for _, r := range records {
		var m Manager
		if err := jsoniter.Unmarshal([]byte(r), &m); err != nil {
			glog.Error(err)
			return retryInterval
		}
		if time.Since(m.Metrics.Extraction.End) > regularInterval {
			key := path.Join(managersKey, m.ManagerUUID)
			_ = mg.db.Delete(dsortCollection, key)
		}
	}

	return regularInterval
}
