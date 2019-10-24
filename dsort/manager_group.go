// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/housekeep/hk"
	"github.com/pkg/errors"
	"github.com/sdomino/scribble"
)

const (
	persistManagersPath = cmn.DSortNameLowercase + "_managers.db" // base name to persist managers' file
	managersCollection  = "managers"
)

var (
	Managers = NewManagerGroup()
)

// ManagerGroup abstracts multiple dsort managers into single struct.
type ManagerGroup struct {
	mtx      sync.Mutex // Synchronizes reading managers field and db access
	managers map[string]*Manager
}

// NewManagerGroup returns new, initialized manager group.
func NewManagerGroup() *ManagerGroup {
	mg := &ManagerGroup{
		managers: make(map[string]*Manager, 1),
	}
	hk.Housekeeper.Register(cmn.DSortNameLowercase, mg.housekeep, hk.DayInterval)
	return mg
}

// Add new, non-initialized manager with given managerUUID to manager group.
// Returned manager is locked, it's caller responsibility to unlock it.
// Returns error when manager with specified managerUUID already exists.
func (mg *ManagerGroup) Add(managerUUID string) (*Manager, error) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	if _, exists := mg.managers[managerUUID]; exists {
		return nil, errors.Errorf("manager with given uuid %s already exists", managerUUID)
	}
	manager := &Manager{
		ManagerUUID: managerUUID,
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
	config := cmn.GCO.Get()
	db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
	if err != nil {
		glog.Error(err)
		return jobsInfos
	}
	records, err := db.ReadAll(managersCollection)
	if err != nil {
		if !os.IsNotExist(err) {
			glog.Error(err)
		}
		return jobsInfos
	}
	for _, r := range records {
		var m Manager
		if err := json.Unmarshal([]byte(r), &m); err != nil {
			glog.Error(err)
			continue
		}
		if descRegex == nil || descRegex.MatchString(m.Metrics.Description) {
			jobsInfos = append(jobsInfos, m.Metrics.ToJobInfo(m.ManagerUUID))
		}
	}

	return jobsInfos
}

// Get gets manager with given mangerUUID. When manager with given uuid does not
// exists and user requested persisted lookup, it looks for it in persistent
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
		config := cmn.GCO.Get()
		db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
		if err != nil {
			glog.Error(err)
			return nil, false
		}
		if err := db.Read(managersCollection, managerUUID, &manager); err != nil {
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

	if manager, ok := mg.managers[managerUUID]; ok && !manager.Metrics.Archived {
		return errors.Errorf("%s process %s still in progress and cannot be removed", cmn.DSortName, managerUUID)
	} else if ok {
		delete(mg.managers, managerUUID)
	}

	config := cmn.GCO.Get()
	db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
	if err != nil {
		glog.Error(err)
		return err
	}
	_ = db.Delete(managersCollection, managerUUID) // Delete only returns err when record does not exist, which should be ignored
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

	manager.Metrics.Archived = true
	config := cmn.GCO.Get()
	db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
	if err != nil {
		glog.Error(err)
		return
	}
	if err = db.Write(managersCollection, managerUUID, manager); err != nil {
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

	config := cmn.GCO.Get()
	db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
	if err != nil {
		glog.Error(err)
		return retryInterval
	}

	managersUUID, err := db.ReadAll(managersCollection)
	if err != nil {
		if os.IsNotExist(err) {
			return regularInterval
		}

		glog.Error(err)
		return retryInterval
	}

	var manager Manager
	for _, managerUUID := range managersUUID {
		if err = db.Read(managersCollection, managerUUID, &manager); err != nil {
			glog.Error(err)
			return retryInterval
		}

		if time.Since(manager.Metrics.Extraction.End) > regularInterval {
			_ = db.Delete(managersCollection, managerUUID)
		}
	}

	return regularInterval
}
