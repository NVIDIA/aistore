// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	scribble "github.com/nanobox-io/golang-scribble"
)

const (
	persistManagersPath = "dsort_managers.db" // base name to persist managers' file
	managersCollection  = "managers"
)

var (
	Managers *ManagerGroup = NewManagerGroup()
)

// ManagerGroup abstracts multiple dsort managers into single struct.
type ManagerGroup struct {
	mtx      sync.Mutex
	managers map[string]*Manager
}

// NewManagerGroup returns new, initialized manager group.
func NewManagerGroup() *ManagerGroup {
	return &ManagerGroup{
		managers: make(map[string]*Manager, 1),
	}
}

// Add new, non-initialized manager with given managerUUID to manager group.
// Returned manager is locked, it's caller responsibility to unlock it.
// Returns error when manager with specified managerUUID already exists.
func (mg *ManagerGroup) Add(managerUUID string) (*Manager, error) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	if _, exists := mg.managers[managerUUID]; exists {
		return nil, fmt.Errorf("manager with given uuid %s already exists", managerUUID)
	}
	manager := &Manager{
		ManagerUUID: managerUUID,
	}
	mg.managers[managerUUID] = manager
	manager.lock()
	return manager, nil
}

// Get gets manager with given mangerUUID. When manager with given uuid does not
// exists, it looks for it in presistent storage and returns it if found. Returns
// false if does not exist, true otherwise.
func (mg *ManagerGroup) Get(managerUUID string) (*Manager, bool) {
	mg.mtx.Lock()
	defer mg.mtx.Unlock()
	manager, exists := mg.managers[managerUUID]
	if !exists {
		config := cmn.GCO.Get()
		db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
		if err != nil {
			glog.Error(err)
			return nil, false
		}
		if err := db.Read(managersCollection, managerUUID, &manager); err != nil {
			glog.Error(err)
			return nil, false
		}
		exists = true
	}
	return manager, exists
}

// persist removes manager from manager group (memory) and moves all information
// about it to presistent storage (file). This operation allows for later access
// of old managers (including managers' metrics).
//
// When error occurs during moving manager to persistent storage, manager is not
// removed from memory.
func (mg *ManagerGroup) persist(managerUUID string, cleanupRequired bool) {
	mg.mtx.Lock()
	manager, exists := mg.managers[managerUUID]
	if !exists {
		mg.mtx.Unlock()
		return
	}

	config := cmn.GCO.Get()
	db, err := scribble.New(filepath.Join(config.Confdir, persistManagersPath), nil)
	if err != nil {
		glog.Error(err)
		goto cleanup
	}
	if err = db.Write(managersCollection, managerUUID, manager); err != nil {
		glog.Error(err)
		goto cleanup
	}
	delete(mg.managers, managerUUID)

cleanup:
	// Cleanup can be done at very end. This enables to read metrics without
	// waiting for cleanup to finish - otherwise we would block on
	// ManagerGroup.Get.
	mg.mtx.Unlock()

	if cleanupRequired {
		manager.cleanup()
	}
}
