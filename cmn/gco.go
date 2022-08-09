// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type globalConfigOwner struct {
	c        atomic.Pointer // pointer to `Config` (cluster + local + override config)
	oc       atomic.Pointer // pointer to `ConfigToUpdate`, override configuration on node
	confPath atomic.Pointer // pointer to initial global config path
	mtx      sync.Mutex
}

// GCO (Global Config Owner) is responsible for updating and notifying
// listeners about any changes in the config. Global Config is loaded
// at startup and then can be accessed/updated by other services.
var GCO *globalConfigOwner

func (gco *globalConfigOwner) Get() *Config {
	return (*Config)(gco.c.Load())
}

func (gco *globalConfigOwner) Put(config *Config) {
	gco.c.Store(unsafe.Pointer(config))
}

func (gco *globalConfigOwner) GetOverrideConfig() *ConfigToUpdate {
	return (*ConfigToUpdate)(gco.oc.Load())
}

func (gco *globalConfigOwner) MergeOverrideConfig(toUpdate *ConfigToUpdate) (overrideConfig *ConfigToUpdate) {
	overrideConfig = gco.GetOverrideConfig()
	if overrideConfig == nil {
		overrideConfig = toUpdate
	} else {
		overrideConfig.Merge(toUpdate)
	}
	return
}

func (gco *globalConfigOwner) SetLocalFSPaths(toUpdate *ConfigToUpdate) (overrideConfig *ConfigToUpdate) {
	overrideConfig = gco.GetOverrideConfig()
	if overrideConfig == nil {
		overrideConfig = toUpdate
	} else {
		overrideConfig.FSP = toUpdate.FSP // no merging required
	}
	return
}

func (gco *globalConfigOwner) PutOverrideConfig(config *ConfigToUpdate) {
	gco.oc.Store(unsafe.Pointer(config))
}

// CopyStruct is a shallow copy, which is fine as FSPaths and BackendConf "exceptions"
// are taken care of separately. When cloning, beware: config is a large structure.
func (gco *globalConfigOwner) Clone() *Config {
	config := &Config{}

	cos.CopyStruct(config, gco.Get())
	return config
}

// When updating we need to make sure that the update is transaction and no
// other update can happen when other transaction is in progress. Therefore,
// we introduce locking mechanism which targets this problem.
//
// NOTE: BeginUpdate must be followed by CommitUpdate.
// NOTE: `ais` package must use config-owner to modify config.
func (gco *globalConfigOwner) BeginUpdate() *Config {
	gco.mtx.Lock()
	return gco.Clone()
}

// CommitUpdate finalizes config update and notifies listeners.
// NOTE: `ais` package must use config-owner to modify config.
func (gco *globalConfigOwner) CommitUpdate(config *Config) {
	gco.c.Store(unsafe.Pointer(config))
	gco.mtx.Unlock()
}

// DiscardUpdate discards commit updates.
// NOTE: `ais` package must use config-owner to modify config
func (gco *globalConfigOwner) DiscardUpdate() {
	gco.mtx.Unlock()
}

func (gco *globalConfigOwner) SetInitialGconfPath(path string) {
	gco.confPath.Store(unsafe.Pointer(&path))
}

func (gco *globalConfigOwner) GetInitialGconfPath() (s string) {
	return *(*string)(gco.confPath.Load())
}

func (gco *globalConfigOwner) Update(cluConfig *ClusterConfig) (err error) {
	config := gco.Clone()
	config.ClusterConfig = *cluConfig
	override := gco.GetOverrideConfig()
	if override != nil {
		err = config.UpdateClusterConfig(*override, apc.Daemon) // update and validate
	} else {
		err = config.Validate()
	}
	if err != nil {
		return
	}
	gco.Put(config)
	return
}
