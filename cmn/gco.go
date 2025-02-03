// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// GCO (Global Config Owner) is responsible for updating and notifying
// listeners about any changes in the config. Global Config is loaded
// at startup and then can be accessed/updated by other services.

type (
	gco struct {
		c        ratomic.Pointer[Config]      // (cluster + local + override config)
		oc       ratomic.Pointer[ConfigToSet] // for a node to override inherited (global) configuration
		confPath ratomic.Pointer[string]      // initial (plain-text) global config path
		mtx      sync.Mutex                   // [BeginUpdate -- CommitUpdate]
	}
)

var GCO *gco

func _initGCO() {
	GCO = &gco{}
	GCO.c.Store(&Config{})

	Rom.init()
}

/////////
// gco //
/////////

func (gco *gco) Get() *Config { return gco.c.Load() }

func (gco *gco) Put(config *Config) {
	gco.c.Store(config)
	// update assorted read-mostly knobs
	Rom.Set(&config.ClusterConfig)
}

func (gco *gco) GetOverride() *ConfigToSet       { return gco.oc.Load() }
func (gco *gco) PutOverride(config *ConfigToSet) { gco.oc.Store(config) }

func (gco *gco) MergeOverride(toUpdate *ConfigToSet) (overrideConfig *ConfigToSet) {
	overrideConfig = gco.GetOverride()
	if overrideConfig == nil {
		overrideConfig = toUpdate
	} else {
		overrideConfig.Merge(toUpdate)
	}
	return
}

func (gco *gco) SetLocalFSPaths(toUpdate *ConfigToSet) (overrideConfig *ConfigToSet) {
	overrideConfig = gco.GetOverride()
	if overrideConfig == nil {
		overrideConfig = toUpdate
	} else {
		overrideConfig.FSP = toUpdate.FSP // no merging required
	}
	return
}

// CopyStruct is a shallow copy, which is fine as FSPaths and BackendConf "exceptions"
// are taken care of separately. When cloning, beware: config is a large structure.
func (gco *gco) Clone() *Config {
	config := &Config{}
	cos.CopyStruct(config, gco.Get())
	return config
}

// When updating we need to make sure that the update is transaction and no
// other update can happen when other transaction is in progress. Therefore,
// we introduce locking mechanism which targets this problem.
// NOTE:
// - BeginUpdate must be followed by CommitUpdate.
// - `ais` package must use config-owner to modify config.
func (gco *gco) BeginUpdate() *Config {
	gco.mtx.Lock()
	return gco.Clone()
}

// CommitUpdate finalizes config update and notifies listeners.
// NOTE: `ais` package must use config-owner to modify config.
func (gco *gco) CommitUpdate(config *Config) {
	gco.c.Store(config)
	gco.mtx.Unlock()
}

// DiscardUpdate discards commit updates.
// NOTE: `ais` package must use config-owner to modify config
func (gco *gco) DiscardUpdate() {
	gco.mtx.Unlock()
}

func (gco *gco) SetInitialGconfPath(path string) { gco.confPath.Store(&path) }
func (gco *gco) GetInitialGconfPath() string     { return *gco.confPath.Load() }

func (gco *gco) Update(cluConfig *ClusterConfig) (err error) {
	config := gco.Clone()
	config.ClusterConfig = *cluConfig
	override := gco.GetOverride()
	if override != nil {
		err = config.UpdateClusterConfig(override, apc.Daemon) // update and validate
	} else {
		err = config.Validate()
	}
	if err != nil {
		return
	}
	gco.Put(config)
	return
}
