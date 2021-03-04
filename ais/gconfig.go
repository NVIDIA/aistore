// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

type (
	globalConfig struct {
		cmn.ClusterConfig
	}
	configOwner struct {
		sync.Mutex
		config     atomic.Pointer // pointer to globalConf
		daemonType string
	}

	configModifier struct {
		pre   func(ctx *configModifier, clone *globalConfig) (updated bool, err error)
		final func(ctx *configModifier, clone *globalConfig)

		toUpdate *cmn.ConfigToUpdate
		msg      *cmn.ActionMsg
		query    url.Values
		wait     bool
	}
)

var _ revs = (*globalConfig)(nil)

func (config *globalConfig) tag() string     { return revsConfTag }
func (config *globalConfig) version() int64  { return config.Version }
func (config *globalConfig) marshal() []byte { return cmn.MustLocalMarshal(config) }
func (config *globalConfig) clone() *globalConfig {
	clone := &globalConfig{}
	cmn.MustMorphMarshal(config, clone)
	return clone
}

func (config *globalConfig) String() string {
	if config == nil {
		return "Conf <nil>"
	}
	return fmt.Sprintf("Conf v%d", config.Version)
}

////////////
// config //
////////////

func newConfOwner(daemonType string) *configOwner {
	return &configOwner{daemonType: daemonType}
}

func (co *configOwner) get() *globalConfig {
	return (*globalConfig)(co.config.Load())
}

func (co *configOwner) put(config *globalConfig) {
	co.config.Store(unsafe.Pointer(config))
}

func (co *configOwner) runPre(ctx *configModifier) (clone *globalConfig, updated bool, err error) {
	co.Lock()
	defer co.Unlock()
	clone = co.get().clone()
	if updated, err = ctx.pre(ctx, clone); err != nil || !updated {
		return
	}
	if err = co.updateGCO(clone); err != nil {
		return
	}
	clone.Version++
	clone.LastUpdated = time.Now().String()
	if err := co.persist(clone); err != nil {
		err = fmt.Errorf("FATAL: failed to persist %s: %v", clone, err)
		return nil, false, err
	}
	return
}

// Update the global config on primary proxy.
func (co *configOwner) modify(ctx *configModifier) (err error) {
	var (
		config  *globalConfig
		updated bool
	)
	if config, updated, err = co.runPre(ctx); err != nil || !updated {
		return err
	}
	if ctx.final != nil {
		ctx.final(ctx, config)
	}
	return
}

func (co *configOwner) persist(config *globalConfig) error {
	local := cmn.GCO.Get()
	savePath := path.Join(local.ConfigDir, gconfFname)
	if err := jsp.SaveMeta(savePath, config); err != nil {
		return err
	}
	co.put(config)
	return nil
}

// PRECONDITION: `co` should be under lock.
func (co *configOwner) updateGCO(newConfig *globalConfig) (err error) {
	debug.AssertMutexLocked(&co.Mutex)
	// NOTE: GCO.Update may mutate the
	return cmn.GCO.Update(newConfig.ClusterConfig)
}

func (co *configOwner) load() (err error) {
	co.Lock()
	defer co.Unlock()
	localConf := cmn.GCO.Get()
	config := &globalConfig{}
	_, err = jsp.LoadMeta(path.Join(localConf.ConfigDir, gconfFname), config)
	if err == nil {
		if err = co.updateGCO(config); err != nil {
			return
		}
		co.put(config)
		return
	}
	if !os.IsNotExist(err) {
		return
	}
	// If gconf file is missing, assume conf provided through CLI as global.
	// NOTE: We cannot use GCO.Get() here as cmn.GCO may also contain custom config.
	config = &globalConfig{}
	_, err = jsp.LoadMeta(cmn.GCO.GetGlobalConfigPath(), config)
	if err != nil {
		return
	}
	co.put(config)
	return
}

func (co *configOwner) modifyOverride(toUpdate *cmn.ConfigToUpdate) (err error) {
	co.Lock()
	defer co.Unlock()
	clone := cmn.GCO.Clone()
	err = cmn.GCO.SetConfigInMem(toUpdate, clone, co.daemonType)
	if err != nil {
		return
	}
	cmn.GCO.Put(clone)

	override := cmn.GCO.GetOverrideConfig()
	if override == nil {
		override = toUpdate
	} else {
		override.Merge(toUpdate)
	}

	jsp.SaveOverrideConfig(clone.ConfigDir, override)
	cmn.GCO.PutOverrideConfig(override)
	return
}
