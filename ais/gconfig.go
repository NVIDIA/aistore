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
	"github.com/NVIDIA/aistore/cmn/jsp"
)

type (
	globalConfig struct {
		cmn.Config
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
	cmn.Assert(config.MetaVersion != 0)
	config.SetRole(co.daemonType)
	co.config.Store(unsafe.Pointer(config))
}

func cfgBeginUpdate() *cmn.Config { return cmn.GCO.BeginUpdate() }
func cfgCommitUpdate(config *cmn.Config, detail string) (err error) {
	if err = jsp.SaveConfig(config); err != nil {
		cmn.GCO.DiscardUpdate()
		return fmt.Errorf("FATAL: failed writing config %s: %s, %v", cmn.GCO.GetGlobalConfigPath(), detail, err)
	}
	cmn.GCO.CommitUpdate(config)
	return
}

func (co *configOwner) runPre(ctx *configModifier) (clone *globalConfig, updated bool, err error) {
	co.Lock()
	defer co.Unlock()
	clone = co.get().clone()
	if updated, err = ctx.pre(ctx, clone); err != nil || !updated {
		return
	}
	clone.Version++
	clone.LastUpdated = time.Now().String()
	if err := co.persist(clone); err != nil {
		err = fmt.Errorf("FATAL: failed to persist %s: %v", clone, err)
		return nil, false, err
	}
	co.updateGCO()
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
	local := cmn.GCO.GetLocal()
	savePath := path.Join(local.ConfigDir, gconfFname)
	if err := jsp.Save(savePath, config, jsp.PlainLocal()); err != nil {
		return err
	}
	co.put(config)
	return nil
}

func (co *configOwner) updateGCO() (err error) {
	cmn.GCO.BeginUpdate()
	config := co.get().clone()
	config.SetLocalConf(cmn.GCO.GetLocal())
	if err = config.Validate(); err != nil {
		return
	}
	cmn.GCO.CommitUpdate(&config.Config)
	return
}

func (co *configOwner) load() (err error) {
	localConf := cmn.GCO.GetLocal()
	config := &globalConfig{}
	_, err = jsp.Load(path.Join(localConf.ConfigDir, gconfFname), config, jsp.Plain())
	if err == nil {
		if config.MetaVersion == 0 {
			config.MetaVersion = cmn.MetaVersion
		}
		co.put(config)
		return co.updateGCO()
	}
	if !os.IsNotExist(err) {
		return
	}
	// If gconf file is missing, assume conf provided through CLI as global.
	// NOTE: We cannot use GCO.Get() here as cmn.GCO may also contain custom config.
	config = &globalConfig{}
	_, err = jsp.Load(cmn.GCO.GetGlobalConfigPath(), config, jsp.Plain())
	if err != nil {
		return
	}
	config.MetaVersion = cmn.MetaVersion
	co.put(config)
	return
}

func setConfig(toUpdate *cmn.ConfigToUpdate, transient bool) error {
	config := cfgBeginUpdate()
	err := jsp.SetConfigInMem(toUpdate, config)
	if transient || err != nil {
		cmn.GCO.DiscardUpdate()
		return err
	}
	_ = transient // Ignore transient for now
	cfgCommitUpdate(config, "set config")
	return nil
}
