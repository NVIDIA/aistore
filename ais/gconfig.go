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

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	globalConfig struct {
		cmn.ClusterConfig
	}
	configOwner struct {
		sync.Mutex
	}

	configModifier struct {
		pre   func(ctx *configModifier, clone *globalConfig) (updated bool, err error)
		final func(ctx *configModifier, clone *globalConfig)

		oldConfig *cmn.Config
		toUpdate  *cmn.ConfigToUpdate
		msg       *cmn.ActionMsg
		query     url.Values
		wait      bool
	}
)

// interface guard
var _ revs = (*globalConfig)(nil)

// as revs
func (config *globalConfig) tag() string             { return revsConfTag }
func (config *globalConfig) version() int64          { return config.Version }
func (config *globalConfig) marshal() []byte         { return cos.MustMarshal(config) }
func (config *globalConfig) jit(p *proxyrunner) revs { g, _ := p.owner.config.get(); return g }
func (config *globalConfig) sgl() *memsys.SGL        { return nil }

////////////
// config //
////////////

func (co *configOwner) get() (*globalConfig, error) {
	gco := cmn.GCO.Get()
	config := &globalConfig{}
	_, err := jsp.LoadMeta(path.Join(gco.ConfigDir, cmn.GlobalConfigFname), config)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return config, err
}

func (co *configOwner) version() int64 {
	return cmn.GCO.Get().Version
}

func (co *configOwner) runPre(ctx *configModifier) (clone *globalConfig, err error) {
	co.Lock()
	defer co.Unlock()
	clone, err = co.get()
	if err != nil {
		return
	}
	// NOTE: config `nil` implies missing cluster config.
	//        We need to use the config provided through CLI to generate cluster config.
	if clone == nil {
		clone = &globalConfig{}
		_, err = jsp.Load(cmn.GCO.GetGlobalConfigPath(), clone, jsp.Plain())
		if err != nil {
			return
		}
	}

	if updated, err := ctx.pre(ctx, clone); err != nil || !updated {
		return nil, err
	}

	ctx.oldConfig = cmn.GCO.Get()
	if err = co.updateGCO(clone); err != nil {
		clone = nil
		return
	}

	clone.Version++
	clone.LastUpdated = time.Now().String()
	if err := co.persist(clone); err != nil {
		err = fmt.Errorf("FATAL: failed to persist %s: %v", clone, err)
		return nil, err
	}
	return
}

// Update the global config on primary proxy.
func (co *configOwner) modify(ctx *configModifier) (config *globalConfig, err error) {
	if config, err = co.runPre(ctx); err != nil || config == nil {
		return
	}
	if ctx.final != nil {
		ctx.final(ctx, config)
	}
	return
}

func (co *configOwner) persist(config *globalConfig) error {
	local := cmn.GCO.Get()
	savePath := path.Join(local.ConfigDir, cmn.GlobalConfigFname)
	return jsp.SaveMeta(savePath, config)
}

// PRECONDITION: `co` should be under lock.
func (co *configOwner) updateGCO(newConfig *globalConfig) (err error) {
	debug.AssertMutexLocked(&co.Mutex)
	return cmn.GCO.Update(newConfig.ClusterConfig)
}

func (co *configOwner) setDaemonConfig(toUpdate *cmn.ConfigToUpdate, transient bool) (err error) {
	co.Lock()
	clone := cmn.GCO.Clone()
	err = cmn.GCO.SetConfigInMem(toUpdate, clone, cmn.Daemon)
	if err != nil {
		co.Unlock()
		return
	}

	override := cmn.GCO.GetOverrideConfig()
	if override == nil {
		override = toUpdate
	} else {
		override.Merge(toUpdate)
	}

	if !transient {
		if err = cmn.SaveOverrideConfig(clone.ConfigDir, override); err != nil {
			co.Unlock()
			return
		}
	}

	cmn.GCO.Put(clone)
	cmn.GCO.PutOverrideConfig(override)
	co.Unlock()
	return
}

func (co *configOwner) resetDaemonConfig() (err error) {
	co.Lock()
	oldConfig := cmn.GCO.Get()
	config, err := co.get()
	if err != nil {
		co.Unlock()
		return err
	}
	cmn.GCO.PutOverrideConfig(nil)
	err = cos.RemoveFile(path.Join(oldConfig.ConfigDir, cmn.OverrideConfigFname))
	if err != nil {
		co.Unlock()
		return
	}
	co.updateGCO(config)
	co.Unlock()
	return
}
