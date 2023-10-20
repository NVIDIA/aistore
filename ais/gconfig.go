// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"
)

type (
	globalConfig struct {
		_sgl *memsys.SGL
		cmn.ClusterConfig
	}
	configOwner struct {
		globalFpath string
		immSize     int64
		sync.Mutex
	}

	configModifier struct {
		pre   func(ctx *configModifier, clone *globalConfig) (updated bool, err error)
		final func(ctx *configModifier, clone *globalConfig)

		oldConfig *cmn.Config
		toUpdate  *cmn.ConfigToSet
		msg       *apc.ActMsg
		query     url.Values
		hdr       http.Header
		wait      bool
	}
)

// interface guard
var _ revs = (*globalConfig)(nil)

// as revs
func (*globalConfig) tag() string           { return revsConfTag }
func (config *globalConfig) version() int64 { return config.Version }
func (*globalConfig) jit(p *proxy) revs     { g, _ := p.owner.config.get(); return g }

func (config *globalConfig) sgl() *memsys.SGL {
	if config._sgl.IsNil() {
		return nil
	}
	return config._sgl
}

func (config *globalConfig) marshal() []byte {
	config._sgl = config._encode(0)
	return config._sgl.Bytes()
}

func (config *globalConfig) _encode(immSize int64) (sgl *memsys.SGL) {
	sgl = memsys.PageMM().NewSGL(immSize)
	err := jsp.Encode(sgl, config, config.JspOpts())
	debug.AssertNoErr(err)
	return
}

/////////////////
// configOwner //
/////////////////

func newConfigOwner(config *cmn.Config) (co *configOwner) {
	co = &configOwner{}
	co.globalFpath = filepath.Join(config.ConfigDir, fname.GlobalConfig)
	return
}

// NOTE:
// iff user ever executed transient config updates this loaded
// from disk version will differ from in-memory cmn.GCO.Get()
// (with respect to those in-memory only updated values)
// See also:
// - api.SetClusterConfig
// - apc.ActTransient
func (co *configOwner) get() (clone *globalConfig, err error) {
	clone = &globalConfig{}
	if _, err = jsp.LoadMeta(co.globalFpath, clone); err == nil {
		return
	}
	clone = nil
	if os.IsNotExist(err) {
		err = nil
	} else {
		nlog.Errorf("failed to load global config from %s: %v", co.globalFpath, err)
	}
	return
}

func (*configOwner) version() int64 { return cmn.GCO.Get().Version }

func (co *configOwner) runPre(ctx *configModifier) (clone *globalConfig, err error) {
	co.Lock()
	defer co.Unlock()
	clone, err = co.get()
	if err != nil {
		return
	}
	// NOTE: nil config == missing config
	if clone == nil {
		clone = &globalConfig{}
		_, err = jsp.Load(cmn.GCO.GetInitialGconfPath(), clone, jsp.Plain())
		if err != nil {
			return
		}
	}

	if updated, err := ctx.pre(ctx, clone); err != nil || !updated {
		return nil, err
	}

	ctx.oldConfig = cmn.GCO.Get()
	if err = cmn.GCO.Update(&clone.ClusterConfig); err != nil {
		clone = nil
		return
	}

	clone.Version++
	clone.LastUpdated = time.Now().String()
	clone._sgl = clone._encode(co.immSize)
	co.immSize = max(co.immSize, clone._sgl.Len())
	if err := co.persist(clone, nil); err != nil {
		clone._sgl.Free()
		clone._sgl = nil
		return nil, cmn.NewErrFailedTo(nil, "persist", clone, err)
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

func (co *configOwner) persist(clone *globalConfig, payload msPayload) error {
	if co.persistBytes(payload, co.globalFpath) {
		return nil
	}
	var wto io.WriterTo
	if clone._sgl != nil {
		wto = clone._sgl
	} else {
		sgl := clone._encode(co.immSize)
		co.immSize = max(co.immSize, sgl.Len())
		defer sgl.Free()
		wto = sgl
	}
	return jsp.SaveMeta(co.globalFpath, clone, wto)
}

func (*configOwner) persistBytes(payload msPayload, globalFpath string) (done bool) {
	if payload == nil {
		return
	}
	confValue := payload[revsConfTag]
	if confValue == nil {
		return
	}
	var (
		config globalConfig
		wto    = bytes.NewBuffer(confValue)
		err    = jsp.SaveMeta(globalFpath, &config, wto)
	)
	done = err == nil
	return
}

func (co *configOwner) setDaemonConfig(toUpdate *cmn.ConfigToSet, transient bool) (err error) {
	co.Lock()
	clone := cmn.GCO.Clone()
	err = setConfigInMem(toUpdate, clone, apc.Daemon)
	if err != nil {
		co.Unlock()
		return
	}

	override := cmn.GCO.GetOverride()
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
	cmn.GCO.PutOverride(override)
	co.Unlock()
	return
}

func setConfigInMem(toUpdate *cmn.ConfigToSet, config *cmn.Config, asType string) (err error) {
	err = config.UpdateClusterConfig(toUpdate, asType)
	return
}

func (co *configOwner) resetDaemonConfig() (err error) {
	co.Lock()
	oldConfig := cmn.GCO.Get()
	config, err := co.get()
	if err != nil || config == nil {
		co.Unlock()
		nlog.Infof("Warning: reset config %s: %v", oldConfig, err)
		return err
	}
	cmn.GCO.PutOverride(nil)
	err = cos.RemoveFile(filepath.Join(oldConfig.ConfigDir, fname.OverrideConfig))
	if err != nil {
		co.Unlock()
		return
	}
	cmn.GCO.Update(&config.ClusterConfig)
	co.Unlock()
	return
}
