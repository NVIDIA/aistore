// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"os"
	"path"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

type (
	globalConf struct {
		cmn.Config
	}
	configOwner struct {
		sync.Mutex
		cmd        atomic.Pointer // pointer to globalConf
		daemonType string
	}
)

var _ revs = (*globalConf)(nil)

func (r *globalConf) tag() string     { return revsConfTag }
func (r *globalConf) version() int64  { return r.Version }
func (r *globalConf) marshal() []byte { return cmn.MustLocalMarshal(r) }

////////////
// config //
////////////

func newConfOwner(daemonType string) *configOwner {
	return &configOwner{daemonType: daemonType}
}

func (co *configOwner) get() *globalConf {
	return (*globalConf)(co.cmd.Load())
}

func (co *configOwner) put(cmd *globalConf) {
	cmd.SetRole(co.daemonType)
	co.cmd.Store(unsafe.Pointer(cmd))
}

func cfgBeginUpdate() *cmn.Config { return cmn.GCO.BeginUpdate() }
func cfgDiscardUpdate()           { cmn.GCO.DiscardUpdate() }
func cfgCommitUpdate(config *cmn.Config, detail string) (err error) {
	if err = jsp.SaveConfig(config); err != nil {
		cmn.GCO.DiscardUpdate()
		return fmt.Errorf("FATAL: failed writing config %s: %s, %v", cmn.GCO.GetGlobalConfigPath(), detail, err)
	}
	cmn.GCO.CommitUpdate(config)
	return
}

// TODO: make it similar to other owner modifiers; ensure CoW
// For now, we only support updating the global configuration - only done by primary.
func (co *configOwner) modify(toUpdate *cmn.ConfigToUpdate, detail string) error {
	co.Lock()
	defer co.Unlock()
	conf := co.get()

	err := jsp.SetConfigInMem(toUpdate, &conf.Config)
	if err != nil {
		return err
	}
	conf.Version++
	if err = co.persist(conf); err != nil {
		return fmt.Errorf("FATAL: failed persist config for %q, err: %v", detail, err)
	}
	co.updateGCO()
	return nil
}

func (co *configOwner) persist(conf *globalConf) error {
	co.put(conf)
	local := cmn.GCO.GetLocal()
	savePath := path.Join(local.ConfigDir, gconfFname)
	if err := jsp.Save(savePath, conf, jsp.PlainLocal()); err != nil {
		return err
	}
	return nil
}

func (co *configOwner) updateGCO() (err error) {
	cmn.GCO.BeginUpdate()
	conf := co.get()
	conf.SetLocalConf(cmn.GCO.GetLocal())
	if err = conf.Validate(); err != nil {
		return
	}
	cmn.GCO.CommitUpdate(&conf.Config)
	return
}

func (co *configOwner) load() (err error) {
	localConf := cmn.GCO.GetLocal()
	gconf := &globalConf{}
	_, err = jsp.Load(path.Join(localConf.ConfigDir, gconfFname), gconf, jsp.Plain())
	if err == nil {
		co.put(gconf)
		return co.updateGCO()
	}
	if !os.IsNotExist(err) {
		return
	}
	// If gconf file is missing, assume conf provided through CLI as global.
	// NOTE: We cannot use GCO.Get() here as cmn.GCO may also contain custom config.
	conf := &globalConf{}
	_, err = jsp.Load(cmn.GCO.GetGlobalConfigPath(), conf, jsp.Plain())
	if err != nil {
		return
	}
	co.put(conf)
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
