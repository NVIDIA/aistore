// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"reflect"
	"sync"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

// NOTE [backward compatibility] ==================================================================================
//
// When pointerizing additional sections:
// - update the corresponding <section>.Validate() to normalize zero/unset fields to their canonical defaults;
// - review/skip fields where zero has an intentional user-visible meaning such as "disabled" or "" (for "none", etc).
// - implement `defaultOmittable()` interface
//
// Release notes for the intervening TBD releases (v5.0, v5.1) must carry a disclaimer.
//
// Note that ensureDefaults() keeps in-memory sections non-nil; PruneOmittables()
// strips all-default sections at encode time - see ais/gconfig.go `_encode`.
// Join/handshake `cluMeta` and apc.WhatNodeConfig/apc.WhatClusterConfig queries stay fully populated (not sparse).
// =================================================================================================================

func (gco *gco) Clone() *Config {
	src := gco.Get()
	dst := &Config{}
	cos.CopyStruct(dst, src) // shallow-copy

	// clone assorted pointers to structs
	src.Auth.CopyTo(&dst.Auth)

	dst.clonePtrs() // deep-copy

	return dst
}

// deep-copy pointerized sections (to break aliasing)
func (c *ClusterConfig) clonePtrs() {
	// optional configuration
	if c.Tracing != nil {
		v := *c.Tracing
		c.Tracing = &v
	}
	if pub := c.Net.HTTP.Pub; pub != nil {
		v := *pub
		c.Net.HTTP.Pub = &v
	}

	// default-omittable sections
	c.rangeDefaultOmittable(func(field reflect.Value) {
		if field.IsNil() {
			return
		}
		clone := reflect.New(field.Type().Elem())
		clone.Elem().Set(field.Elem())
		field.Set(clone)
	})
}

func (c *ClusterConfig) rangeDefaultOmittable(visit func(reflect.Value)) {
	v := reflect.ValueOf(c).Elem()

	for i := range v.NumField() {
		field := v.Field(i)
		if field.Kind() != reflect.Pointer || !field.CanInterface() {
			continue
		}
		if _, ok := field.Interface().(defaultOmittable); ok {
			visit(field)
		}
	}
}

// Materialize default-omittable sections before section validation.
// Runtime consumers may therefore dereference them unconditionally.
func (c *ClusterConfig) ensureDefaults() {
	c.rangeDefaultOmittable(func(field reflect.Value) {
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
	})
}

// PruneOmittables converts cluster config to its persistence/metasync sparse form.
// The caller must invoke it only on a private copy (shallow is fine), never on the live config.
//
// A default-omittable section is removed when its validated value equals the
// canonical defaults produced by validating a zero value. Any non-default
// section remains unchanged, including its zero-valued fields.
//
// Configuration GET APIs (apc.WhatClusterConfig, apc.WhatNodeConfig)
// deliberately return the fully materialized effective configuration, so
// clients need not resolve server-side defaults. The asymmetry is intentional:
// do not make those paths sparse.
//
// See also: ensureDefaults and clonePtrs; all three are driven by defaultOmittable
// marker interface.
func (c *ClusterConfig) PruneOmittables() {
	c.rangeDefaultOmittable(func(field reflect.Value) {
		if field.IsNil() {
			return
		}

		curr := reflect.New(field.Type().Elem())
		curr.Elem().Set(field.Elem())
		if err := curr.Interface().(defaultOmittable).Validate(); err != nil {
			debug.AssertNoErr(err)
			return
		}

		dflt := reflect.New(field.Type().Elem())
		if err := dflt.Interface().(defaultOmittable).Validate(); err != nil {
			debug.AssertNoErr(err)
			return
		}

		if reflect.DeepEqual(curr.Elem().Interface(), dflt.Elem().Interface()) {
			field.SetZero()
		}
	})
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
	// copy
	config := gco.Clone()
	config.ClusterConfig = *cluConfig
	config.ClusterConfig.clonePtrs()

	// post-4.1 fixup: pointer and `omitempty`
	if config.Tracing != nil {
		if !config.Tracing.Enabled && config.Tracing.ExporterEndpoint == "" {
			config.Tracing = nil
		}
	}
	// post-4.2 fixup: dsort is conditionally linked ("go:build dsort")
	dropDsortConfig(config)

	override := gco.GetOverride()
	if override != nil {
		err = config.UpdateClusterConfig(override, apc.Daemon, CopyPropsOpts{Transient: false, IgnoreScope: true}) // update and validate
	} else {
		err = config.Validate()
	}
	if err != nil {
		return err
	}
	gco.Put(config)
	return nil
}
