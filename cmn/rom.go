// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/feat"
)

// read-mostly and most often used config values: assigned at startup and updated
// when cluster config changes to reduce the number of GCO.Get() calls

type readMostly struct {
	timeout struct {
		cplane    time.Duration // Config.Timeout.CplaneOperation
		keepalive time.Duration // MaxKeepalive
		ecstreams time.Duration // EcStreams
	}
	features       feat.Flags
	level, modules int
	testingEnv     bool
	authEnabled    bool
	cskEnabled     bool
}

var Rom readMostly

func (rom *readMostly) init() {
	rom.timeout.cplane = time.Second + time.Millisecond
	rom.timeout.keepalive = 2*time.Second + time.Millisecond
	rom.timeout.ecstreams = SharedStreamsDflt
}

func (rom *readMostly) Set(cfg *ClusterConfig) {
	rom.timeout.cplane = cfg.Timeout.CplaneOperation.D()
	rom.timeout.keepalive = cfg.Timeout.MaxKeepalive.D()
	if d := cfg.Timeout.EcStreams; d != 0 {
		rom.timeout.ecstreams = d.D()
	}
	rom.features = cfg.Features

	rom.authEnabled = cfg.Auth.Enabled
	rom.cskEnabled = cfg.Auth.CSKEnabled()

	// pre-parse for V (below)
	rom.level, rom.modules = cfg.Log.Level.Parse()
}

func (rom *readMostly) CplaneOperation() time.Duration { return rom.timeout.cplane }
func (rom *readMostly) MaxKeepalive() time.Duration    { return rom.timeout.keepalive }
func (rom *readMostly) EcStreams() time.Duration       { return rom.timeout.ecstreams }
func (rom *readMostly) Features() feat.Flags           { return rom.features }
func (rom *readMostly) TestingEnv() bool               { return rom.testingEnv }
func (rom *readMostly) AuthEnabled() bool              { return rom.authEnabled }
func (rom *readMostly) CSKEnabled() bool               { return rom.cskEnabled }

func (rom *readMostly) V(verbosity, fl int) bool {
	return rom.level >= verbosity || rom.modules&fl != 0
}
