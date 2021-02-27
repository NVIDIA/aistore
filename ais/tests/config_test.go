// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

// Note: Run these tests on both K8s and local.
// Minikube doesn't use TestingEnv which doesn't limit number of corner cases tested.

func TestConfig(t *testing.T) {
	oconfig := tutils.GetClusterConfig(t)
	olruconfig := oconfig.LRU
	operiodic := oconfig.Periodic

	tutils.SetClusterConfig(t, configRegression)

	nconfig := tutils.GetClusterConfig(t)
	nlruconfig := nconfig.LRU
	nperiodic := nconfig.Periodic

	if nperiodic.StatsTimeStr != configRegression["periodic.stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTimeStr, configRegression["periodic.stats_time"])
	} else {
		o := operiodic.StatsTimeStr
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"periodic.stats_time": o})
	}
	if nlruconfig.DontEvictTimeStr != configRegression["lru.dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTimeStr, configRegression["lru.dont_evict_time"])
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.dont_evict_time": olruconfig.DontEvictTimeStr})
	}
	if nlruconfig.CapacityUpdTimeStr != configRegression["lru.capacity_upd_time"] {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTimeStr, configRegression["lru.capacity_upd_time"])
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.capacity_upd_time": olruconfig.CapacityUpdTimeStr})
	}
	if hw, err := strconv.Atoi(configRegression["lru.highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nlruconfig.HighWM, hw)
	} else {
		oldhwmStr, err := cmn.ConvertToString(olruconfig.HighWM)
		if err != nil {
			t.Fatalf("Error parsing HighWM: %v", err)
		}
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.highwm": oldhwmStr})
	}
	if lw, err := strconv.Atoi(configRegression["lru.lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nlruconfig.LowWM, lw)
	} else {
		oldlwmStr, err := cmn.ConvertToString(olruconfig.LowWM)
		if err != nil {
			t.Fatalf("Error parsing LowWM: %v", err)
		}
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.lowwm": oldlwmStr})
	}
	if pt, err := cmn.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.enabled": fmt.Sprintf("%v", olruconfig.Enabled)})
	}
}

func TestConfigGet(t *testing.T) {
	smap := tutils.GetClusterMap(t, tutils.GetPrimaryURL())

	proxy, err := smap.GetRandProxy(false)
	tassert.CheckFatal(t, err)
	tutils.GetDaemonConfig(t, proxy)

	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tutils.GetDaemonConfig(t, target)
}
