// Package integration_test.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// Note: Run these tests on both K8s and local.
// Minikube doesn't use TestingEnv which doesn't limit number of corner cases tested.

const errWMConfigNotExpected = "expected 'disk.disk_util_low_wm' to be %d, got: %d"

func TestConfig(t *testing.T) {
	var (
		highWM           = int32(80)
		lowWM            = int32(60)
		cleanupWM        = int32(55)
		updTime          = time.Second * 20
		configRegression = map[string]string{
			"periodic.stats_time":   updTime.String(),
			"space.cleanupwm":       strconv.Itoa(int(cleanupWM)),
			"space.lowwm":           strconv.Itoa(int(lowWM)),
			"space.highwm":          strconv.Itoa(int(highWM)),
			"lru.enabled":           "true",
			"lru.capacity_upd_time": updTime.String(),
			"lru.dont_evict_time":   updTime.String(),
		}
		oconfig      = tools.GetClusterConfig(t)
		ospaceconfig = oconfig.Space
		olruconfig   = oconfig.LRU
		operiodic    = oconfig.Periodic
	)
	defer tools.SetClusterConfig(t, cos.StrKVs{
		"periodic.stats_time":   oconfig.Periodic.StatsTime.String(),
		"space.cleanupwm":       strconv.Itoa(int(oconfig.Space.CleanupWM)),
		"space.lowwm":           strconv.Itoa(int(oconfig.Space.LowWM)),
		"space.highwm":          strconv.Itoa(int(oconfig.Space.HighWM)),
		"lru.enabled":           strconv.FormatBool(oconfig.LRU.Enabled),
		"lru.capacity_upd_time": oconfig.LRU.CapacityUpdTime.String(),
		"lru.dont_evict_time":   oconfig.LRU.DontEvictTime.String(),
	})

	tools.SetClusterConfig(t, configRegression)

	nconfig := tools.GetClusterConfig(t)
	nlruconfig := nconfig.LRU
	nspaceconfig := nconfig.Space
	nperiodic := nconfig.Periodic

	if v, _ := time.ParseDuration(configRegression["periodic.stats_time"]); nperiodic.StatsTime != cos.Duration(v) {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTime, configRegression["periodic.stats_time"])
	} else {
		o := operiodic.StatsTime
		tools.SetClusterConfig(t, cos.StrKVs{"periodic.stats_time": o.String()})
	}
	if v, _ := time.ParseDuration(configRegression["lru.dont_evict_time"]); nlruconfig.DontEvictTime != cos.Duration(v) {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTime, configRegression["lru.dont_evict_time"])
	} else {
		o := olruconfig.DontEvictTime
		tools.SetClusterConfig(t, cos.StrKVs{"lru.dont_evict_time": o.String()})
	}

	if v, _ := time.ParseDuration(configRegression["lru.capacity_upd_time"]); nlruconfig.CapacityUpdTime != cos.Duration(v) {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTime, configRegression["lru.capacity_upd_time"])
	} else {
		o := olruconfig.CapacityUpdTime
		tools.SetClusterConfig(t, cos.StrKVs{"lru.capacity_upd_time": o.String()})
	}
	if hw, err := strconv.Atoi(configRegression["space.highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nspaceconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nspaceconfig.HighWM, hw)
	} else {
		oldhwmStr, err := cos.ConvertToString(ospaceconfig.HighWM)
		if err != nil {
			t.Fatalf("Error parsing HighWM: %v", err)
		}
		tools.SetClusterConfig(t, cos.StrKVs{"space.highwm": oldhwmStr})
	}
	if lw, err := strconv.Atoi(configRegression["space.lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nspaceconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nspaceconfig.LowWM, lw)
	} else {
		oldlwmStr, err := cos.ConvertToString(ospaceconfig.LowWM)
		if err != nil {
			t.Fatalf("Error parsing LowWM: %v", err)
		}
		tools.SetClusterConfig(t, cos.StrKVs{"space.lowwm": oldlwmStr})
	}
	if pt, err := cos.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		tools.SetClusterConfig(t, cos.StrKVs{"lru.enabled": strconv.FormatBool(olruconfig.Enabled)})
	}
}

func TestConfigGet(t *testing.T) {
	smap := tools.GetClusterMap(t, tools.GetPrimaryURL())

	proxy, err := smap.GetRandProxy(false)
	tassert.CheckFatal(t, err)
	tools.GetDaemonConfig(t, proxy)

	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tools.GetDaemonConfig(t, target)
}

func TestConfigSetGlobal(t *testing.T) {
	var (
		ecCondition bool
		smap        = tools.GetClusterMap(t, tools.GetPrimaryURL())
		config      = tools.GetClusterConfig(t)
		check       = func(snode *meta.Snode, c *cmn.Config) {
			tassert.Errorf(t, c.EC.Enabled == ecCondition,
				"%s expected 'ec.enabled' to be %v, got %v", snode, ecCondition, c.EC.Enabled)
		}
	)
	ecCondition = !config.EC.Enabled
	toUpdate := &cmn.ConfigToSet{EC: &cmn.ECConfToSet{
		Enabled: apc.Ptr(ecCondition),
	}}

	tools.SetClusterConfigUsingMsg(t, toUpdate)
	checkConfig(t, smap, check)

	// Reset config
	ecCondition = config.EC.Enabled
	tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled": strconv.FormatBool(ecCondition),
	})
	checkConfig(t, smap, check)

	// wait for ec
	flt := xact.ArgsMsg{Kind: apc.ActECEncode}
	_, _ = api.WaitForXactionIC(baseParams, &flt)
}

func TestConfigFailOverrideClusterOnly(t *testing.T) {
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
		config     = tools.GetClusterConfig(t)
	)
	proxy, err := smap.GetRandProxy(false /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Try overriding cluster only config on a daemon
	err = api.SetDaemonConfig(baseParams, proxy.ID(), cos.StrKVs{"ec.enabled": strconv.FormatBool(!config.EC.Enabled)})
	tassert.Fatalf(t, err != nil, "expected error to occur when trying to override cluster only config")

	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.EC.Enabled == config.EC.Enabled,
		"expected 'ec.enabled' to be %v, got: %v", config.EC.Enabled, daemonConfig.EC.Enabled)

	// wait for ec
	flt := xact.ArgsMsg{Kind: apc.ActECEncode}
	_, _ = api.WaitForXactionIC(baseParams, &flt)
}

func TestConfigOverrideAndRestart(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL      = tools.GetPrimaryURL()
		baseParams    = tools.BaseAPIParams(proxyURL)
		smap          = tools.GetClusterMap(t, proxyURL)
		config        = tools.GetClusterConfig(t)
		origProxyCnt  = smap.CountActivePs()
		origTargetCnt = smap.CountActiveTs()
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override cluster config on the selected proxy
	newLowWM := config.Disk.DiskUtilLowWM - 10
	err = api.SetDaemonConfig(baseParams, proxy.ID(),
		cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(newLowWM, 10)})
	tassert.CheckFatal(t, err)

	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Restart and check that config persisted
	tlog.Logf("Killing %s\n", proxy.StringEx())
	cmd, err := tools.KillNode(proxy)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "proxy removed", smap.Version, origProxyCnt-1, origTargetCnt)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, false, apc.Proxy)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "proxy restored", smap.Version, origProxyCnt, origTargetCnt)
	tassert.CheckFatal(t, err)

	daemonConfig = tools.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Reset node config.
	err = api.SetDaemonConfig(baseParams, proxy.ID(),
		cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(config.Disk.DiskUtilLowWM, 10)})
	tassert.CheckFatal(t, err)
}

func TestConfigSyncToNewNode(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL      = tools.GetPrimaryURL()
		smap          = tools.GetClusterMap(t, proxyURL)
		config        = tools.GetClusterConfig(t)
		origProxyCnt  = smap.CountActivePs()
		origTargetCnt = smap.CountActiveTs()
	)
	// 1. Kill random non-primary
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	tlog.Logf("Killing %s\n", proxy.StringEx())
	cmd, err := tools.KillNode(proxy)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{
			"ec.enabled": strconv.FormatBool(config.EC.Enabled),
		})
	})

	smap, err = tools.WaitForClusterState(proxyURL, "proxy removed", smap.Version, origProxyCnt-1, origTargetCnt)
	tassert.CheckError(t, err)
	if err != nil || smap.Primary.ID() == proxy.ID() {
		time.Sleep(time.Second)
		_ = tools.RestoreNode(cmd, false, apc.Proxy)
		time.Sleep(time.Second)
		t.Fatalf("failed to kill %s, %s", proxy, smap.StringEx())
	}

	// 2. After proxy is killed, update cluster configuration
	newECEnabled := !config.EC.Enabled
	tlog.Logf("Globally changing ec.enabled to %t (%s)\n", newECEnabled, smap.StringEx())
	tools.SetClusterConfig(t, cos.StrKVs{
		"ec.enabled": strconv.FormatBool(newECEnabled),
	})

	// 3. Restart proxy
	err = tools.RestoreNode(cmd, false, apc.Proxy)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "proxy restored", smap.Version, origProxyCnt, origTargetCnt)
	tassert.CheckFatal(t, err)

	// 4. Ensure the proxy has lastest updated config
	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.EC.Enabled == newECEnabled,
		"expected 'ec.Enabled' to be %v, got: %v", newECEnabled, daemonConfig.EC.Enabled)

	// wait for ec
	flt := xact.ArgsMsg{Kind: apc.ActECEncode}
	_, _ = api.WaitForXactionIC(baseParams, &flt)
}

func checkConfig(t *testing.T, smap *meta.Smap, check func(*meta.Snode, *cmn.Config)) {
	for _, node := range smap.Pmap {
		config := tools.GetDaemonConfig(t, node)
		check(node, config)
	}
	for _, node := range smap.Tmap {
		config := tools.GetDaemonConfig(t, node)
		check(node, config)
	}
}

func TestConfigOverrideAndResetDaemon(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL   = tools.GetPrimaryURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
		config     = tools.GetClusterConfig(t)
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override a cluster config on daemon
	newLowWM := config.Disk.DiskUtilLowWM - 10
	err = api.SetDaemonConfig(baseParams, proxy.ID(),
		cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(newLowWM, 10)})
	tassert.CheckFatal(t, err)

	daemonConfig := tools.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Reset daemon and check if the override is gone.
	err = api.ResetDaemonConfig(baseParams, proxy.ID())
	tassert.CheckFatal(t, err)
	daemonConfig = tools.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == config.Disk.DiskUtilLowWM,
		errWMConfigNotExpected, config.Disk.DiskUtilLowWM, daemonConfig.Disk.DiskUtilLowWM)
}

func TestConfigOverrideAndResetCluster(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})
	var (
		daemonConfig *cmn.Config
		proxyURL     = tools.GetPrimaryURL()
		baseParams   = tools.BaseAPIParams(proxyURL)
		smap         = tools.GetClusterMap(t, proxyURL)
		config       = tools.GetClusterConfig(t)
		newLowWM     = config.Disk.DiskUtilLowWM - 10
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override a cluster config on daemon and primary
	primary, err := tools.GetPrimaryProxy(proxyURL)
	tassert.CheckFatal(t, err)
	for _, node := range []*meta.Snode{primary, proxy} {
		err = api.SetDaemonConfig(baseParams, node.ID(),
			cos.StrKVs{"disk.disk_util_low_wm": strconv.FormatInt(newLowWM, 10)})
		tassert.CheckFatal(t, err)

		daemonConfig = tools.GetDaemonConfig(t, node)
		tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
			errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)
	}

	// Reset all daemons and check if the override is gone.
	err = api.ResetClusterConfig(baseParams)
	tassert.CheckFatal(t, err)
	for _, node := range []*meta.Snode{primary, proxy} {
		daemonConfig = tools.GetDaemonConfig(t, node)
		tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == config.Disk.DiskUtilLowWM,
			errWMConfigNotExpected, config.Disk.DiskUtilLowWM, daemonConfig.Disk.DiskUtilLowWM)
	}
}

func TestConfigRebalance(t *testing.T) {
	var (
		oRebalance = tools.GetClusterConfig(t).Rebalance.Enabled
	)
	defer tools.SetClusterConfig(t, cos.StrKVs{
		"rebalance.enabled": strconv.FormatBool(oRebalance),
	})

	tools.EnableRebalance(t)

	nRebalance := tools.GetClusterConfig(t).Rebalance.Enabled

	if !nRebalance {
		t.Errorf("Rebalance was not enabled: current value %v, should be: %v", nRebalance, true)
	}
	tools.DisableRebalance(t)

	nRebalance = tools.GetClusterConfig(t).Rebalance.Enabled
	if nRebalance {
		t.Errorf("Rebalance was not disabled: current value %v, should be: %v", nRebalance, false)
	}
}
