// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

// Note: Run these tests on both K8s and local.
// Minikube doesn't use TestingEnv which doesn't limit number of corner cases tested.

const errWMConfigNotExpected = "expected 'disk.disk_util_low_wm' to be %d, got: %d"

func TestConfig(t *testing.T) {
	oconfig := tutils.GetClusterConfig(t)
	olruconfig := oconfig.LRU
	operiodic := oconfig.Periodic

	tutils.SetClusterConfig(t, configRegression)

	nconfig := tutils.GetClusterConfig(t)
	nlruconfig := nconfig.LRU
	nperiodic := nconfig.Periodic

	if v, _ := time.ParseDuration(configRegression["periodic.stats_time"]); nperiodic.StatsTime != cos.Duration(v) {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTime, configRegression["periodic.stats_time"])
	} else {
		o := operiodic.StatsTime
		tutils.SetClusterConfig(t, cos.SimpleKVs{"periodic.stats_time": o.String()})
	}
	if v, _ := time.ParseDuration(configRegression["lru.dont_evict_time"]); nlruconfig.DontEvictTime != cos.Duration(v) {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTime, configRegression["lru.dont_evict_time"])
	} else {
		o := olruconfig.DontEvictTime
		tutils.SetClusterConfig(t, cos.SimpleKVs{"lru.dont_evict_time": o.String()})
	}

	if v, _ := time.ParseDuration(configRegression["lru.capacity_upd_time"]); nlruconfig.CapacityUpdTime != cos.Duration(v) {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTime, configRegression["lru.capacity_upd_time"])
	} else {
		o := olruconfig.CapacityUpdTime
		tutils.SetClusterConfig(t, cos.SimpleKVs{"lru.capacity_upd_time": o.String()})
	}
	if hw, err := strconv.Atoi(configRegression["lru.highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nlruconfig.HighWM, hw)
	} else {
		oldhwmStr, err := cos.ConvertToString(olruconfig.HighWM)
		if err != nil {
			t.Fatalf("Error parsing HighWM: %v", err)
		}
		tutils.SetClusterConfig(t, cos.SimpleKVs{"lru.highwm": oldhwmStr})
	}
	if lw, err := strconv.Atoi(configRegression["lru.lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nlruconfig.LowWM, lw)
	} else {
		oldlwmStr, err := cos.ConvertToString(olruconfig.LowWM)
		if err != nil {
			t.Fatalf("Error parsing LowWM: %v", err)
		}
		tutils.SetClusterConfig(t, cos.SimpleKVs{"lru.lowwm": oldlwmStr})
	}
	if pt, err := cos.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		tutils.SetClusterConfig(t, cos.SimpleKVs{"lru.enabled": fmt.Sprintf("%v", olruconfig.Enabled)})
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

func TestConfigSetGlobal(t *testing.T) {
	var (
		ecCondition bool
		smap        = tutils.GetClusterMap(t, tutils.GetPrimaryURL())
		config      = tutils.GetClusterConfig(t)
		check       = func(snode *cluster.Snode, c *cmn.Config) {
			tassert.Errorf(t, c.EC.Enabled == ecCondition,
				"%s expected 'ec.enabled' to be %v, got %v", snode, ecCondition, c.EC.Enabled)
		}
	)
	ecCondition = !config.EC.Enabled
	toUpdate := &cmn.ConfigToUpdate{EC: &cmn.ECConfToUpdate{
		Enabled: api.Bool(ecCondition),
	}}

	tutils.SetClusterConfigUsingMsg(t, toUpdate)
	checkConfig(t, smap, check)

	// Reset config
	ecCondition = config.EC.Enabled
	tutils.SetClusterConfig(t, cos.SimpleKVs{
		"ec.enabled": strconv.FormatBool(ecCondition),
	})
	checkConfig(t, smap, check)
}

func TestConfigFailOverrideClusterOnly(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
		config     = tutils.GetClusterConfig(t)
	)
	proxy, err := smap.GetRandProxy(false /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Try overriding cluster only config on a daemon
	err = api.SetDaemonConfig(baseParams, proxy.DaemonID, cos.SimpleKVs{
		"ec.enabled": strconv.FormatBool(!config.EC.Enabled),
	})
	tassert.Fatalf(t, err != nil, "expected error to occur when trying to override cluster only config")

	daemonConfig := tutils.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.EC.Enabled == config.EC.Enabled,
		"expected 'ec.enabled' to be %v, got: %v", config.EC.Enabled, daemonConfig.EC.Enabled)
}

func TestConfigOverrideAndRestart(t *testing.T) {
	t.Skipf("skipping %s", t.Name()) // TODO -- FIXME: revise and enable
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL      = tutils.GetPrimaryURL()
		baseParams    = tutils.BaseAPIParams(proxyURL)
		smap          = tutils.GetClusterMap(t, proxyURL)
		config        = tutils.GetClusterConfig(t)
		origProxyCnt  = smap.CountActiveProxies()
		origTargetCnt = smap.CountActiveTargets()
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override cluster config on the selected proxy
	newLowWM := config.Disk.DiskUtilLowWM - 10
	err = api.SetDaemonConfig(baseParams, proxy.DaemonID, cos.SimpleKVs{
		"disk.disk_util_low_wm": fmt.Sprintf("%d", newLowWM),
	})
	tassert.CheckFatal(t, err)

	daemonConfig := tutils.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Restart and check that config persisted
	tlog.Logf("Killing %s\n", proxy.StringEx())
	cmd, err := tutils.KillNode(proxy)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForClusterState(proxyURL, "proxy removed", smap.Version, origProxyCnt-1, origTargetCnt)
	tassert.CheckError(t, err)

	err = tutils.RestoreNode(cmd, false, cmn.Proxy)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(proxyURL, "proxy restored", smap.Version, origProxyCnt, origTargetCnt)
	tassert.CheckFatal(t, err)

	tlog.Logf("Wait for rebalance\n")
	args := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)

	daemonConfig = tutils.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Reset node config.
	err = api.SetDaemonConfig(baseParams, proxy.DaemonID, cos.SimpleKVs{
		"disk.disk_util_low_wm": fmt.Sprintf("%d", config.Disk.DiskUtilLowWM),
	})
	tassert.CheckFatal(t, err)
}

func TestConfigSyncToNewNode(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL      = tutils.GetPrimaryURL()
		smap          = tutils.GetClusterMap(t, proxyURL)
		config        = tutils.GetClusterConfig(t)
		origProxyCnt  = smap.CountActiveProxies()
		origTargetCnt = smap.CountActiveTargets()
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// 1. Kill a random proxy
	tlog.Logf("Killing %s\n", proxy.StringEx())
	cmd, err := tutils.KillNode(proxy)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		tutils.SetClusterConfig(t, cos.SimpleKVs{
			"ec.enabled": strconv.FormatBool(config.EC.Enabled),
		})
	})

	smap, err = tutils.WaitForClusterState(proxyURL, "proxy removed", smap.Version, origProxyCnt-1, origTargetCnt)
	tassert.CheckError(t, err)

	// 2. After proxy is killed, update cluster configuration
	newECEnabled := !config.EC.Enabled
	tutils.SetClusterConfig(t, cos.SimpleKVs{
		"ec.enabled": strconv.FormatBool(newECEnabled),
	})

	// 3. Restart proxy
	err = tutils.RestoreNode(cmd, false, cmn.Proxy)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(proxyURL, "proxy restored", smap.Version, origProxyCnt, origTargetCnt)
	tassert.CheckFatal(t, err)

	tlog.Logf("Wait for rebalance\n")
	args := api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXaction(baseParams, args)

	// 4. Ensure the proxy has lastest updated config
	daemonConfig := tutils.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.EC.Enabled == newECEnabled,
		"expected 'ec.Enabled' to be %v, got: %v", newECEnabled, daemonConfig.EC.Enabled)
}

func checkConfig(t *testing.T, smap *cluster.Smap, check func(*cluster.Snode, *cmn.Config)) {
	for _, node := range smap.Pmap {
		config := tutils.GetDaemonConfig(t, node)
		check(node, config)
	}
	for _, node := range smap.Tmap {
		config := tutils.GetDaemonConfig(t, node)
		check(node, config)
	}
}

func TestConfigOverrideAndResetDaemon(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal, MinProxies: 2})
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
		config     = tutils.GetClusterConfig(t)
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override a cluster config on daemon
	newLowWM := config.Disk.DiskUtilLowWM - 10
	err = api.SetDaemonConfig(baseParams, proxy.DaemonID, cos.SimpleKVs{
		"disk.disk_util_low_wm": fmt.Sprintf("%d", newLowWM),
	})
	tassert.CheckFatal(t, err)

	daemonConfig := tutils.GetDaemonConfig(t, proxy)
	tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
		errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)

	// Reset daemon and check if the override is gone.
	err = api.ResetDaemonConfig(baseParams, proxy.DaemonID)
	tassert.CheckFatal(t, err)
	daemonConfig = tutils.GetDaemonConfig(t, proxy)
	tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == config.Disk.DiskUtilLowWM,
		errWMConfigNotExpected, config.Disk.DiskUtilLowWM, daemonConfig.Disk.DiskUtilLowWM)
}

func TestConfigOverrideAndResetCluster(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal, MinProxies: 2})
	var (
		daemonConfig *cmn.Config
		proxyURL     = tutils.GetPrimaryURL()
		baseParams   = tutils.BaseAPIParams(proxyURL)
		smap         = tutils.GetClusterMap(t, proxyURL)
		config       = tutils.GetClusterConfig(t)
		newLowWM     = config.Disk.DiskUtilLowWM - 10
	)
	proxy, err := smap.GetRandProxy(true /*exclude primary*/)
	tassert.CheckFatal(t, err)

	// Override a cluster config on daemon and primary
	primary, err := tutils.GetPrimaryProxy(proxyURL)
	tassert.CheckFatal(t, err)
	for _, node := range []*cluster.Snode{primary, proxy} {
		err = api.SetDaemonConfig(baseParams, node.DaemonID, cos.SimpleKVs{
			"disk.disk_util_low_wm": fmt.Sprintf("%d", newLowWM),
		})
		tassert.CheckFatal(t, err)

		daemonConfig = tutils.GetDaemonConfig(t, node)
		tassert.Errorf(t, daemonConfig.Disk.DiskUtilLowWM == newLowWM,
			errWMConfigNotExpected, newLowWM, daemonConfig.Disk.DiskUtilLowWM)
	}

	// Reset all daemons and check if the override is gone.
	err = api.ResetClusterConfig(baseParams)
	tassert.CheckFatal(t, err)
	for _, node := range []*cluster.Snode{primary, proxy} {
		daemonConfig = tutils.GetDaemonConfig(t, node)
		tassert.Fatalf(t, daemonConfig.Disk.DiskUtilLowWM == config.Disk.DiskUtilLowWM,
			errWMConfigNotExpected, config.Disk.DiskUtilLowWM, daemonConfig.Disk.DiskUtilLowWM)
	}
}
