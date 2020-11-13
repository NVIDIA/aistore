// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

type nodesCnt int

func (n nodesCnt) satisfied(actual int) bool {
	if n == 0 {
		return true
	}
	return int(n) == actual
}

func JoinCluster(proxyURL string, node *cluster.Snode) error {
	return devtools.JoinCluster(devtoolsCtx, proxyURL, node, registerTimeout)
}

// TODO: There is duplication between `UnregisterNode` and `RemoveTarget` - when to use which?
func RemoveTarget(t *testing.T, proxyURL string, smap *cluster.Smap) (*cluster.Smap, *cluster.Snode) {
	var (
		removeTarget, _ = smap.GetRandTarget()
		origTgtCnt      = smap.CountActiveTargets()
		args            = &cmn.ActValDecommision{DaemonID: removeTarget.ID(), SkipRebalance: true}
	)
	Logf("Removing target %s from %s\n", removeTarget.ID(), smap)

	err := UnregisterNode(proxyURL, args)
	tassert.CheckFatal(t, err)
	newSmap, err := WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountActiveProxies(),
		origTgtCnt-1,
	)
	tassert.CheckFatal(t, err)
	newTgtCnt := newSmap.CountActiveTargets()
	tassert.Fatalf(t, newTgtCnt == origTgtCnt-1,
		"new smap expected to have 1 target less: %d (v%d) vs %d (v%d)", newTgtCnt, origTgtCnt,
		newSmap.Version, smap.Version)

	return newSmap, removeTarget
}

// TODO: There is duplication between `JoinCluster` and `RestoreTarget` - when to use which?
func RestoreTarget(t *testing.T, proxyURL string, target *cluster.Snode) (newSmap *cluster.Smap) {
	smap := GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, smap.GetTarget(target.DaemonID) == nil, "unexpected target %s in smap", target.ID())
	Logf("Reregistering target %s, current Smap: %s\n", target, smap.StringEx())
	err := JoinCluster(proxyURL, target)
	tassert.CheckFatal(t, err)
	newSmap, err = WaitForClusterState(
		proxyURL,
		"to join target back",
		smap.Version,
		smap.CountActiveProxies(),
		smap.CountActiveTargets()+1,
	)
	tassert.CheckFatal(t, err)
	return newSmap
}

func ClearMaintenance(baseParams api.BaseParams, tsi *cluster.Snode) {
	val := &cmn.ActValDecommision{DaemonID: tsi.ID(), SkipRebalance: true}
	// it can fail if the node is not under maintenance but it is OK
	_, _ = api.StopMaintenance(baseParams, val)
}

func RandomProxyURL(ts ...*testing.T) (url string) {
	var (
		baseParams = BaseAPIParams(proxyURLReadOnly)
		smap, err  = waitForStartup(baseParams, ts...)
		retries    = 3
	)
	if err == nil {
		return _getRandomProxyURL(smap)
	}
	for _, node := range pmapReadOnly {
		url := node.URL(cmn.NetworkPublic)
		if url == proxyURLReadOnly {
			continue
		}
		if retries == 0 {
			return ""
		}
		baseParams = BaseAPIParams(url)
		if smap, err = waitForStartup(baseParams, ts...); err == nil {
			return _getRandomProxyURL(smap)
		}
		retries--
	}
	return ""
}

func _getRandomProxyURL(smap *cluster.Smap) string {
	proxies := smap.Pmap.ActiveNodes()
	return proxies[rand.Intn(len(proxies))].URL(cmn.NetworkPublic)
}

// WaitForClusterState waits until a cluster reaches specified state, meaning:
// - smap has version larger than origVersion
// - number of proxies is equal proxyCnt, unless proxyCnt == 0
// - number of targets is equal targetCnt, unless targetCnt == 0.
//
// It returns the smap which satisfies those requirements.
// NOTE: Upon successful return from this function cluster state might have already changed.
func WaitForClusterState(proxyURL, reason string, origVersion int64, proxyCnt, targetCnt int) (*cluster.Smap, error) {
	var (
		lastVersion                               int64
		smapChangeDeadline, timeStart, opDeadline time.Time

		expPrx = nodesCnt(proxyCnt)
		expTgt = nodesCnt(targetCnt)
	)

	timeStart = time.Now()
	smapChangeDeadline = timeStart.Add(2 * proxyChangeLatency)
	opDeadline = timeStart.Add(3 * proxyChangeLatency)

	Logf("Waiting for cluster state = (p=%d, t=%d, version > v%d) %s\n", expPrx, expTgt, origVersion, reason)

	var (
		loopCnt    int
		satisfied  bool
		baseParams = BaseAPIParams(proxyURL)
	)

	// Repeat until success or timeout.
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil {
			if !cmn.IsErrConnectionRefused(err) {
				return nil, err
			}
			Logf("%v\n", err)
			goto next
		}

		satisfied = expTgt.satisfied(smap.CountActiveTargets()) &&
			expPrx.satisfied(smap.CountActiveProxies()) &&
			smap.Version > origVersion
		if !satisfied {
			d := time.Since(timeStart)
			Logf("Still waiting at %s, current %s, elapsed (%s)\n", proxyURL, smap, d.Truncate(time.Second))
		}

		if smap.Version != lastVersion {
			smapChangeDeadline = cmn.MinTime(time.Now().Add(proxyChangeLatency), opDeadline)
		}

		// if the primary's map changed to the state we want, wait for the map get populated
		if satisfied {
			syncedSmap := &cluster.Smap{}
			cmn.CopyStruct(syncedSmap, smap)

			// skip primary proxy and mock targets
			var proxyID string
			for _, p := range smap.Pmap {
				if p.PublicNet.DirectURL == proxyURL {
					proxyID = p.ID()
				}
			}
			err = WaitMapVersionSync(smapChangeDeadline, syncedSmap, origVersion, []string{MockDaemonID, proxyID})
			if err != nil {
				return nil, err
			}

			if syncedSmap.Version != smap.Version {
				if !expTgt.satisfied(smap.CountActiveTargets()) ||
					!expPrx.satisfied(smap.CountActiveProxies()) {
					return nil, fmt.Errorf("%s changed after sync (to %s) and does not satisfy the state",
						smap, syncedSmap)
				}
				Logf("%s changed after sync (to %s) but satisfies the state\n", smap, syncedSmap)
			}

			return smap, nil
		}

		lastVersion = smap.Version
		loopCnt++
	next:
		if time.Now().After(smapChangeDeadline) {
			break
		}

		time.Sleep(cmn.MinDuration(time.Second*time.Duration(loopCnt), time.Second*7)) // sleep longer every loop
	}

	return nil, fmt.Errorf("timed out waiting for the cluster to stabilize")
}

func WaitNodeRestored(t *testing.T, proxyURL, reason, nodeID string, origVersion int64, proxyCnt,
	targetCnt int) *cluster.Smap {
	smap, err := WaitForClusterState(proxyURL, reason, origVersion, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)
	_, err = api.WaitNodeAdded(BaseAPIParams(proxyURL), nodeID)
	tassert.CheckFatal(t, err)
	return smap
}

func WaitForNewSmap(proxyURL string, prevVersion int64) (newSmap *cluster.Smap, err error) {
	return WaitForClusterState(proxyURL, "new smap version", prevVersion, 0, 0)
}

func WaitMapVersionSync(timeout time.Time, smap *cluster.Smap, prevVersion int64, idsToIgnore []string) error {
	return devtools.WaitMapVersionSync(devtoolsCtx, timeout, smap, prevVersion, idsToIgnore)
}

func GetTargetsMountpaths(t *testing.T, smap *cluster.Smap, params api.BaseParams) map[string][]string {
	mpathsByTarget := make(map[string][]string, smap.CountTargets())
	for _, target := range smap.Tmap {
		mpl, err := api.GetMountpaths(params, target)
		tassert.CheckError(t, err)
		mpathsByTarget[target.DaemonID] = mpl.Available
	}

	return mpathsByTarget
}

func CheckNodeAlive(t testing.TB, node *cluster.Snode) {
	_, err := getPID(node.PublicNet.DaemonPort)
	tassert.CheckFatal(t, err)
}

func KillNode(node *cluster.Snode) (cmd RestoreCmd, err error) {
	var (
		daemonID = node.ID()
		port     = node.PublicNet.DaemonPort
		pid      string
	)
	cmd.Node = node
	if containers.DockerRunning() {
		Logf("Stopping container %s\n", daemonID)
		err := containers.StopContainer(daemonID)
		return cmd, err
	}

	pid, cmd.Cmd, cmd.Args, err = getProcess(port)
	if err != nil {
		return
	}
	_, err = exec.Command("kill", "-2", pid).CombinedOutput()
	if err != nil {
		return
	}
	// wait for the process to actually disappear
	to := time.Now().Add(time.Second * 30)
	for {
		_, _, _, errpid := getProcess(port)
		if errpid != nil {
			break
		}
		if time.Now().After(to) {
			err = fmt.Errorf("failed to kill -2 process pid=%s at port %s", pid, port)
			break
		}
		time.Sleep(time.Second)
	}

	exec.Command("kill", "-9", pid).CombinedOutput()
	time.Sleep(time.Second)

	if err != nil {
		_, _, _, errpid := getProcess(port)
		if errpid != nil {
			err = nil
		} else {
			err = fmt.Errorf("failed to kill -9 process pid=%s at port %s", pid, port)
		}
	}
	return
}

func RestoreNode(cmd RestoreCmd, asPrimary bool, tag string) error {
	if containers.DockerRunning() {
		Logf("Restarting %s container %s\n", tag, cmd)
		return containers.RestartContainer(cmd.Cmd)
	}
	if asPrimary && !cmn.StringInSlice("-skip_startup=true", cmd.Args) {
		// 50-50 to apply flag or not (randomize to test different startup paths)
		if rand.Intn(2) == 0 {
			cmd.Args = append(cmd.Args, "-skip_startup=true")
		}
	}

	if !cmn.AnyHasPrefixInSlice("-daemon_id", cmd.Args) {
		cmd.Args = append(cmd.Args, "-daemon_id="+cmd.Node.ID())
	}

	Logf("Restoring %s: %s %+v\n", tag, cmd.Cmd, cmd.Args)

	ncmd := exec.Command(cmd.Cmd, cmd.Args...)
	// When using Ctrl-C on test, children (restored daemons) should not be
	// killed as well.
	// (see: https://groups.google.com/forum/#!topic/golang-nuts/shST-SDqIp4)
	ncmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if asPrimary {
		// Sets the environment variable to start as primary proxy to true
		env := os.Environ()
		env = append(env, fmt.Sprintf("%s=true", cmn.EnvVars.IsPrimary))
		ncmd.Env = env
	}

	if err := ncmd.Start(); err != nil {
		return err
	}
	return ncmd.Process.Release()
}

// getPID uses 'lsof' to find the pid of the ais process listening on a port
func getPID(port string) (string, error) {
	output, err := exec.Command("lsof", []string{"-sTCP:LISTEN", "-i", ":" + port}...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("error executing LSOF command: %v", err)
	}

	// Skip lines before first appearance of "COMMAND"
	lines := strings.Split(string(output), "\n")
	i := 0
	for ; ; i++ {
		if strings.HasPrefix(lines[i], "COMMAND") {
			break
		}
	}

	// second colume is the pid
	return strings.Fields(lines[i+1])[1], nil
}

// getProcess finds the ais process by 'lsof' using a port number, it finds the ais process's
// original command line by 'ps', returns the command line for later to restart(restore) the process.
func getProcess(port string) (string, string, []string, error) {
	pid, err := getPID(port)
	if err != nil {
		return "", "", nil, fmt.Errorf("error getting pid on port: %v", err)
	}

	output, err := exec.Command("ps", "-p", pid, "-o", "command").CombinedOutput()
	if err != nil {
		return "", "", nil, fmt.Errorf("error executing PS command: %v", err)
	}

	line := strings.Split(string(output), "\n")[1]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", "", nil, fmt.Errorf("no returned fields")
	}

	return pid, fields[0], fields[1:], nil
}
