// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
)

const (
	nodeRetryInterval = 2 * time.Second // interval to check for node health
	maxNodeRetry      = 10              // max retries to get health
)

const resilverTimeout = time.Minute

type (
	nodesCnt int

	WaitRetryOpts struct {
		MaxRetries int
		Interval   time.Duration
	}
)

func (n nodesCnt) satisfied(actual int) bool {
	if n == 0 {
		return true
	}
	return int(n) == actual
}

// Add an alive node that is not in SMap to the cluster.
// Use to add a new node to the cluster or get back a node removed by `RemoveNodeFromSmap`
func JoinCluster(proxyURL string, node *cluster.Snode) (string, error) {
	return devtools.JoinCluster(DevtoolsCtx, proxyURL, node, registerTimeout)
}

// Restore a node put into maintenance: in this case the node is in
// Smap and canceling maintenance gets the node back.
func RestoreTarget(t *testing.T, proxyURL string, target *cluster.Snode) (rebID string, newSmap *cluster.Smap) {
	smap := GetClusterMap(t, proxyURL)
	tlog.Logf("Joining target %s (current %s)\n", target.StringEx(), smap.StringEx())
	val := &apc.ActValRmNode{DaemonID: target.ID()}
	rebID, err := api.StopMaintenance(BaseAPIParams(proxyURL), val)
	tassert.CheckFatal(t, err)
	newSmap, err = WaitForClusterState(
		proxyURL,
		"join target",
		smap.Version,
		smap.CountActiveProxies(),
		smap.CountActiveTargets()+1,
	)
	tassert.CheckFatal(t, err)
	return rebID, newSmap
}

func ClearMaintenance(baseParams api.BaseParams, tsi *cluster.Snode) {
	val := &apc.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: true}
	// it can fail if the node is not under maintenance but it is OK
	_, _ = api.StopMaintenance(baseParams, val)
}

func RandomProxyURL(ts ...*testing.T) (url string) {
	var (
		baseParams = BaseAPIParams(proxyURLReadOnly)
		smap, err  = waitForStartup(baseParams)
		retries    = 3
	)
	if err == nil {
		return getRandomProxyURL(smap)
	}
	for _, node := range pmapReadOnly {
		url := node.URL(cmn.NetPublic)
		if url == proxyURLReadOnly {
			continue
		}
		if retries == 0 {
			return ""
		}
		baseParams = BaseAPIParams(url)
		if smap, err = waitForStartup(baseParams); err == nil {
			return getRandomProxyURL(smap)
		}
		retries--
	}
	if len(ts) > 0 {
		tassert.CheckFatal(ts[0], err)
	}

	return ""
}

func getRandomProxyURL(smap *cluster.Smap) string {
	proxies := smap.Pmap.ActiveNodes()
	return proxies[rand.Intn(len(proxies))].URL(cmn.NetPublic)
}

// Return the first proxy from smap that is IC member. The primary
// proxy has higher priority.
func GetICProxy(t testing.TB, smap *cluster.Smap, ignoreID string) *cluster.Snode {
	if smap.IsIC(smap.Primary) {
		return smap.Primary
	}
	for _, proxy := range smap.Pmap {
		if ignoreID != "" && proxy.ID() == ignoreID {
			continue
		}
		if !smap.IsIC(proxy) {
			continue
		}
		return proxy
	}
	t.Fatal("failed to choose random IC member")
	return nil
}

// WaitForClusterStateActual waits until a cluster reaches specified state, meaning:
// - smap has version larger than origVersion
// - number of proxies in Smap is equal proxyCnt, unless proxyCnt == 0
// - number of targets in Smap is equal targetCnt, unless targetCnt == 0.
//
// It returns the smap which satisfies those requirements.
func WaitForClusterStateActual(proxyURL, reason string, origVersion int64, proxyCnt, targetCnt int,
	syncIgnoreIDs ...string) (*cluster.Smap, error) {
	for {
		smap, err := WaitForClusterState(proxyURL, reason, origVersion, proxyCnt, targetCnt, syncIgnoreIDs...)
		if err != nil {
			return nil, err
		}
		if smap.CountTargets() == targetCnt && smap.CountProxies() == proxyCnt {
			return smap, nil
		}
		tlog.Logf("Smap changed from %d to %d, but the number of proxies(%d/%d)/targets(%d/%d) is not reached",
			origVersion, smap.Version, targetCnt, smap.CountTargets(), proxyCnt, smap.CountProxies())
		origVersion = smap.Version
	}
}

// WaitForClusterState waits until a cluster reaches specified state, meaning:
// - smap has version larger than origVersion
// - number of active proxies is equal proxyCnt, unless proxyCnt == 0
// - number of active targets is equal targetCnt, unless targetCnt == 0.
//
// It returns the smap which satisfies those requirements.
// NOTE: Upon successful return from this function cluster state might have already changed.
func WaitForClusterState(proxyURL, reason string, origVer int64, pcnt, tcnt int, ignoreIDs ...string) (*cluster.Smap, error) {
	var (
		lastVersion                               int64
		smapChangeDeadline, timeStart, opDeadline time.Time

		expPrx = nodesCnt(pcnt)
		expTgt = nodesCnt(tcnt)

		baseParams = BaseAPIParams(proxyURL)
		loopCnt    int
		satisfied  bool
	)

	timeStart = time.Now()
	smapChangeDeadline = timeStart.Add(2 * proxyChangeLatency)
	opDeadline = timeStart.Add(3 * proxyChangeLatency)

	tlog.Logf("Waiting for: %q(p%d, t%d, Smap > v%d)\n", reason, expPrx, expTgt, origVer)

	// Repeat until success or timeout.
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil {
			if !cos.IsRetriableConnErr(err) {
				return nil, err
			}
			tlog.Logf("%v\n", err)
			goto next
		}
		satisfied = expTgt.satisfied(smap.CountActiveTargets()) &&
			expPrx.satisfied(smap.CountActiveProxies()) &&
			smap.Version > origVer
		if !satisfied {
			if d := time.Since(timeStart); d > 7*time.Second {
				p := "primary"
				if smap.Primary.PubNet.URL != proxyURL {
					p = proxyURL
				}
				tlog.Logf("Polling %s[%s] for (t=%d, p=%d, Smap > v%d)\n", p, smap, expTgt, expPrx, origVer)
			}
		}
		if smap.Version != lastVersion {
			smapChangeDeadline = cos.MinTime(time.Now().Add(proxyChangeLatency), opDeadline)
		}
		// if the primary's map changed to the state we want, wait for the map get populated
		if satisfied {
			syncedSmap := &cluster.Smap{}
			cos.CopyStruct(syncedSmap, smap)

			// skip primary proxy and mock targets
			var proxyID string
			for _, p := range smap.Pmap {
				if p.PubNet.URL == proxyURL {
					proxyID = p.ID()
				}
			}

			idsToIgnore := cos.NewStringSet(MockDaemonID, proxyID)
			idsToIgnore.Add(ignoreIDs...)
			err = devtools.WaitMapVersionSync(
				baseParams,
				DevtoolsCtx,
				smapChangeDeadline,
				syncedSmap,
				origVer,
				idsToIgnore,
			)
			if err != nil {
				tlog.Logf("Failed waiting for cluster state: %v (%s, %s, %v, %v)\n",
					err, smap, syncedSmap, origVer, idsToIgnore)
				return nil, err
			}

			if syncedSmap.Version != smap.Version {
				if !expTgt.satisfied(smap.CountActiveTargets()) ||
					!expPrx.satisfied(smap.CountActiveProxies()) {
					return nil,
						fmt.Errorf("%s changed after sync (to %s) and does not satisfy the state",
							smap, syncedSmap)
				}
				tlog.Logf("%s changed after sync (to %s) but satisfies the state\n", smap, syncedSmap)
			}

			return smap, nil
		}

		lastVersion = smap.Version
		loopCnt++
	next:
		if time.Now().After(smapChangeDeadline) {
			break
		}
		// sleep longer each iter (up to a certain limit)
		time.Sleep(cos.MinDuration(time.Second*time.Duration(loopCnt), time.Second*7))
	}

	return nil, fmt.Errorf("timed out waiting for the cluster to stabilize")
}

func WaitForNewSmap(proxyURL string, prevVersion int64) (newSmap *cluster.Smap, err error) {
	return WaitForClusterState(proxyURL, "new smap version", prevVersion, 0, 0)
}

func WaitForResilvering(t *testing.T, baseParams api.BaseParams, target *cluster.Snode) {
	args := api.XactReqArgs{Kind: apc.ActResilver, Timeout: resilverTimeout}
	if target != nil {
		args.DaemonID = target.ID()
		time.Sleep(2 * time.Second)
	} else {
		time.Sleep(4 * time.Second)
	}
	err := api.WaitForXactionNode(baseParams, args, _xactSnapFinished)
	tassert.CheckFatal(t, err)
}

func _xactSnapFinished(snaps api.NodesXactMultiSnap) bool {
	tid, xsnap := snaps.Running()
	if tid != "" {
		tlog.Logf("t[%s]: x-%s[%s] is running\n", tid, xsnap.Kind, xsnap.ID)
		return false
	}
	return true
}

func GetTargetsMountpaths(t *testing.T, smap *cluster.Smap, params api.BaseParams) map[*cluster.Snode][]string {
	mpathsByTarget := make(map[*cluster.Snode][]string, smap.CountTargets())
	for _, target := range smap.Tmap {
		mpl, err := api.GetMountpaths(params, target)
		tassert.CheckFatal(t, err)
		mpathsByTarget[target] = mpl.Available
	}

	return mpathsByTarget
}

func KillNode(node *cluster.Snode) (cmd RestoreCmd, err error) {
	restoreNodesOnce.Do(func() {
		initNodeCmd()
	})

	var (
		daemonID = node.ID()
		port     = node.PubNet.Port
		pid      int
	)
	cmd.Node = node
	if containers.DockerRunning() {
		tlog.Logf("Stopping container %s\n", daemonID)
		err := containers.StopContainer(daemonID)
		return cmd, err
	}

	pid, cmd.Cmd, cmd.Args, err = getProcess(port)
	if err != nil {
		return
	}

	if err = syscall.Kill(pid, syscall.SIGINT); err != nil {
		return
	}
	// wait for the process to actually disappear
	to := time.Now().Add(time.Second * 30)
	for {
		if _, _, _, errPs := getProcess(port); errPs != nil {
			break
		}
		if time.Now().After(to) {
			err = fmt.Errorf("failed to 'kill -2' process (pid: %d, port: %s)", pid, port)
			break
		}
		time.Sleep(time.Second)
	}

	syscall.Kill(pid, syscall.SIGKILL)
	time.Sleep(time.Second)

	if err != nil {
		if _, _, _, errPs := getProcess(port); errPs != nil {
			err = nil
		} else {
			err = fmt.Errorf("failed to 'kill -9' process (pid: %d, port: %s)", pid, port)
		}
	}
	return
}

func ShutdownNode(_ *testing.T, baseParams api.BaseParams, node *cluster.Snode) (pid int, cmd RestoreCmd, err error) {
	restoreNodesOnce.Do(func() {
		initNodeCmd()
	})

	var (
		daemonID = node.ID()
		port     = node.PubNet.Port
	)
	tlog.Logf("Shutting down %s\n", node.StringEx())
	cmd.Node = node
	if containers.DockerRunning() {
		tlog.Logf("Stopping container %s\n", daemonID)
		err = containers.StopContainer(daemonID)
		return
	}

	pid, cmd.Cmd, cmd.Args, err = getProcess(port)
	if err != nil {
		return
	}

	actValue := &apc.ActValRmNode{DaemonID: daemonID}
	_, err = api.ShutdownNode(baseParams, actValue)
	return
}

func RestoreNode(cmd RestoreCmd, asPrimary bool, tag string) error {
	if containers.DockerRunning() {
		tlog.Logf("Restarting %s container %s\n", tag, cmd)
		return containers.RestartContainer(cmd.Node.ID())
	}

	if !cos.AnyHasPrefixInSlice("-daemon_id", cmd.Args) {
		cmd.Args = append(cmd.Args, "-daemon_id="+cmd.Node.ID())
	}

	tlog.Logf("Restoring %s: %s %+v\n", tag, cmd.Cmd, cmd.Args)
	_, err := startNode(cmd.Cmd, cmd.Args, asPrimary)
	return err
}

func startNode(cmd string, args []string, asPrimary bool) (pid int, err error) {
	ncmd := exec.Command(cmd, args...)
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

	if err = ncmd.Start(); err != nil {
		return
	}
	pid = ncmd.Process.Pid
	err = ncmd.Process.Release()
	return
}

func DeployNode(t *testing.T, node *cluster.Snode, conf *cmn.Config, localConf *cmn.LocalConfig) int {
	conf.ConfigDir = t.TempDir()
	conf.LogDir = t.TempDir()
	conf.TestFSP.Root = t.TempDir()
	conf.TestFSP.Instance = 42

	if localConf == nil {
		localConf = &cmn.LocalConfig{}
		localConf.ConfigDir = conf.ConfigDir
		localConf.HostNet.Port = conf.HostNet.Port
		localConf.HostNet.PortIntraControl = conf.HostNet.PortIntraControl
		localConf.HostNet.PortIntraData = conf.HostNet.PortIntraData
	}

	localConfFile := filepath.Join(conf.ConfigDir, "ais_local.json")
	err := jsp.SaveMeta(localConfFile, localConf, nil)
	tassert.CheckFatal(t, err)

	configFile := filepath.Join(conf.ConfigDir, "ais.json")
	err = jsp.SaveMeta(configFile, &conf.ClusterConfig, nil)
	tassert.CheckFatal(t, err)

	args := []string{
		"-role=" + node.Type(),
		"-daemon_id=" + node.ID(),
		"-config=" + configFile,
		"-local_config=" + localConfFile,
	}

	cmd := getAISNodeCmd(t)
	pid, err := startNode(cmd, args, false)
	tassert.CheckFatal(t, err)
	return pid
}

// CleanupNode kills the process.
func CleanupNode(t *testing.T, pid int) {
	err := syscall.Kill(pid, syscall.SIGKILL)
	// Ignore error if process is not found.
	if errors.Is(err, syscall.ESRCH) {
		return
	}
	tassert.CheckError(t, err)
}

// getAISNodeCmd finds the command for deploying AIS node
func getAISNodeCmd(t *testing.T) string {
	// Get command from cached restore CMDs when available
	if len(restoreNodes) != 0 {
		for _, cmd := range restoreNodes {
			return cmd.Cmd
		}
	}

	// If no cached comand, use a random proxy to get command
	proxyURL := RandomProxyURL()
	proxy, err := GetPrimaryProxy(proxyURL)
	tassert.CheckFatal(t, err)
	rcmd := GetRestoreCmd(proxy)
	return rcmd.Cmd
}

// getPID uses 'lsof' to find the pid of the ais process listening on a port
func getPID(port string) (int, error) {
	output, err := exec.Command("lsof", []string{"-sTCP:LISTEN", "-i", ":" + port}...).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("error executing LSOF command: %v", err)
	}

	// Skip lines before first appearance of "COMMAND"
	lines := strings.Split(string(output), "\n")
	i := 0
	for ; ; i++ {
		if strings.HasPrefix(lines[i], "COMMAND") {
			break
		}
	}

	// Second column is the pid.
	pid := strings.Fields(lines[i+1])[1]
	return strconv.Atoi(pid)
}

// getProcess finds the ais process by 'lsof' using a port number, it finds the ais process's
// original command line by 'ps', returns the command line for later to restart(restore) the process.
func getProcess(port string) (pid int, cmd string, args []string, err error) {
	pid, err = getPID(port)
	if err != nil {
		return 0, "", nil, fmt.Errorf("error getting pid on port: %v", err)
	}

	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command").CombinedOutput()
	if err != nil {
		return 0, "", nil, fmt.Errorf("error executing PS command: %v", err)
	}

	line := strings.Split(string(output), "\n")[1]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0, "", nil, fmt.Errorf("no returned fields")
	}

	return pid, fields[0], fields[1:], nil
}

func WaitForNodeToTerminate(pid int, timeout ...time.Duration) error {
	var (
		ctx           = context.Background()
		retryInterval = time.Second
		deadline      = time.Minute
	)

	if len(timeout) > 0 {
		deadline = timeout[0]
	}

	tlog.Logf("Waiting for process PID=%d to terminate\n", pid)
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, deadline)
	defer cancel()
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}

	done := make(chan error)
	go func() {
		_, err := process.Wait()
		done <- err
	}()
	for {
		time.Sleep(retryInterval)
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			break
		}
	}
}

func GetRestoreCmd(si *cluster.Snode) RestoreCmd {
	var (
		err error
		cmd = RestoreCmd{Node: si}
	)
	if containers.DockerRunning() {
		return cmd
	}
	cmd.PID, cmd.Cmd, cmd.Args, err = getProcess(si.PubNet.Port)
	cos.AssertNoErr(err)
	return cmd
}

// EnsureOrigClusterState verifies the cluster has the same nodes after tests
// If a node is killed, it restores the node
func EnsureOrigClusterState(t *testing.T) {
	if len(restoreNodes) == 0 {
		return
	}
	var (
		proxyURL       = RandomProxyURL()
		smap           = GetClusterMap(t, proxyURL)
		baseParam      = BaseAPIParams(proxyURL)
		afterProxyCnt  = smap.CountActiveProxies()
		afterTargetCnt = smap.CountActiveTargets()
		tgtCnt         int
		proxyCnt       int
		updated        bool
		retried        bool
	)
retry:
	for _, cmd := range restoreNodes {
		node := smap.GetNode(cmd.Node.ID())
		if node == nil && !retried {
			tlog.Logf("Warning: %s %s not found in %s - retrying...", cmd.Node.Type(), cmd.Node.ID(), smap.StringEx())
			time.Sleep(4 * time.Second)
			smap = GetClusterMap(t, proxyURL)
			// alternatively, could simply compare versions
			retried = true
			goto retry
		}
	}
	// restore
	for _, cmd := range restoreNodes {
		if cmd.Node.IsProxy() {
			proxyCnt++
		} else {
			tgtCnt++
		}
		node := smap.GetNode(cmd.Node.ID())
		if node != nil {
			tassert.Errorf(t, node.Equals(cmd.Node),
				"%s %s changed, before %+v, after %+v", cmd.Node.Type(), node.ID(), cmd.Node, node)
		} else {
			tassert.Errorf(t, false, "%s %s not found in %s", cmd.Node.Type(), cmd.Node.ID(), smap.StringEx())
		}
		if containers.DockerRunning() {
			if node == nil {
				err := RestoreNode(cmd, false, cmd.Node.Type())
				tassert.CheckError(t, err)
				updated = true
			}
			continue
		}

		_, err := getPID(cmd.Node.PubNet.Port)
		if err != nil {
			tassert.CheckError(t, err)
			if err = RestoreNode(cmd, false, cmd.Node.Type()); err == nil {
				_, err := WaitNodeAdded(baseParam, cmd.Node.ID())
				tassert.CheckError(t, err)
			}
			tassert.CheckError(t, err)
			updated = true
		}
	}

	tassert.Errorf(t, afterProxyCnt == proxyCnt, "Some proxies crashed(?): expected %d, have %d", proxyCnt, afterProxyCnt)
	tassert.Errorf(t, tgtCnt == afterTargetCnt, "Some targets crashed(?): expected %d, have %d", tgtCnt, afterTargetCnt)

	if !updated {
		return
	}

	_, err := WaitForClusterState(proxyURL, "cluster to stabilize", smap.Version, proxyCnt, tgtCnt)
	tassert.CheckFatal(t, err)

	if tgtCnt != afterTargetCnt {
		WaitForRebalAndResil(t, BaseAPIParams(proxyURL))
	}
}

func WaitNodeAdded(baseParams api.BaseParams, nodeID string) (*cluster.Smap, error) {
	i := 0

retry:
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return nil, err
	}
	node := smap.GetNode(nodeID)
	if node != nil {
		return smap, WaitNodeReady(node.URL(cmn.NetPublic))
	}
	time.Sleep(nodeRetryInterval)
	i++
	if i > maxNodeRetry {
		return nil, fmt.Errorf("max retry (%d) exceeded - node not in smap", maxNodeRetry)
	}

	goto retry
}

func WaitNodeReady(url string, opts ...*WaitRetryOpts) (err error) {
	var (
		i          = 0
		baseParams = BaseAPIParams(url)

		retries       = maxNodeRetry
		retryInterval = nodeRetryInterval
	)

	if len(opts) > 0 && opts[0] != nil {
		retries = opts[0].MaxRetries
		retryInterval = opts[0].Interval
	}

while503:
	err = api.Health(baseParams)
	if err == nil {
		return nil
	}
	if !cmn.IsStatusServiceUnavailable(err) && !cos.IsRetriableConnErr(err) {
		return
	}
	time.Sleep(retryInterval)
	i++
	if i > retries {
		return fmt.Errorf("node start failed - max retries (%d) exceeded", retries)
	}
	goto while503
}
