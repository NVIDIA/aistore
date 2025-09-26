// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
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
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"

	corev1 "k8s.io/api/core/v1"
)

const (
	maxNodeRetry       = 10               // max retries to get health
	processKillTimeout = 30 * time.Second // timeout to wait for process to terminate
	DefaultNamespace   = "ais"
)

const resilverTimeout = time.Minute

type (
	nodesCnt int

	WaitRetryOpts struct {
		MaxRetries int
		Interval   time.Duration
	}
)

var ErrTimedOutStabilize = errors.New("timed out waiting for the cluster to stabilize")

func (n nodesCnt) satisfied(actual int) bool {
	if n == 0 {
		return true
	}
	return int(n) == actual
}

// as the name implies
func WaitNodePubAddrNotInUse(si *meta.Snode, timeout time.Duration) error {
	var (
		addr     = si.PubNet.TCPEndpoint()
		interval = controlPlaneSleep
	)
	tlog.Logfln("Waiting for %s to shutdown (and stop listening)", si.StringEx())
	time.Sleep(interval) // not immediate
	for elapsed := time.Duration(0); elapsed < timeout; elapsed += interval {
		if _, err := net.DialTimeout("tcp4", addr, interval); err != nil {
			time.Sleep(interval)
			return nil
		}
		time.Sleep(interval)
	}
	return errors.New("timeout")
}

// Add an alive node that is not in SMap to the cluster.
// Use to add a new node to the cluster or get back a node removed by `RemoveNodeUnsafe`
func JoinCluster(proxyURL string, node *meta.Snode) (string, error) {
	return _joinCluster(proxyURL, node, registerTimeout)
}

// Restore a node put into maintenance: in this case the node is in
// Smap and canceling maintenance gets the node back.
func RestoreTarget(t *testing.T, proxyURL string, target *meta.Snode) (rebID string, newSmap *meta.Smap) {
	smap := GetClusterMap(t, proxyURL)
	tlog.Logfln("Joining target %s (current %s)", target.StringEx(), smap.StringEx())
	val := &apc.ActValRmNode{DaemonID: target.ID()}
	rebID, err := api.StopMaintenance(BaseAPIParams(proxyURL), val)
	tassert.CheckFatal(t, err)
	newSmap, err = WaitForClusterState(
		proxyURL,
		"join target",
		smap.Version,
		smap.CountActivePs(),
		smap.CountActiveTs()+1,
	)
	tassert.CheckFatal(t, err)
	return rebID, newSmap
}

func PromptWaitOnHerr(herr *cmn.ErrHTTP) {
	const sleep = 10 * time.Second
	tlog.Logfln("Warning: %v", herr)
	tlog.Logfln("Warning: waiting %v and retrying...", sleep)
	time.Sleep(sleep)
}

func StartMaintenance(bp api.BaseParams, actValue *apc.ActValRmNode) (xid string, err error) {
	xid, err = api.StartMaintenance(bp, actValue)
	if err == nil {
		return
	}
	herr, ok := err.(*cmn.ErrHTTP)
	if !ok {
		return
	}
	if herr.TypeCode != "ErrLimitedCoexistence" {
		return
	}

	PromptWaitOnHerr(herr)
	return api.StartMaintenance(bp, actValue)
}

func ClearMaintenance(bp api.BaseParams, tsi *meta.Snode) {
	val := &apc.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: true}
	// it can fail if the node is not under maintenance but it is OK
	_, _ = api.StopMaintenance(bp, val)
}

// TODO: to handle ErrLimitedCoexistence add PromptWaitOnHerr + retry
func DecommissionNode(bp api.BaseParams, actValue *apc.ActValRmNode) (xid string, err error) {
	return api.DecommissionNode(bp, actValue)
}

func RandomProxyURL(ts ...*testing.T) (url string) {
	var (
		bp        = BaseAPIParams(proxyURLReadOnly)
		smap, err = waitForStartup(bp)
		retries   = 3
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
		bp = BaseAPIParams(url)
		if smap, err = waitForStartup(bp); err == nil {
			return getRandomProxyURL(smap)
		}
		retries--
	}
	if len(ts) > 0 {
		tassert.CheckFatal(ts[0], err)
	}

	return ""
}

func getRandomProxyURL(smap *meta.Smap) string {
	proxies := smap.Pmap.ActiveNodes()
	return proxies[rand.IntN(len(proxies))].URL(cmn.NetPublic)
}

// Return the first proxy from smap that is IC member. The primary
// proxy has higher priority.
func GetICProxy(t testing.TB, smap *meta.Smap, ignoreID string) *meta.Snode {
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
	syncIgnoreIDs ...string) (*meta.Smap, error) {
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

func _minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// WaitForClusterState waits until a cluster reaches specified state, meaning:
// - smap has version larger than origVersion
// - number of active proxies is equal proxyCnt, unless proxyCnt == 0
// - number of active targets is equal targetCnt, unless targetCnt == 0.
//
// It returns the smap which satisfies those requirements.
// NOTE: Upon successful return from this function cluster state might have already changed.
func WaitForClusterState(proxyURL, reason string, origVer int64, pcnt, tcnt int, ignoreIDs ...string) (*meta.Smap, error) {
	const (
		maxSleep = 7 * time.Second
		maxWait  = 2 * time.Minute
	)
	var (
		expPrx  = nodesCnt(pcnt)
		expTgt  = nodesCnt(tcnt)
		bp      = BaseAPIParams(proxyURL)
		lastVer int64
		iter    int
	)
	if expPrx == 0 && expTgt == 0 {
		if origVer > 0 {
			tlog.Logfln("Waiting for %q (Smap > v%d)", reason, origVer)
		} else {
			tlog.Logfln("Waiting for %q", reason)
		}
	} else {
		if origVer > 0 {
			tlog.Logfln("Waiting for %q (p%d, t%d, Smap > v%d)", reason, expPrx, expTgt, origVer)
		} else {
			tlog.Logfln("Waiting for %q (p%d, t%d)", reason, expPrx, expTgt)
		}
	}
	started := time.Now()
	deadline := started.Add(maxWait)
	opDeadline := started.Add(2 * maxWait)
	for {
		var (
			smap, err = api.GetClusterMap(bp)
			ok        bool
		)
		if err != nil {
			if !cos.IsRetriableConnErr(err) {
				return nil, err
			}
			tlog.Logfln("%v", err)
			goto next
		}
		ok = expTgt.satisfied(smap.CountActiveTs()) && expPrx.satisfied(smap.CountActivePs()) &&
			smap.Version > origVer
		if ok && time.Since(started) < time.Second {
			time.Sleep(time.Second)
			lastVer = smap.Version
			continue
		}
		if !ok {
			if time.Since(started) > maxSleep {
				pid := pidFromURL(smap, proxyURL)
				if expPrx == 0 && expTgt == 0 {
					tlog.Logfln("Polling %s(%s) for (Smap > v%d)", meta.Pname(pid), smap.StringEx(), origVer)
				} else {
					tlog.Logfln("Polling %s(%s) for (t=%d, p=%d, Smap > v%d)",
						meta.Pname(pid), smap.StringEx(), expTgt, expPrx, origVer)
				}
			}
		}
		if smap.Version != lastVer && lastVer != 0 {
			deadline = _minTime(time.Now().Add(maxWait), opDeadline)
		}
		// if the primary's map changed to the state we want, wait for the map get populated
		if ok {
			syncedSmap := &meta.Smap{}
			cos.CopyStruct(syncedSmap, smap)

			// skip primary proxy and mock targets
			proxyID := pidFromURL(smap, proxyURL)
			idsToIgnore := cos.NewStrSet(MockDaemonID, proxyID)
			idsToIgnore.Add(ignoreIDs...)
			err = waitSmapSync(bp, deadline, syncedSmap, origVer, idsToIgnore)
			if err != nil {
				tlog.Logfln("Failed waiting for cluster state condition: %v (%s, %s, %v, %v)",
					err, smap, syncedSmap, origVer, idsToIgnore)
				return nil, err
			}
			if !expTgt.satisfied(smap.CountActiveTs()) || !expPrx.satisfied(smap.CountActivePs()) {
				return nil, fmt.Errorf("%s updated and does not satisfy the state condition anymore", smap.StringEx())
			}
			return smap, nil
		}
		lastVer = smap.Version
		iter++
	next:
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(min(time.Second*time.Duration(iter), maxSleep))
	}

	return nil, ErrTimedOutStabilize
}

func pidFromURL(smap *meta.Smap, proxyURL string) string {
	for _, p := range smap.Pmap {
		if p.PubNet.URL == proxyURL {
			return p.ID()
		}
	}
	return ""
}

func WaitForNewSmap(proxyURL string, prevVersion int64) (newSmap *meta.Smap, err error) {
	return WaitForClusterState(proxyURL, "new Smap version", prevVersion, 0, 0)
}

func WaitForResilvering(t *testing.T, bp api.BaseParams, target *meta.Snode) {
	args := xact.ArgsMsg{Kind: apc.ActResilver, Timeout: resilverTimeout}
	if target != nil {
		args.DaemonID = target.ID()
		time.Sleep(controlPlaneSleep)
	} else {
		time.Sleep(2 * controlPlaneSleep)
	}
	allFinished := func(snaps xact.MultiSnap) (done, resetProbeFreq bool) {
		tid, xsnap, err := snaps.RunningTarget("")
		tassert.CheckFatal(t, err)
		if tid != "" {
			tlog.Logfln("t[%s]: x-%s[%s] is running", tid, xsnap.Kind, xsnap.ID)
			return false, false
		}
		return true, true
	}
	err := api.WaitForXactionNode(bp, &args, allFinished)
	tassert.CheckFatal(t, err)
}

func GetTargetsMountpaths(t *testing.T, smap *meta.Smap, params api.BaseParams) map[*meta.Snode][]string {
	mpathsByTarget := make(map[*meta.Snode][]string, smap.CountTargets())
	for _, target := range smap.Tmap {
		mpl, err := api.GetMountpaths(params, target)
		tassert.CheckFatal(t, err)
		mpathsByTarget[target] = mpl.Available
	}

	return mpathsByTarget
}

// killProxyOrTargetK8s kills a proxy or target in test environments using kubeconfig-based K8s client.
// This function is designed for use in Go tests running from shell environments.
func killProxyOrTargetK8s(bp api.BaseParams, node *meta.Snode, namespace ...string) error {
	smap, err := api.GetClusterMap(bp)
	if err != nil {
		return fmt.Errorf("failed to get cluster map; err: %v", err)
	}

	client, err := k8s.InitTestClient(namespace...)
	if err != nil {
		return fmt.Errorf("failed to get test K8s client; err: %v", err)
	}

	ds, err := api.GetStatsAndStatus(bp, node)
	if err != nil {
		return fmt.Errorf("failed to get stats and status; err: %v", err)
	}
	tlog.Logfln("Deleting pod %s", ds.K8sPodName)
	err = client.Delete(k8s.Pod, ds.K8sPodName)
	if err != nil {
		return fmt.Errorf("failed to delete pod %s; err: %v", ds.K8sPodName, err)
	}

	err = WaitForCondition(func() bool {
		phase, err := client.Health(ds.K8sPodName)
		if err != nil {
			return false
		}
		if phase != string(corev1.PodRunning) {
			tlog.Logfln("Pod %s is not running, phase: %s", ds.K8sPodName, phase)
			return false
		}
		return true
	}, WaitRetryOpts{MaxRetries: maxNodeRetry, Interval: processKillTimeout})
	if err != nil {
		return err
	}
	tlog.Logfln("Joining node %s (current %s)", node.StringEx(), smap.StringEx())
	// In K8s deployments, the StatefulSet may restart the target so quickly that the kalive ping doesn't miss it and the cluster map version may not increment.
	_, err = WaitForClusterState(bp.URL, "cluster to stabilize", 0, smap.CountActivePs(), smap.CountActiveTs())
	return err
}

// In Kubernetes mode, killing a pod will cause it to be automatically recreated by the StatefulSet.
// In non-Kubernetes modes, the node must be manually restored using the returned RestoreCmd.
func KillNode(bp api.BaseParams, node *meta.Snode) (cmd RestoreCmd, err error) {
	var (
		daemonID = node.ID()
		port     = node.PubNet.Port
		pid      int
	)

	isK8s, _ := isClusterK8s()
	switch {
	case isK8s:
		return cmd, killProxyOrTargetK8s(bp, node, DefaultNamespace)
	case docker.IsRunning():
		tlog.Logfln("Stopping container %s", daemonID)
		err := docker.Stop(daemonID)
		return cmd, err
	default: // local deployment
		restoreNodesOnce.Do(func() {
			initNodeCmd()
		})
		cmd.Node = node
		if docker.IsRunning() {
			tlog.Logfln("Stopping container %s", daemonID)
			err := docker.Stop(daemonID)
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
		to := time.Now().Add(processKillTimeout)
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
	}
	return cmd, err
}

func ShutdownNode(bp api.BaseParams, node *meta.Snode) (pid int, cmd RestoreCmd, rebID string, err error) {
	pid, cmd, rebID, err = _shutdownNode(bp, node)

	if err == nil {
		return
	}
	herr, ok := err.(*cmn.ErrHTTP)
	if !ok {
		return
	}
	if herr.TypeCode != "ErrLimitedCoexistence" {
		return
	}

	PromptWaitOnHerr(herr)

	pid, cmd, rebID, err = _shutdownNode(bp, node)
	return
}

func _shutdownNode(bp api.BaseParams, node *meta.Snode) (pid int, cmd RestoreCmd, rebID string, err error) {
	restoreNodesOnce.Do(func() {
		initNodeCmd()
	})

	var (
		daemonID = node.ID()
		port     = node.PubNet.Port
	)
	tlog.Logfln("Shutting down %s", node.StringEx())
	cmd.Node = node
	if docker.IsRunning() {
		tlog.Logfln("Stopping container %s", daemonID)
		err = docker.Stop(daemonID)
		return pid, cmd, "", err
	}

	pid, cmd.Cmd, cmd.Args, err = getProcess(port)
	if err != nil {
		return pid, cmd, "", err
	}

	actValue := &apc.ActValRmNode{DaemonID: daemonID}
	rebID, err = api.ShutdownNode(bp, actValue)

	return pid, cmd, rebID, err
}

func RestoreNode(cmd RestoreCmd, asPrimary bool, tag string) error {
	if docker.IsRunning() {
		tlog.Logfln("Restarting %s container [%s %s]", tag, cmd.Node.ID(), cmd.Cmd)
		return docker.Restart(cmd.Node.ID())
	}

	if !cos.AnyHasPrefixInSlice("-daemon_id", cmd.Args) {
		cmd.Args = append(cmd.Args, "-daemon_id="+cmd.Node.ID())
	}

	tlog.Logfln("Restoring %s: %s %+v", tag, cmd.Cmd, cmd.Args)
	pid, err := startNode(&cmd, asPrimary)
	if err == nil && pid <= 0 {
		err = fmt.Errorf("RestoreNode: invalid process ID %d", pid)
	}
	return err
}

// When using Ctrl-C on test, children (restored daemons) should not be killed as well.
// (see: https://groups.google.com/forum/#!topic/golang-nuts/shST-SDqIp4)
func startNode(cmd *RestoreCmd, asPrimary bool) (int, error) {
	ncmd := exec.Command(cmd.Cmd, cmd.Args...) //nolint:gosec // used only in tests
	ncmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if asPrimary {
		// Sets the environment variable to start as primary
		environ := os.Environ()
		environ = append(environ, env.AisPrimaryEP+"="+cmd.Node.PubNet.URL)
		ncmd.Env = environ
	}
	if err := ncmd.Start(); err != nil {
		return 0, err
	}
	pid := ncmd.Process.Pid
	err := ncmd.Process.Release()
	return pid, err
}

func DeployNode(t *testing.T, node *meta.Snode, conf *cmn.Config, localConf *cmn.LocalConfig) int {
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

	localConfFile := filepath.Join(conf.ConfigDir, fname.PlainLocalConfig)
	err := jsp.SaveMeta(localConfFile, localConf, nil)
	tassert.CheckFatal(t, err)

	configFile := filepath.Join(conf.ConfigDir, "ais.json")
	err = jsp.SaveMeta(configFile, &conf.ClusterConfig, nil)
	tassert.CheckFatal(t, err)

	var cmd = RestoreCmd{
		Args: []string{
			"-role=" + node.Type(),
			"-daemon_id=" + node.ID(),
			"-config=" + configFile,
			"-local_config=" + localConfFile,
		},
		Node: node,
		Cmd:  getAISNodeCmd(t),
	}

	pid, err := startNode(&cmd, false)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, pid > 0, "invalid process ID %d", pid)
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
	var (
		sport = ":" + port
		cmd   = exec.Command("lsof", "-sTCP:LISTEN", "-i", sport)
	)
	output, err := cmd.CombinedOutput()
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
	var (
		spid = strconv.Itoa(pid)
		ps   = exec.Command("ps", "-p", spid, "-o", "command")
	)
	output, err := ps.CombinedOutput()
	if err != nil {
		return 0, "", nil, fmt.Errorf("error executing PS command: %v", err)
	}

	line := strings.Split(string(output), "\n")[1]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0, "", nil, errors.New("no returned fields")
	}

	return pid, fields[0], fields[1:], nil
}

func WaitForPID(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		tlog.Logfln("Warning: PID %d already terminated: %v", pid, err)
		return nil
	}

	var (
		cancel context.CancelFunc
		ctx    = context.Background()
		done   = make(chan error)
	)
	tlog.Logfln("Waiting for PID %d to terminate", pid)

	const (
		timeout = 4 * time.Second
	)
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	go func() {
		_, erw := process.Wait() // wait with no timeout
		done <- erw
	}()

	for {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func GetRestoreCmd(si *meta.Snode) RestoreCmd {
	var (
		err error
		cmd = RestoreCmd{Node: si}
	)
	if docker.IsRunning() {
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
		afterProxyCnt  = smap.CountActivePs()
		afterTargetCnt = smap.CountActiveTs()
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
			time.Sleep(2 * controlPlaneSleep)
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
			tassert.Errorf(t, node.Eq(cmd.Node),
				"%s %s changed, before %+v, after %+v", cmd.Node.Type(), node.ID(), cmd.Node, node)
		} else {
			tassert.Errorf(t, false, "%s %s not found in %s", cmd.Node.Type(), cmd.Node.ID(), smap.StringEx())
		}
		if docker.IsRunning() {
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

func WaitNodeAdded(bp api.BaseParams, nodeID string) (*meta.Smap, error) {
	var i int
retry:
	smap, err := api.GetClusterMap(bp)
	if err != nil {
		return nil, err
	}
	node := smap.GetNode(nodeID)
	if node != nil {
		return smap, WaitNodeReady(node.URL(cmn.NetPublic))
	}
	time.Sleep(controlPlaneSleep)
	i++
	if i > maxNodeRetry {
		return nil, fmt.Errorf("max retry (%d) exceeded - node not in smap", maxNodeRetry)
	}

	goto retry
}

func WaitNodeReady(url string, opts ...*WaitRetryOpts) (err error) {
	var (
		bp            = BaseAPIParams(url)
		retries       = maxNodeRetry
		retryInterval = controlPlaneSleep
		i             int
	)
	if len(opts) > 0 && opts[0] != nil {
		retries = opts[0].MaxRetries
		retryInterval = opts[0].Interval
	}
while503:
	err = api.Health(bp)
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

func _joinCluster(proxyURL string, node *meta.Snode, timeout time.Duration) (rebID string, err error) {
	bp := api.BaseParams{Client: gctx.Client, URL: proxyURL, Token: LoggedUserToken}
	smap, err := api.GetClusterMap(bp)
	if err != nil {
		return "", err
	}
	if rebID, _, err = api.JoinCluster(bp, node, 0 /*node flags*/); err != nil {
		return
	}

	// If node is already in cluster we should not wait for map version
	// sync because update will not be scheduled
	if node := smap.GetNode(node.ID()); node == nil {
		err = waitSmapSync(bp, time.Now().Add(timeout), smap, smap.Version, cos.NewStrSet())
		return
	}
	return
}

func _nextNode(smap *meta.Smap, idsToIgnore cos.StrSet) (sid string, isproxy, exists bool) {
	for _, d := range smap.Pmap {
		if !idsToIgnore.Contains(d.ID()) {
			sid = d.ID()
			isproxy, exists = true, true
			return
		}
	}
	for _, d := range smap.Tmap {
		if !idsToIgnore.Contains(d.ID()) {
			sid = d.ID()
			isproxy, exists = false, true
			return
		}
	}
	return
}

func waitSmapSync(bp api.BaseParams, timeout time.Time, smap *meta.Smap, ver int64, ignore cos.StrSet) error {
	var (
		prevSid string
		orig    = ignore.Clone()
	)
	for {
		sid, isproxy, exists := _nextNode(smap, ignore)
		if !exists {
			break
		}
		if sid == prevSid {
			time.Sleep(time.Second)
		}
		sname := meta.Tname(sid)
		if isproxy {
			sname = meta.Pname(sid)
		}
		newSmap, err := api.GetNodeClusterMap(bp, sid)
		if err != nil && !cos.IsRetriableConnErr(err) &&
			!cmn.IsStatusServiceUnavailable(err) && !cmn.IsStatusBadGateway(err) /* retry as well */ {
			return err
		}
		if err == nil && newSmap.Version > ver {
			ignore.Add(sid)
			if newSmap.Version > smap.Version {
				tlog.Logfln("Updating %s to %s from %s", smap, newSmap.StringEx(), sname)
				cos.CopyStruct(smap, newSmap)
			}
			if newSmap.Version > ver+1 {
				// reset
				if ver <= 0 {
					tlog.Logfln("Received %s from %s", newSmap.StringEx(), sname)
				} else {
					tlog.Logfln("Received newer %s from %s, updated wait-for condition (%d => %d)",
						newSmap.StringEx(), sname, ver, newSmap.Version)
				}
				ver = newSmap.Version - 1
				ignore = orig.Clone()
				ignore.Add(sid)
			}
			continue
		}
		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for %s to sync Smap > v%d", sname, ver)
		}
		if newSmap != nil {
			if snode := newSmap.GetNode(sid); snode != nil {
				tlog.Logfln("Waiting for %s(%s) to sync Smap > v%d", snode.StringEx(), newSmap, ver)
			} else {
				tlog.Logfln("Waiting for %s(%s) to sync Smap > v%d", sname, newSmap, ver)
				tlog.Logfln("(Warning: %s hasn't joined yet - not present)", sname)
			}
		}
		prevSid = sid
	}
	if currSmap == nil || currSmap.Version < smap.Version {
		currSmap = smap
	}
	return nil
}

// remove node unsafe
func _removeNodeFromSmap(proxyURL, sid string, timeout time.Duration) error {
	var (
		bp        = api.BaseParams{Client: gctx.Client, URL: proxyURL, Token: LoggedUserToken}
		smap, err = api.GetClusterMap(bp)
		node      = smap.GetNode(sid)
	)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}
	if node != nil && smap.IsPrimary(node) {
		return errors.New("unregistering primary proxy is not allowed")
	}
	tlog.Logfln("Remove %s from %s", node.StringEx(), smap)

	err = api.RemoveNodeUnsafe(bp, sid)
	if err != nil {
		return err
	}

	// If node does not exist in cluster we should not wait for map version
	// sync because update will not be scheduled.
	if node != nil {
		return waitSmapSync(bp, time.Now().Add(timeout), smap, smap.Version, cos.NewStrSet(node.ID()))
	}
	return nil
}
