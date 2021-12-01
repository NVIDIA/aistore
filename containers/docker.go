// Package containers provides common utilities for managing containerized deployments of AIS
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package containers

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// For container naming
const (
	prefixStr = "ais"
	proxyStr  = "_proxy_"
	targetStr = "_target_"
	pattern   = "^\"" + prefixStr + "\\d+" + proxyStr + "[1-9]+\"$" // Checking '_proxy_' should suffice
)

var dockerRunning = false

// Detect docker cluster at startup
func init() {
	cmd := exec.Command("docker", "ps", "--format", "\"{{.Names}}\"")
	bytes, err := cmd.Output()
	if err != nil {
		return
	}

	r := regexp.MustCompile(pattern)
	lines := strings.Split(string(bytes), "\n")
	// Checks to see if there is any container P_proxy_{jj}" running
	for _, line := range lines {
		match := r.MatchString(line)
		if match {
			dockerRunning = true
			return
		}
	}
}

// DockerRunning returns true if docker-based AIStore cluster is detected
func DockerRunning() bool {
	return dockerRunning
}

// ContainerCount is used by clusterHealthCheck to test if any container crashed after
// a test completes
func ContainerCount(clusterNumber ...int) (proxyCnt, targetCnt int) {
	cmd := exec.Command("docker", "ps")
	bytes, err := cmd.Output()
	if err != nil {
		return
	}
	var cluster int
	if len(clusterNumber) != 0 {
		cluster = clusterNumber[0]
	}

	lines := strings.Split(string(bytes), "\n")
	proxyPrefix := prefixStr + strconv.Itoa(cluster) + proxyStr
	targetPrefix := prefixStr + strconv.Itoa(cluster) + targetStr
	for _, line := range lines {
		if strings.Contains(line, proxyPrefix) {
			proxyCnt++
		} else if strings.Contains(line, targetPrefix) {
			targetCnt++
		}
	}

	return
}

func clustersMap() (map[int]int, error) {
	cmd := exec.Command("docker", "ps", "--format", "\"{{.Names}}\"")
	bytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	clusterRegex := regexp.MustCompile(`ais(\d+)_(target|proxy)_\d+`)

	m := make(map[int]int)
	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if !clusterRegex.MatchString(line) {
			continue
		}

		matches := clusterRegex.FindStringSubmatch(line)
		// first match the whole string, second number after ais prefix
		// matched because of \d+ being inside parenthesis
		cos.Assert(len(matches) > 1)
		i, err := strconv.ParseInt(matches[1], 10, 32)
		cos.AssertNoErr(err)
		m[int(i)]++
	}

	return m, nil
}

// ClusterIDs returns all IDs of running ais docker clusters
func ClusterIDs() ([]int, error) {
	m, err := clustersMap()
	if err != nil {
		return nil, err
	}

	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids, nil
}

// TargetsInCluster returns the names of the targets in cluster i
func TargetsInCluster(i int) (ans []string) {
	return nodesInCluster(i, targetStr)
}

// ProxiesInCluster returns the names of the targets in cluster i
func ProxiesInCluster(i int) (ans []string) {
	return nodesInCluster(i, proxyStr)
}

func nodesInCluster(i int, prefix string) (ans []string) {
	cmd := exec.Command("docker", "ps", "--format", "\"{{.Names}}\"")
	bytes, err := cmd.Output()
	if err != nil {
		return
	}
	targetPrefix := prefixStr + strconv.Itoa(i) + prefix
	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.Contains(line, targetPrefix) {
			ans = append(ans, strings.Trim(line, "\""))
		}
	}
	return
}

// DockerCreateMpathDir creates a directory that will be used as a mountpath for each target in cluster c
func DockerCreateMpathDir(c int, mpathFQN string) (err error) {
	targetNames := TargetsInCluster(c)
	for _, target := range targetNames {
		err = ContainerExec(target, "mkdir", "-p", mpathFQN)
		if err != nil {
			return err
		}
	}
	return nil
}

// DockerRemoveMpathDir removes a directory named mpathFQN for each target in cluster c
func DockerRemoveMpathDir(c int, mpathFQN string) (err error) {
	targetNames := TargetsInCluster(c)
	for _, target := range targetNames {
		err = ContainerExec(target, "rm", "-rf", mpathFQN)
		if err != nil {
			return err
		}
	}
	return nil
}

// ContainerExec executes a docker exec command for containerName
func ContainerExec(containerName string, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("not enough arguments to execute a command")
	}
	temp := append([]string{"docker", "exec", containerName}, args...)
	cmd := exec.Command(temp[0], temp[1:]...)

	_, err := cmd.Output()
	if err != nil {
		glog.Infof("%q error executing docker command: docker exec %s %v.\n", err.Error(), containerName, args)
		return err
	}
	return nil
}

// StopContainer simulates killing a target or proxy (by given container id)
func StopContainer(cid string) error {
	cmd := exec.Command("docker", "stop", cid)
	return cmd.Run()
}

// RestartContainer restores previously killed target or proxy with given container id
func RestartContainer(cid string) error {
	cmd := exec.Command("docker", "restart", cid)
	return cmd.Run()
}

// DisconnectContainer disconnects specific containerID from all networks.
// Returns networks from which the container has been disconnected.
func DisconnectContainer(containerID string) ([]string, error) {
	networks, err := containerNetworkList(containerID)
	if err != nil {
		return nil, err
	}

	for _, network := range networks {
		cmd := exec.Command("docker", "network", "disconnect", "-f", network, containerID)
		if err := cmd.Run(); err != nil {
			return nil, err
		}
	}

	return networks, nil
}

// ConnectContainer connects specific containerID to all provided networks.
func ConnectContainer(containerID string, networks []string) error {
	for _, network := range networks {
		cmd := exec.Command("docker", "network", "connect", network, containerID)
		if err := cmd.Run(); err != nil {
			return err
		}
	}

	return nil
}

func ClusterProxyURL(i int) (string, error) {
	proxies := ProxiesInCluster(i)

	if len(proxies) == 0 {
		return "", fmt.Errorf("couldn't find any proxies in cluster %d", i)
	}

	containerID := proxies[0]

	aisPublicNetwork := prefixStr + strconv.Itoa(i) + "_public"

	cmd := exec.Command("docker", "inspect", "--format={{.NetworkSettings.Networks."+aisPublicNetwork+".IPAddress }}", containerID)
	bytes, err := cmd.Output()
	if err != nil {
		return "", err
	}

	output := strings.TrimSpace(string(bytes))
	return output, nil
}

// containerNetworkList returns all names of networks to which given container
// is connected.
func containerNetworkList(containerID string) ([]string, error) {
	cmd := exec.Command("docker", "inspect", "--format={{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", containerID)
	bytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	output := strings.TrimSpace(string(bytes))
	return strings.Split(output, " "), nil
}
