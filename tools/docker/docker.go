// Packager docker provides common utilities for managing containerized AIS deployments
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package docker

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// naming
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
func IsRunning() bool {
	return dockerRunning
}

func clustersMap() (map[int]int, error) {
	const (
		a, b, c, d = "docker", "ps", "--format", "\"{{.Names}}\""
	)
	cmd := exec.Command(a, b, c, d)
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

// CreateMpathDir creates a directory that will be used as a mountpath for each target in cluster c
func CreateMpathDir(c int, mpathFQN string) (err error) {
	targetNames := TargetsInCluster(c)
	for _, target := range targetNames {
		err = _exec(target, "mkdir", "-p", mpathFQN)
		if err != nil {
			return err
		}
	}
	return nil
}

// RemoveMpathDir removes a directory named mpathFQN for each target in cluster c
func RemoveMpathDir(c int, mpathFQN string) (err error) {
	targetNames := TargetsInCluster(c)
	for _, target := range targetNames {
		err = _exec(target, "rm", "-rf", mpathFQN)
		if err != nil {
			return err
		}
	}
	return nil
}

func _exec(containerName string, args ...string) error {
	if len(args) == 0 {
		return errors.New("not enough arguments to execute a command")
	}
	temp := append([]string{"docker", "exec", containerName}, args...)
	cmd := exec.Command(temp[0], temp[1:]...) //nolint:gosec // used only in tests

	_, err := cmd.Output()
	if err != nil {
		nlog.Infof("%q error executing docker command: docker exec %s %v.\n", err.Error(), containerName, args)
		return err
	}
	return nil
}

// Stop simulates killing a target or proxy (by given container id)
func Stop(cid string) error {
	cmd := exec.Command("docker", "stop", cid)
	return cmd.Run()
}

// Restart restores previously killed target or proxy with given container id
func Restart(cid string) error {
	cmd := exec.Command("docker", "restart", cid)
	return cmd.Run()
}

// Disconnect disconnects specific containerID from all networks.
// Returns networks from which the container has been disconnected.
func Disconnect(containerID string) ([]string, error) {
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

// Connect connects specific containerID to all provided networks.
func Connect(containerID string, networks []string) error {
	for _, network := range networks {
		cmd := exec.Command("docker", "network", "connect", network, containerID)
		if err := cmd.Run(); err != nil {
			return err
		}
	}

	return nil
}

func ClusterEndpoint(i int) (string, error) {
	proxies := ProxiesInCluster(i)
	if len(proxies) == 0 {
		return "", fmt.Errorf("couldn't find any proxies in cluster %d", i)
	}
	var (
		containerID      = proxies[0]
		aisPublicNetwork = prefixStr + strconv.Itoa(i) + "_public"
		format           = "--format={{.NetworkSettings.Networks." + aisPublicNetwork + ".IPAddress }}"

		cmd = exec.Command("docker", "inspect", format, containerID)
	)
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
