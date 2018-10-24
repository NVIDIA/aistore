/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package tutils provides common low-level utilities for all dfcpub unit and integration tests
package tutils

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

var (
	dockerRunning = false
)

// Detect docker cluster at startup
func init() {
	cmd := exec.Command("docker", "ps")
	bytes, err := cmd.Output()
	if err == nil && strings.Contains(string(bytes), "proxy_1") {
		dockerRunning = true
	}
}

// DockerRunning returns true if docker-based DFC cluster is detected
func DockerRunning() bool {
	return dockerRunning
}

// ContainerCount is used by clusterHealthCheck to test if any container crashed after
// a test completes
func ContainerCount(clusterNumber ...int) (proxyCnt int, targetCnt int) {
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
	proxyPrefix := "dfc" + strconv.Itoa(cluster) + "_proxy_"
	targetPrefix := "dfc" + strconv.Itoa(cluster) + "_target_"
	for _, line := range lines {
		if strings.Contains(line, proxyPrefix) {
			proxyCnt++
		} else if strings.Contains(line, targetPrefix) {
			targetCnt++
		}
	}

	return
}

// ClusterCount returns the current number of clusters running
func ClusterCount() int {
	cmd := exec.Command("docker", "ps", "--format", "\"{{.Names}}\"")
	bytes, err := cmd.Output()
	if err != nil {
		return 0
	}
	m := make(map[string]int)
	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if len(line) > len("\"dfci_") && strings.HasPrefix(line, "\"dfc") {
			// containter names have the prefix dfci_ where i is the ith dfc cluster
			m[line[:len("\"dfci_")]]++
		}
	}
	return len(m)
}

// TargetsInCluster returns the names of the targets in cluster i
func TargetsInCluster(i int) (ans []string) {
	cmd := exec.Command("docker", "ps", "--format", "\"{{.Names}}\"")
	bytes, err := cmd.Output()
	if err != nil {
		return
	}
	targetPrefix := "dfc" + strconv.Itoa(i) + "_target_"
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
		return fmt.Errorf("Not enough arguments to execute a command")
	}
	temp := append([]string{"docker", "exec", containerName}, args...)
	cmd := exec.Command(temp[0], temp[1:]...)

	_, err := cmd.Output()
	if err != nil {
		Logf("%q error executing docker command: docker exec %s %v.\n", err.Error(), containerName, args)
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

// DisconnectContainer diconnects specific containerID from all networks.
// Returns all networks from which has been disconnected.
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
