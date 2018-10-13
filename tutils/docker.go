/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package tutils provides common low-level utilities for all dfcpub unit and integration tests
package tutils

import (
	"os/exec"
	"strings"
)

var (
	dockerRunning = false
)

// Detect docker cluster at startup
func init() {
	cmd := exec.Command("docker", "ps")
	bytes, err := cmd.Output()
	if err == nil && strings.Contains(string(bytes), "dfcproxy_1") {
		dockerRunning = true
	}
}

// DockerRunning returns true if docker-based DFC cluster is detected
func DockerRunning() bool {
	return dockerRunning
}

// Used by clusterHealthCheck to test if any container crashed after
// a test completes
func NumberOfRunningContainers() (proxyCnt int, targetCnt int) {
	cmd := exec.Command("docker", "ps")
	bytes, err := cmd.Output()
	if err != nil {
		return
	}

	lines := strings.Split(string(bytes), "\n")
	for _, line := range lines {
		if strings.Contains(line, "dfcproxy_") {
			proxyCnt++
		} else if strings.Contains(line, "dfctarget_") {
			targetCnt++
		}
	}

	return
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
