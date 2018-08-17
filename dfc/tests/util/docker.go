/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package util

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
