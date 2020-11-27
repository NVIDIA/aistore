// Package k8s provides utilities for communicating with Kubernetes cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const (
	k8sPodNameEnv = "HOSTNAME"

	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

var (
	detectOnce sync.Once
	NodeName   string
)

func initDetect() {
	var podName string

	if podName = os.Getenv(k8sPodNameEnv); podName == "" {
		if glog.V(3) {
			glog.Infof("HOSTNAME environment not found, assuming non-Kubernetes deployment")
		}
		return
	}

	glog.Infof("Verifying type of deployment (HOSTNAME = %q)", podName)

	client, err := GetClient()
	if err != nil {
		glog.Warningf("Couldn't initiate a K8s client, err: %v", err)
		return
	}

	pod, err := client.Pod(podName)
	if err != nil {
		glog.Warningf("Failed to get pod %q, err: %v", podName, err)
		return
	}

	node, err := client.Node(pod.Spec.NodeName)
	if err != nil {
		glog.Errorf("Failed to get node of a pod %q, err: %v", podName, err)
		return
	}
	NodeName = node.Name
}

func Detect() error {
	detectOnce.Do(initDetect)

	if NodeName == "" {
		return fmt.Errorf("operation requires Kubernetes deployment")
	}
	return nil
}

func CleanName(name string) string {
	return strings.ReplaceAll(strings.ToLower(name), "_", "-")
}
