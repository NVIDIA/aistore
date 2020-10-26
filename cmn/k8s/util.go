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
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	k8sHostNameEnv = "K8S_HOST_NAME"

	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

var (
	detectOnce sync.Once
	NodeName   string
)

func initDetect() {
	if NodeName = os.Getenv(k8sHostNameEnv); NodeName == "" {
		return
	}

	client, err := GetClient()
	if err != nil {
		glog.Errorf("couldn't initiate a K8s client, err: %v", err) // TODO: make it a Warning
		NodeName = ""
		return
	}
	node, err := client.Node(NodeName)
	if err != nil {
		glog.Errorf("failed to get node, err: %v", err) // TODO: make it a Warning
		NodeName = ""
		return
	}
	debug.Assert(node.Name == NodeName)
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
