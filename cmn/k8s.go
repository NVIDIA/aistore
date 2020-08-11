// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"os"
	"os/exec"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const (
	K8SHostName = "K8S_HOST_NAME"
	KubeDefault = "default"
	Kubectl     = "kubectl"
	KubePod     = "pod"
	KubeSvc     = "svc"
)

func DetectK8s() (k8snode string) {
	if k8snode = os.Getenv(K8SHostName); k8snode == "" {
		return
	}
	output, err := exec.Command(Kubectl, "get", "node", k8snode, "--template={{.metadata.name}}").Output()
	if err != nil {
		glog.Errorf("not detecting K8s, err: %v", err) // TODO: make it a Warning
		return ""
	}
	if s := string(output); s != k8snode {
		k8snode = ""
		glog.Errorf("not detecting K8s: %s != %s", k8snode, s) // TODO: make it a Warning
	}
	return
}
