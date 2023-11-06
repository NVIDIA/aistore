// Package k8s provides utilities for communicating with Kubernetes cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn/nlog"
	v1 "k8s.io/api/core/v1"
)

const (
	k8sPodNameEnv  = "HOSTNAME"
	k8sNodeNameEnv = "K8S_NODE_NAME"

	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

var (
	detectOnce sync.Once
	NodeName   string
)

func initDetect() {
	var (
		pod      *v1.Pod
		nodeName = os.Getenv(k8sNodeNameEnv)
		podName  = os.Getenv(k8sPodNameEnv)
	)
	client, err := GetClient()
	if err != nil {
		nlog.Infof("K8s client nil => non-Kubernetes deployment: (%s: %q, %s: %q)", k8sPodNameEnv, podName, k8sNodeNameEnv, nodeName)
		return
	}
	nlog.Infof("Checking (%s: %q, %s: %q)", k8sPodNameEnv, podName, k8sNodeNameEnv, nodeName)

	// if specified, `k8sNodeNameEnv` takes precedence: proceed directly to check
	if nodeName != "" {
		goto checkNode
	}
	if podName == "" {
		nlog.Infoln("K8s environment (above) not set => non-Kubernetes deployment")
		return
	}

	// check POD
	pod, err = client.Pod(podName)
	if err != nil {
		nlog.Errorf("Failed to get K8s pod %q: %v (tip: try setting %q env variable)", podName, err, k8sNodeNameEnv)
		return
	}
	nodeName = pod.Spec.NodeName
	nlog.Infoln("pod.Spec: NodeName", nodeName, "Hostname", pod.Spec.Hostname, "HostNetwork", pod.Spec.HostNetwork)

checkNode: // always check Node
	node, err := client.Node(nodeName)
	if err != nil {
		nlog.Errorf("Failed to get K8s node %q: %v (tip: try setting %q env variable)", nodeName, err, k8sNodeNameEnv)
		return
	}

	NodeName = node.Name
	nlog.Infoln("K8s Node.Name:", NodeName, "Namespace:", node.Namespace)

	_ppvols(pod.Spec.Volumes)
}

func _ppvols(volumes []v1.Volume) {
	for i := range volumes {
		name := "K8s Volume: " + volumes[i].Name
		if claim := volumes[i].VolumeSource.PersistentVolumeClaim; claim != nil {
			nlog.Infof("%s, %+v", name, claim)
		} else {
			nlog.Infoln(name)
		}
	}
}

func Detect() error {
	detectOnce.Do(initDetect)

	if NodeName == "" {
		return fmt.Errorf("the operation requires Kubernetes")
	}
	return nil
}

// POD name (K8s doesn't allow `_` and uppercase)
func CleanName(name string) string { return strings.ReplaceAll(strings.ToLower(name), "_", "-") }

const (
	shortNameETL = 6
	longNameETL  = 32
)

func ValidateEtlName(name string) error {
	const prefix = "ETL name %q "
	l := len(name)
	if l < shortNameETL {
		return fmt.Errorf(prefix+"is too short", name)
	}
	if l > longNameETL {
		return fmt.Errorf(prefix+"is too long", name)
	}
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		return fmt.Errorf(prefix+"is invalid: can only contain [a-z0-9-]", name)
	}
	return nil
}
