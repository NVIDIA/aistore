// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"os"

	"github.com/NVIDIA/aistore/cmn/nlog"
	v1 "k8s.io/api/core/v1"
)

const (
	// env var names
	k8sPodNameEnv  = "HOSTNAME"
	k8sNodeNameEnv = "K8S_NODE_NAME"

	// misc.
	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

var NodeName string // assign upon successful initialization

var ErrK8sRequired = errors.New("the operation requires Kubernetes")

func Init() {
	var (
		pod      *v1.Pod
		nodeName = os.Getenv(k8sNodeNameEnv)
		podName  = os.Getenv(k8sPodNameEnv)
	)
	_initClient()
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
	nlog.Infoln("K8s spec: NodeName", nodeName, "Hostname", pod.Spec.Hostname, "HostNetwork", pod.Spec.HostNetwork)
	_ppvols(pod.Spec.Volumes)

checkNode: // always check Node
	node, err := client.Node(nodeName)
	if err != nil {
		nlog.Errorf("Failed to get K8s node %q: %v (tip: try setting %q env variable)", nodeName, err, k8sNodeNameEnv)
		return
	}

	NodeName = node.Name
	nlog.Infoln("K8s node: Name", NodeName, "Namespace", node.Namespace)
}

func _ppvols(volumes []v1.Volume) {
	for i := range volumes {
		name := "  " + volumes[i].Name
		if claim := volumes[i].VolumeSource.PersistentVolumeClaim; claim != nil {
			nlog.Infof("%s (%v)", name, claim)
		} else {
			nlog.Infoln(name)
		}
	}
}

func IsK8s() bool { return NodeName != "" }
