// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	v1 "k8s.io/api/core/v1"
)

const (
	defaultPodNameEnv   = "HOSTNAME"
	defaultNamespaceEnv = "POD_NAMESPACE"
)

const (
	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

const nonK8s = "non-Kubernetes deployment"

var (
	NodeName string // assign upon successful initialization

	ErrK8sRequired = errors.New("the operation requires Kubernetes")
)

func Init() {
	_initClient()
	client, err := GetClient()
	if err != nil {
		nlog.Infoln(nonK8s, "(init k8s-client returned:", _short(err)+")")
		return
	}

	var (
		pod      *v1.Pod
		nodeName = os.Getenv(env.AIS.K8sNode)
		podName  = os.Getenv(env.AIS.K8sPod)
	)
	if podName != "" {
		debug.Func(func() {
			pn := os.Getenv(defaultPodNameEnv)
			debug.Assertf(pn == "" || pn == podName, "%q vs %q", pn, podName)
		})
	} else {
		podName = os.Getenv(defaultPodNameEnv)
	}
	nlog.Infof("Checking K8s pod: %q, node: %q", podName, nodeName)

	// node name specified - proceed directly to check
	if nodeName != "" {
		goto checkNode
	}
	if podName == "" {
		nlog.Infoln("K8s environment (above) not set =>", nonK8s)
		return
	}

	// check POD
	pod, err = client.Pod(podName)
	if err != nil {
		nlog.Errorf("Failed to get K8s pod %q: %v", podName, err)
		return
	}
	nodeName = pod.Spec.NodeName
	nlog.Infoln("K8s spec: NodeName", nodeName, "Hostname", pod.Spec.Hostname, "HostNetwork", pod.Spec.HostNetwork)
	_ppvols(pod.Spec.Volumes)

checkNode: // always check Node
	node, err := client.Node(nodeName)
	if err != nil {
		nlog.Errorf("Failed to get K8s node %q: %v", nodeName, err)
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

func _short(err error) string {
	const max = 20
	msg := err.Error()
	idx := strings.IndexByte(msg, ',')
	switch {
	case len(msg) < max:
		return msg
	case idx > max:
		return msg[:idx]
	default:
		return msg[:max]
	}
}
