// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		nlog.Infoln(nonK8s, "(init k8s-client returned: '"+_short(err)+"')")
		return
	}

	var (
		pod      *v1.Pod
		podName  = os.Getenv(env.AisK8sPod)
		nodeName = os.Getenv(env.AisK8sNode)
	)
	if podName != "" {
		debug.Func(func() {
			pn := os.Getenv(defaultPodNameEnv)
			debug.Assertf(pn == "" || pn == podName, "%q vs %q", pn, podName)
		})
	} else {
		podName = os.Getenv(defaultPodNameEnv)
	}
	nlog.Infof("Checking pod: %q, node: %q", podName, nodeName)

	if podName == "" {
		if nodeName != "" {
			// If the Pod is not set but the Node is, we should continue checking.
			goto checkNode
		}
		nlog.Infof("Env %q and %q are not set => %s", env.AisK8sNode, env.AisK8sPod, nonK8s)
		return
	}

	// Check Pod.
	pod, err = client.Pod(podName)
	if err != nil {
		cos.ExitLogf("Failed to get Pod %q, err: %v", podName, err)
		return
	}
	nodeName = pod.Spec.NodeName
	nlog.Infoln("Pod spec", "name", podName, "namespace", pod.Namespace, "node", nodeName, "hostname", pod.Spec.Hostname, "host_network", pod.Spec.HostNetwork)
	_ppvols(pod.Spec.Volumes)

checkNode:
	// Check Node.
	node, err := client.Node(nodeName)
	if err != nil {
		cos.ExitLogf("Failed to get Node %q, err: %v", nodeName, err)
		return
	}

	NodeName = node.Name
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
	const sizeLimit = 32
	msg := err.Error()
	idx := strings.IndexByte(msg, ',')
	switch {
	case len(msg) < sizeLimit:
		return msg
	case idx > sizeLimit:
		return msg[:idx]
	default:
		return msg[:sizeLimit] + " ..."
	}
}
