// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type PodStatus struct {
	State    string // "Waiting" | "Running" | "Terminated"
	CtrName  string // main container name
	Reason   string
	Message  string
	ExitCode int32
}

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
	if _, err := GetClient(); err != nil {
		nlog.Infoln(nonK8s, "(init k8s-client returned: '"+_short(err)+"')")
		return
	}
	podName := _podName()
	if podName == "" {
		nlog.Infof("Env %q is not set => %s", env.AisK8sPod, nonK8s)
		return
	}
	_initNode()
	nlog.Infoln("Pod info:", "name", podName, ",namespace", _namespace(), ",node", NodeName)
}

// Resolve this node's name from the environment.
func _initNode() {
	if NodeName = os.Getenv(env.AisK8sNode); NodeName == "" {
		cos.ExitLogf("Failed to get K8s node name: env %q is not set", env.AisK8sNode)
	}
}

// Resolve this pod's name from the environment (empty when not in a pod).
func _podName() string {
	podName := os.Getenv(env.AisK8sPod)
	if podName == "" {
		return os.Getenv(defaultPodNameEnv)
	}
	debug.Func(func() {
		pn := os.Getenv(defaultPodNameEnv)
		debug.Assertf(pn == "" || pn == podName, "%q vs %q", pn, podName)
	})
	return podName
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

func (ps *PodStatus) String() string {
	return fmt.Sprintf("container: %s, state: %s, reason: %s, message: %s, exitCode: %d", ps.CtrName, ps.State, ps.Reason, ps.Message, ps.ExitCode)
}

func (ps PodStatus) Error() string { return ps.String() }
