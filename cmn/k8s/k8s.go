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

	"k8s.io/client-go/rest"
)

type PodStatus struct {
	State    string // "Waiting" | "Running" | "Terminated"
	CtrName  string // main container name
	Reason   string
	Message  string
	ExitCode int32
}

const defaultPodNameEnv = "HOSTNAME"

const (
	Default = "default"
	Pod     = "pod"
	Svc     = "svc"
)

const (
	nonK8s        = "non-Kubernetes deployment"
	missingK8sEnv = "K8s environment variable not found"
)

var (
	NodeName string // assign upon successful initialization

	ErrK8sRequired = errors.New("the operation requires Kubernetes")
)

func Init() {
	if err := _initClient(); err != nil {
		// Non-K8s deployment
		if softNonK8s(err) {
			nlog.Infoln(nonK8s, "(init k8s-client returned: '"+_short(err)+"')")
			return
		}
		cos.ExitLogf("k8s client initialization failed: %v", err)
		return
	}
	// in-cluster: both the node name and the pod name are required
	nodeName := os.Getenv(env.AisK8sNode)
	if nodeName == "" {
		cos.ExitLogf("%s: %q", missingK8sEnv, env.AisK8sNode)
	}
	podName := resolvePodName()
	if podName == "" {
		cos.ExitLogf("%s: %q (or %q)", missingK8sEnv, env.AisK8sPod, defaultPodNameEnv)
	}
	nlog.Infof("Pod info: name: %q, namespace: %q, node: %q", podName, _namespace(), nodeName)

	NodeName = nodeName // last: IsK8s() implies an initialized client
}

// softNonK8s is true when client init failed because we are not in a cluster.
// Any other init error is a hard failure (misconfigured in-cluster deploy).
func softNonK8s(err error) bool {
	return errors.Is(err, rest.ErrNotInCluster)
}

func resolvePodName() string {
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
