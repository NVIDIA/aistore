// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
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

// initial Pod query backoff: 1s doubling up to 30s cap, ~2.5 min total
var podQueryBackoff = wait.Backoff{
	Duration: time.Second,
	Factor:   2,
	Cap:      30 * time.Second,
	Steps:    10,
}

func Init() {
	_initClient()
	client, err := GetClient()
	if err != nil {
		nlog.Infoln(nonK8s, "(init k8s-client returned: '"+_short(err)+"')")
		return
	}

	var (
		pod     *v1.Pod
		podName = os.Getenv(env.AisK8sPod)
	)
	if podName != "" {
		debug.Func(func() {
			pn := os.Getenv(defaultPodNameEnv)
			debug.Assertf(pn == "" || pn == podName, "%q vs %q", pn, podName)
		})
	} else {
		podName = os.Getenv(defaultPodNameEnv)
	}
	nlog.Infof("Checking pod: %q", podName)

	if podName == "" {
		nlog.Infof("Env %q is not set => %s", env.AisK8sPod, nonK8s)
		return
	}

	// Check Pod. Retry with backoff: a transient failure to reach the API server
	// at startup (e.g., node-local networking not yet ready) must not turn into
	// a fatal exit and a kubelet crash-loop.
	err = retry.OnError(podQueryBackoff, func(error) bool { return true }, func() (e error) {
		if pod, e = client.Pod(podName); e != nil {
			nlog.Warningln("failed to get Pod", podName, "- will retry:", e)
		}
		return e
	})
	if err != nil {
		cos.ExitLogf("Failed to get Pod %q, err: %v", podName, err)
		return
	}

	// Check if pod is already scheduled or fall back to env var
	switch {
	case pod.Spec.NodeName != "":
		NodeName = pod.Spec.NodeName
	case os.Getenv(env.AisK8sNode) != "":
		NodeName = os.Getenv(env.AisK8sNode)
	default:
		cos.ExitLogf("Failed to get K8s node name. %q is not set", env.AisK8sNode)
		return
	}

	nlog.Infoln("Pod info:", "name", podName, ",namespace", pod.Namespace, ",node", NodeName, ",hostname", pod.Spec.Hostname, ",host_network", pod.Spec.HostNetwork)
	_ppvols(pod.Spec.Volumes)
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

func (ps *PodStatus) String() string {
	return fmt.Sprintf("container: %s, state: %s, reason: %s, message: %s, exitCode: %d", ps.CtrName, ps.State, ps.Reason, ps.Message, ps.ExitCode)
}

func (ps PodStatus) Error() string { return ps.String() }
