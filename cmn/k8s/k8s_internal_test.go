// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/tools/tassert"

	"k8s.io/client-go/rest"
)

func resetInitState(t *testing.T) {
	t.Helper()
	NodeName = ""
	_defaultK8sClient = nil
	t.Cleanup(func() {
		NodeName = ""
		_defaultK8sClient = nil
	})
}

func TestResolvePodName(t *testing.T) {
	tests := []struct {
		name     string
		myPod    string
		hostname string
		want     string
	}{
		{name: "from_my_pod", myPod: "ais-target-0", hostname: "ais-target-0", want: "ais-target-0"},
		{name: "from_my_pod_only", myPod: "ais-target-0", hostname: "", want: "ais-target-0"},
		{name: "from_hostname", myPod: "", hostname: "ais-proxy-0", want: "ais-proxy-0"},
		{name: "unset", myPod: "", hostname: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(env.AisK8sPod, tt.myPod)
			t.Setenv(defaultPodNameEnv, tt.hostname)
			got := resolvePodName()
			tassert.Fatalf(t, got == tt.want, "resolvePodName() = %q, want %q", got, tt.want)
		})
	}
}

func TestInitNonK8s(t *testing.T) {
	// Soft path only when InClusterConfig returns ErrNotInCluster.
	_, err := rest.InClusterConfig()
	if err == nil {
		t.Skip("in-cluster: Init() requires " + env.AisK8sNode + " and a pod name")
	}
	tassert.Fatal(t, softNonK8s(err), "expected ErrNotInCluster outside a cluster")

	resetInitState(t)
	Init()

	tassert.Fatal(t, !IsK8s(), "expected IsK8s() to be false")
	tassert.Fatalf(t, NodeName == "", "expected empty NodeName, got %q", NodeName)
	tassert.Fatal(t, _defaultK8sClient == nil, "expected k8s client to remain uninitialized")

	_, err = GetClient()
	tassert.Fatal(t, errors.Is(err, errClientNotInit), "expected errClientNotInit from GetClient()")
}

func TestSoftNonK8s(t *testing.T) {
	tassert.Fatal(t, softNonK8s(rest.ErrNotInCluster), "ErrNotInCluster should be soft")
	tassert.Fatal(t, softNonK8s(errors.Join(rest.ErrNotInCluster, errors.New("wrap"))), "wrapped ErrNotInCluster should be soft")
	tassert.Fatal(t, !softNonK8s(errors.New("open /var/run/secrets/kubernetes.io/serviceaccount/token: no such file or directory")), "token read error should be hard")
	tassert.Fatal(t, !softNonK8s(errors.New("invalid configuration: no configuration has been provided")), "NewForConfig-style error should be hard")
}
