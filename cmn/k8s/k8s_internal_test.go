// Package k8s: initialization, client, and misc. helpers
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package k8s

import (
	"testing"

	"github.com/NVIDIA/aistore/api/env"
)

func TestInitNodeFromEnv(t *testing.T) {
	t.Cleanup(func() { NodeName = "" })
	NodeName = ""
	t.Setenv(env.AisK8sNode, "env-node")

	_initNode()
	if NodeName != "env-node" {
		t.Fatalf("expected NodeName %q, got %q", "env-node", NodeName)
	}
	if !IsK8s() {
		t.Fatal("expected IsK8s() to be true")
	}
}

func TestPodName(t *testing.T) {
	t.Setenv(env.AisK8sPod, "ais-target-0")
	t.Setenv(defaultPodNameEnv, "ais-target-0")
	if pn := _podName(); pn != "ais-target-0" {
		t.Fatalf("expected pod name %q, got %q", "ais-target-0", pn)
	}
}

// MY_POD is unset in the dev/k8s proxy statefulset; HOSTNAME carries the pod name there.
func TestPodNameFromHostname(t *testing.T) {
	t.Setenv(env.AisK8sPod, "")
	t.Setenv(defaultPodNameEnv, "ais-proxy-0")
	if pn := _podName(); pn != "ais-proxy-0" {
		t.Fatalf("expected pod name %q, got %q", "ais-proxy-0", pn)
	}
}

func TestPodNameNonK8s(t *testing.T) {
	t.Setenv(env.AisK8sPod, "")
	t.Setenv(defaultPodNameEnv, "")
	if pn := _podName(); pn != "" {
		t.Fatalf("expected empty pod name, got %q", pn)
	}
}
