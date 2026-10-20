// Package main contains logic for the aisinit container
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/api/env"
	aiscmn "github.com/NVIDIA/aistore/cmn"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRetry(t *testing.T) {
	failTwice := func() func() (int, error) {
		calls := 0
		return func() (int, error) {
			calls++
			if calls <= 2 {
				return 0, errors.New("not ready")
			}
			return calls, nil
		}
	}
	if _, err := retry("check", 2, 0, failTwice()); err == nil {
		t.Error("expected an error after 2 attempts")
	}
	if val, err := retry("check", 3, 0, failTwice()); err != nil || val != 3 {
		t.Errorf("expected success on attempt 3, got %d, %v", val, err)
	}
}

func TestApplyHostNet(t *testing.T) {
	const (
		podDNS = "t-0.svc.ns.svc.cluster.local"
		pub    = "10.0.0.5"
		lb     = "203.0.113.7"
		ovr    = "t-0.example.com"
	)
	tests := []struct {
		name             string
		params           hostNetParams
		wantHostname     string
		intraDataCleared bool
	}{
		{name: "proxy IP", params: hostNetParams{dnsMode: env.PubNetDNSModeIP}, wantHostname: pub},
		{name: "proxy ignores external access and host network", params: hostNetParams{dnsMode: env.PubNetDNSModeIP, externalAccess: true, hostNetwork: true}, wantHostname: pub},
		{name: "proxy Node", params: hostNetParams{dnsMode: env.PubNetDNSModeNode}, wantHostname: pub},
		{name: "proxy Pod", params: hostNetParams{dnsMode: env.PubNetDNSModePod}, wantHostname: podDNS},
		{name: "unknown mode falls back to public hostname", params: hostNetParams{dnsMode: "bogus"}, wantHostname: pub},
		{name: "target IP", params: hostNetParams{isTarget: true, dnsMode: env.PubNetDNSModeIP}, wantHostname: pub},
		{name: "target Pod", params: hostNetParams{isTarget: true, dnsMode: env.PubNetDNSModePod}, wantHostname: podDNS},
		{name: "target host network IP discovers", params: hostNetParams{isTarget: true, hostNetwork: true, dnsMode: env.PubNetDNSModeIP}, wantHostname: "", intraDataCleared: true},
		{name: "target host network Node", params: hostNetParams{isTarget: true, hostNetwork: true, dnsMode: env.PubNetDNSModeNode}, wantHostname: pub},
		{name: "target host network Pod", params: hostNetParams{isTarget: true, hostNetwork: true, dnsMode: env.PubNetDNSModePod}, wantHostname: podDNS},
		{name: "target external access", params: hostNetParams{isTarget: true, externalAccess: true, dnsMode: env.PubNetDNSModePod}, wantHostname: lb},
		{name: "target external access beats host network", params: hostNetParams{isTarget: true, externalAccess: true, hostNetwork: true, dnsMode: env.PubNetDNSModeIP}, wantHostname: lb, intraDataCleared: true},
		{name: "override beats external access", params: hostNetParams{isTarget: true, externalAccess: true, dnsMode: env.PubNetDNSModeIP, override: ovr}, wantHostname: ovr},
		{name: "override beats Pod", params: hostNetParams{dnsMode: env.PubNetDNSModePod, override: ovr}, wantHostname: ovr},
		{name: "override beats host network discovery", params: hostNetParams{isTarget: true, hostNetwork: true, dnsMode: env.PubNetDNSModeIP, override: ovr}, wantHostname: ovr, intraDataCleared: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := tc.params
			params.publicHostName = pub
			params.podName, params.serviceName, params.namespace, params.clusterDomain = "t-0", "svc", "ns", "cluster.local"
			params.k8s = fake.NewSimpleClientset(&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "t-0", Namespace: "ns"},
				Status:     corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{Ingress: []corev1.LoadBalancerIngress{{IP: lb}}}},
			})
			wantIntraData := podDNS
			if tc.intraDataCleared {
				wantIntraData = ""
			}

			var hostNet aiscmn.LocalNetConfig
			params.apply(&hostNet)
			if hostNet.Hostname != tc.wantHostname {
				t.Errorf("Hostname = %q, want %q", hostNet.Hostname, tc.wantHostname)
			}
			if hostNet.HostnameIntraData != wantIntraData {
				t.Errorf("HostnameIntraData = %q, want %q", hostNet.HostnameIntraData, wantIntraData)
			}
			if hostNet.HostnameIntraControl != podDNS {
				t.Errorf("HostnameIntraControl = %q, want %q", hostNet.HostnameIntraControl, podDNS)
			}
		})
	}
}
