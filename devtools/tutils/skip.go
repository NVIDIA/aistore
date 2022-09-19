// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/docker"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

type SkipTestArgs struct {
	Bck                   cmn.Bck
	RequiredDeployment    ClusterType
	MinTargets            int
	MinProxies            int
	MinMountpaths         int
	RequiresRemoteCluster bool
	RequiresAuth          bool
	Long                  bool
	RemoteBck             bool
	CloudBck              bool
	K8s                   bool
	Local                 bool
}

const fmtSkippingShort = "skipping %s in short mode"

func ShortSkipf(tb testing.TB, a ...any) {
	var msg string
	if len(a) > 0 {
		msg = fmt.Sprint(a...) + ": "
	}
	msg += fmt.Sprintf(fmtSkippingShort, tb.Name())
	tb.Skip(msg)
}

func CheckSkip(tb testing.TB, args SkipTestArgs) {
	var smap *cluster.Smap
	if args.RequiresRemoteCluster && RemoteCluster.UUID == "" {
		tb.Skipf("%s requires remote cluster", tb.Name())
	}
	if args.RequiresAuth && LoggedUserToken == "" {
		tb.Skipf("%s requires authentication token", tb.Name())
	}
	if args.Long && testing.Short() {
		tb.Skipf(fmtSkippingShort, tb.Name())
	}
	if args.RemoteBck {
		proxyURL := GetPrimaryURL()
		if !isRemoteBucket(tb, proxyURL, args.Bck) {
			tb.Skipf("%s requires a remote bucket (have %q)", tb.Name(), args.Bck)
		}
	}
	if args.CloudBck {
		proxyURL := GetPrimaryURL()
		if !isCloudBucket(tb, proxyURL, args.Bck) {
			tb.Skipf("%s requires a cloud bucket", tb.Name())
		}
	}

	switch args.RequiredDeployment {
	case ClusterTypeK8s:
		// NOTE: The test suite doesn't have to be deployed on K8s, the cluster has to be.
		isK8s, err := isClusterK8s()
		if err != nil {
			tb.Fatalf("Unrecognized error upon checking K8s deployment; err: %v", err)
		}
		if !isK8s {
			tb.Skipf("%s requires Kubernetes", tb.Name())
		}
	case ClusterTypeLocal:
		isLocal, err := isClusterLocal()
		tassert.CheckFatal(tb, err)
		if !isLocal {
			tb.Skipf("%s requires local deployment", tb.Name())
		}
	case ClusterTypeDocker:
		if !docker.IsRunning() {
			tb.Skipf("%s requires docker deployment", tb.Name())
		}
	}

	if args.MinTargets > 0 || args.MinMountpaths > 0 || args.MinProxies > 0 {
		smap = GetClusterMap(tb, GetPrimaryURL())
	}

	if args.MinTargets > 0 {
		if smap.CountTargets() < args.MinTargets {
			tb.Skipf("%s requires at least %d targets (have %d)",
				tb.Name(), args.MinTargets, smap.CountTargets())
		}
	}

	if args.MinProxies > 0 {
		if smap.CountProxies() < args.MinProxies {
			tb.Skipf("%s requires at least %d proxies (have %d)",
				tb.Name(), args.MinProxies, smap.CountProxies())
		}
	}

	if args.MinMountpaths > 0 {
		targets := smap.Tmap.ActiveNodes()
		proxyURL := GetPrimaryURL()
		baseParams := BaseAPIParams(proxyURL)
		mpList, err := api.GetMountpaths(baseParams, targets[0])
		tassert.CheckFatal(tb, err)
		if l := len(mpList.Available); l < args.MinMountpaths {
			tb.Skipf("%s requires at least %d mountpaths (have %d)", tb.Name(), args.MinMountpaths, l)
		}
	}
}
