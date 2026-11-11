// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// keep temporary remote endpoints alive until every node has switched routes
func setRemaisBackend(t *testing.T, bp api.BaseParams, backend *cmn.BackendConf) {
	t.Helper()
	if err := api.SetClusterConfigUsingMsg(bp, &cmn.ConfigToSet{Backend: backend}, false); err != nil {
		t.Errorf("set remote backend: %v", err)
		return
	}
	smap := tools.GetClusterMap(t, bp.URL)
	bp.URL = smap.Primary.URL(cmn.NetPublic)
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	var remotes cmn.BackendConfAIS
	tassert.CheckFatal(t, cos.MorphMarshal(backend.Get(apc.AIS), &remotes))

	var (
		pending  []string
		lastErr  error
		deadline = time.Now().Add(max(10*time.Second, 2*config.Timeout.MaxKeepalive.D()+6*config.Timeout.CplaneOperation.D()))
	)
	for {
		var nodes meta.RemAisNodes
		if nodes, lastErr = api.GetRemaisNodes(bp); lastErr == nil {
			pending = pending[:0]
			pending = remaisPending(pending, smap.Pmap, nodes.Proxy, config.Version, remotes)
			pending = remaisPending(pending, smap.Tmap, nodes.Target, config.Version, remotes)
			if len(pending) == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Errorf("remote routes did not reach config v%d (%v): %v", config.Version, remotes, lastErr)
		return
	}
	for _, s := range pending {
		t.Errorf("%s: remote routes did not reach config v%d (%v)", s, config.Version, remotes)
	}
}

// append active nodes that are yet to switch
func remaisPending(pending []string, nmap meta.NodeMap, vecs map[string]*meta.RemAisVec, ver int64, remotes cmn.BackendConfAIS) []string {
	for id, si := range nmap {
		if si.InMaintOrDecomm() {
			continue
		}
		all, ok := vecs[id]
		if !ok {
			pending = append(pending, id+": no response")
			continue
		}
		if all.Ver >= ver && remaisSwitched(all, remotes) {
			continue
		}
		pending = append(pending, id+": "+string(cos.MustMarshal(all)))
	}
	return pending
}

func remaisSwitched(all *meta.RemAisVec, remotes cmn.BackendConfAIS) bool {
	for _, rem := range all.A {
		if rem.UUID != tools.RemoteCluster.UUID {
			continue
		}
		urls := remotes[rem.Alias]
		if len(urls) == 0 {
			urls = remotes[rem.UUID]
		}
		return slices.Contains(urls, rem.URL)
	}
	return false
}
