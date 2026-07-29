// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

type forceJoinCluMeta struct {
	Smap   *meta.Smap         `json:"smap"`
	BMD    *meta.BMD          `json:"bmd"`
	RMD    *meta.RMD          `json:"rmd"`
	Config *cmn.ClusterConfig `json:"config"`
}

func TestForceJoinRequiresPrimary(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		RequiredDeployment: tools.ClusterTypeLocal,
		MinProxies:         2,
		MinTargets:         1,
	})

	smap := tools.GetClusterMap(t, tools.GetPrimaryURL())
	var secondary *meta.Snode
	for pid, psi := range smap.Pmap {
		if pid != smap.Primary.ID() {
			secondary = psi
			break
		}
	}
	tassert.Fatalf(t, secondary != nil, "failed to select a secondary proxy from %s", smap.StringEx())

	t.Run("proxy-without-identity", func(t *testing.T) {
		testForceJoinAttack(t, secondary, nil)
	})
	t.Run("target-as-cluster-member", func(t *testing.T) {
		tsi := smap.Tmap.ActiveNodes()[0]
		hdr := http.Header{
			apc.HdrSenderID:   []string{secondary.ID()},
			apc.HdrSenderName: []string{secondary.StringEx()},
		}
		testForceJoinAttack(t, tsi, hdr)
	})
}

func testForceJoinAttack(t *testing.T, victim *meta.Snode, hdr http.Header) {
	var registrations atomic.Int32
	rogue := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		registrations.Add(1)
	}))
	defer rogue.Close()

	original := getForceJoinCluMeta(t, victim.PubNet.URL)
	attack := &forceJoinCluMeta{}
	err := jsoniter.Unmarshal(cos.MustMarshal(original), attack)
	tassert.CheckFatal(t, err)
	u, err := url.Parse(rogue.URL)
	tassert.CheckFatal(t, err)
	ni := meta.NetInfo{}
	ni.Init(u.Scheme, u.Hostname(), u.Port())
	pid := attack.Smap.Primary.ID()
	attackerPrimary := attack.Smap.Primary.Clone()
	attackerPrimary.PubNet, attackerPrimary.ControlNet, attackerPrimary.DataNet = ni, ni, ni
	attack.Smap.Primary = attackerPrimary
	attack.Smap.Pmap[pid] = attackerPrimary
	attack.Smap.Version += 100
	attack.Config.Auth.Enabled = false

	t.Cleanup(func() {
		smap, err := api.GetClusterMap(tools.BaseAPIParams(victim.PubNet.URL))
		tassert.CheckFatal(t, err)
		if smap.Primary.ControlNet.URL == rogue.URL {
			tassert.CheckError(t, forceJoin(victim.ControlNet.URL, original, hdr))
		}
	})

	err = forceJoin(victim.ControlNet.URL, attack, hdr)
	got, getErr := api.GetClusterMap(tools.BaseAPIParams(victim.PubNet.URL))
	tassert.CheckFatal(t, getErr)
	if got.Primary.ControlNet.URL == rogue.URL || registrations.Load() != 0 {
		t.Fatalf("unauthenticated force-join took over %s: primary URL changed to %q, registrations=%d",
			victim.StringEx(), got.Primary.ControlNet.URL, registrations.Load())
	}
	tassert.Fatalf(t, err != nil, "expected unauthenticated force-join to be rejected by %s", victim.StringEx())
}

func getForceJoinCluMeta(t *testing.T, nodeURL string) *forceJoinCluMeta {
	bp := tools.BaseAPIParams(nodeURL)
	bp.Method = http.MethodGet
	req := api.AllocRp()
	req.BaseParams = bp
	req.Path = apc.URLPathDae.S
	req.Query = url.Values{apc.QparamWhat: []string{apc.WhatSmapVote}}
	out := &forceJoinCluMeta{}
	_, err := req.DoReqAny(out)
	api.FreeRp(req)
	tassert.CheckFatal(t, err)
	return out
}

func forceJoin(nodeURL string, cm *forceJoinCluMeta, hdr http.Header) error {
	bp := tools.BaseAPIParams(nodeURL)
	bp.Method = http.MethodPost
	bp.Token = ""
	req := api.AllocRp()
	req.BaseParams = bp
	req.Path = apc.URLPathDaeForceJoin.S
	req.Query = url.Values{apc.QparamPrepare: []string{"false"}}
	req.Header = hdr
	req.Body = cos.MustMarshal(&apc.ActMsg{Action: apc.ActPrimaryForce, Value: cm})
	err := req.DoRequest()
	api.FreeRp(req)
	return err
}
