//go:build debug

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
)

func (t *target) setusr1() { hk.SetUSR1(t.usr1) }
func (p *proxy) setusr1()  { hk.SetUSR1(p.usr1) }

// When "odd" and "even" nodes cannot coexist together
//
// NOTE:
// - this code is triggered by `SIGUSR1` simultaneously on all nodes in the cluster;
// - it executes only locally without any inter-node synchronization;
// - hence, the sleep at the bottom.
//
// TODO:
// - rm sleep
// - randomize version bump to create scenarios

func (h *htrun) usr1() bool /*split done*/ {
	smap := h.owner.smap.get()
	if smap.CountActivePs() < 2 {
		nlog.Errorln("not enough proxies to perform the split:", smap.StringEx())
		return false
	}
	// 1. current primary port
	oport, err := strconv.ParseInt(smap.Primary.PubNet.Port, 10, 64)
	debug.AssertNoErr(err)

	// 2. deterministically select the second primary that satisfies the following condition:
	// its port must have a different parity (which is why we cannot use `meta.HrwProxy`)
	var (
		npsi *meta.Snode
		maxH uint64
	)
	for _, psi := range smap.Pmap {
		if flags := psi.Flags.Clear(meta.SnodeIC); flags != 0 {
			nlog.Warningln(h.String(), "skipping", psi.StringEx(), psi.Flags)
			continue
		}
		port, err := strconv.ParseInt(psi.PubNet.Port, 10, 64)
		debug.AssertNoErr(err)
		if port%2 != oport%2 && psi.IDDigest > maxH {
			npsi = psi
			maxH = psi.IDDigest
		}
	}
	if npsi == nil {
		nlog.Errorln(h.String(), "failed to select the second primary candidate that'd have public port with parity !=",
			oport, smap.StringEx())
		return false
	}

	nlog.Warningln(h.String(), "second primary candidate", npsi.StringEx(), "- proceeding to split "+strings.Repeat("*", 30))

	// 3. split cluster in two
	var (
		myport int64
		clone  = smap.clone()
		nodes  = []meta.NodeMap{clone.Pmap, clone.Tmap}
	)
	myport, err = strconv.ParseInt(h.si.PubNet.Port, 10, 64)
	debug.AssertNoErr(err)

	for _, nmap := range nodes {
		for id, si := range nmap {
			port, err := strconv.ParseInt(si.PubNet.Port, 10, 64)
			debug.AssertNoErr(err)

			if myport%2 != port%2 {
				delete(nmap, id)
			}
		}
	}

	clone.Version += 100 // note above
	if myport%2 != oport%2 {
		debug.Assert(clone.GetNode(npsi.ID()) != nil, clone.StringEx())
		clone.Primary = npsi
	}

	time.Sleep(time.Second) // note above

	h.owner.smap.put(clone)

	return true
}

func (p *proxy) usr1() {
	smap := p.owner.smap.get()
	opid := smap.Primary.ID()
	if !p.htrun.usr1() {
		return
	}

	nsmap := p.owner.smap.get()
	nlog.Infoln(p.String(), "split-brain", nsmap.StringEx())

	if opid != nsmap.Primary.ID() && p.SID() == nsmap.Primary.ID() {
		nlog.Infoln(p.String(), "becoming new primary")
		p.becomeNewPrimary("")
	}
}

func (t *target) usr1() {
	if t.htrun.usr1() {
		nsmap := t.owner.smap.get()
		nlog.Infoln(t.String(), "split-brain", nsmap.StringEx())
	}
}
