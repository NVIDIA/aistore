// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
)

// POST /v1/sort
func (p *proxy) proxyStartSortHandler(w http.ResponseWriter, r *http.Request) {
	rs := &dsort.RequestSpec{}
	if cmn.ReadJSON(w, r, &rs) != nil {
		return
	}
	parsedRS, err := rs.Parse()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	bck := cluster.CloneBck(&parsedRS.Bck)
	args := bckInitArgs{p: p, w: w, r: r, bck: bck, perms: apc.AceObjLIST | apc.AceGET}
	if _, err = args.initAndTry(); err != nil {
		return
	}

	bck = cluster.CloneBck(&parsedRS.OutputBck)
	args = bckInitArgs{p: p, w: w, r: r, bck: bck, perms: apc.AcePUT}
	if _, err = args.initAndTry(); err != nil {
		return
	}

	dsort.ProxyStartSortHandler(w, r, parsedRS)
}
