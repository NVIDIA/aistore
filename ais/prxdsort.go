// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
)

// POST /v1/sort
func (p *proxyrunner) proxyStartSortHandler(w http.ResponseWriter, r *http.Request) {
	rs := &dsort.RequestSpec{}
	if cmn.ReadJSON(w, r, &rs) != nil {
		return
	}
	parsedRS, err := rs.Parse()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	bck := cluster.NewBckEmbed(parsedRS.Bck)
	args := bckInitArgs{p: p, w: w, r: r, bck: bck, lookupRemote: true, perms: cmn.AccessObjLIST | cmn.AccessGET}
	if _, err = args.initAndTry(bck.Name); err != nil {
		return
	}

	bck = cluster.NewBckEmbed(parsedRS.OutputBck)
	args = bckInitArgs{p: p, w: w, r: r, bck: bck, lookupRemote: true, perms: cmn.AccessPUT}
	if _, err = args.initAndTry(bck.Name); err != nil {
		return
	}

	dsort.ProxyStartSortHandler(w, r, parsedRS)
}
