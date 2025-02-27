// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/certloader"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

//
// API endpoint: load TLS cert
//

func (h *htrun) daeLoadX509(w http.ResponseWriter, r *http.Request) {
	if err := certloader.Load(); err != nil {
		h.writeErr(w, r, err)
	}
}

func (p *proxy) cluLoadX509(w http.ResponseWriter, r *http.Request) {
	// 1. self
	var err error
	if err = certloader.Load(); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// 2. cluster
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDaeX509.S}
	args.to = core.AllNodes
	results := p.bcastGroup(args)
	freeBcArgs(args)

	for _, res := range results {
		if res.err != nil {
			err = res.errorf("node %s: %v", res.si, res.err)
			nlog.Errorln(err)
		}
	}
	freeBcastRes(results)
	if err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) callLoadX509(w http.ResponseWriter, r *http.Request, node *meta.Snode, smap *smapX) {
	cargs := allocCargs()
	cargs.si = node
	cargs.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathDaeX509.S}
	res := p.call(cargs, smap)

	err := res.unwrap()
	freeCargs(cargs)
	freeCR(res)
	if err != nil {
		p.writeErr(w, r, err)
	}
}
