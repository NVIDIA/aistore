// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/nlog"
	aistls "github.com/NVIDIA/aistore/cmn/tls"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

func (h *htrun) daeLoadX509(w http.ResponseWriter, r *http.Request) {
	if err := aistls.Load(); err != nil {
		h.writeErr(w, r, err)
	}
}

func (p *proxy) cluLoadX509(w http.ResponseWriter, r *http.Request) {
	// 1. self
	var err error
	if err = aistls.Load(); err != nil {
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
