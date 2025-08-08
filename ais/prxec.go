// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
)

func (p *proxy) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.httpecpost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPost)
	}
}

// +gen:endpoint POST /v1/ec/open-ec-streams
// +gen:endpoint POST /v1/ec/close-ec-streams
// +gen:endpoint POST /v1/ec/open-shared-dm
// +gen:endpoint POST /v1/ec/close-shared-dm
// Enable or disable erasure coding and shared data management
func (p *proxy) httpecpost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathEC.L, 1, false)
	if err != nil {
		return
	}
	action := apiItems[0]
	switch action {
	case apc.ActOpenEC:
		p.ec.setActive(mono.NanoTime())
	case apc.ActCloseEC:
		p.ec.setActive(0)

	// TODO: refactor as post-toggle-shared-streams
	case apc.ActOpenSDM:
		p.dm.setActive(mono.NanoTime())
	case apc.ActCloseSDM:
		p.dm.setActive(0)

	default:
		p.writeErr(w, r, errActEc(action))
	}
}

//
// primary action: on | off
//

func (p *proxy) onEC(bck *meta.Bck) error {
	if !bck.Props.EC.Enabled {
		return nil
	}
	return p.ec.on(p, p.ec.timeout())
}
