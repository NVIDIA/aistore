// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// dsort (distributed sort) is removed; keep the endpoint for older CLI binaries
// that still list dsort jobs (`ais show job`, completions, etc.)
// TODO: remove one release later

const dsortRemoved = "dsort is removed and no longer supported"

func (*proxy) regDsortStub(handlers []networkHandler) []networkHandler {
	return append(handlers, networkHandler{r: apc.Sort, h: dsortStubHandler, net: accessNetPublic})
}

func dsortStubHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// always return an empty list
		w.Header().Set(cos.HdrContentType, cos.ContentJSON)
		w.Write([]byte("[]"))
	default:
		http.Error(w, dsortRemoved, http.StatusNotImplemented)
	}
}
