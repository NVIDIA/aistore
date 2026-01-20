//go:build !dsort

// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */

// For respective implementations, see ais/dsort_hdl.go

package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/kvdb"
)

//
// proxy
//

const dsortNotEnabled = "dsort not enabled (build with -tags dsort)"

func (*proxy) regDsort(handlers []networkHandler) []networkHandler {
	return append(handlers, networkHandler{r: apc.Sort, h: dsortStubHandler, net: accessNetPublic})
}
func (*proxy) initDsort(*cmn.Config) {}

func dsortStubHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// stub always returns empty list
		w.Header().Set(cos.HdrContentType, cos.ContentJSON)
		w.Write([]byte("[]"))
	default:
		http.Error(w, dsortNotEnabled, http.StatusNotImplemented)
	}
}

//
// target
//

func (*target) initDsort(kvdb.Driver, *cmn.Config)                  {}
func (*target) regDsort(handlers []networkHandler) []networkHandler { return handlers }
