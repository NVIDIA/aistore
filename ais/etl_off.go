//go:build !etl

// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/ext/etl"
)

func etlOff(w http.ResponseWriter, r *http.Request) { cmn.WriteErr(w, r, etl.ErrNotBuilt) }

func (*proxy) etlHandler(w http.ResponseWriter, r *http.Request)                      { etlOff(w, r) }
func (*proxy) etlExists(string) error                                                 { return etl.ErrNotBuilt }
func (*target) etlHandler(w http.ResponseWriter, r *http.Request)                     { etlOff(w, r) }
func (*target) etlObjHandler(w http.ResponseWriter, r *http.Request)                  { etlOff(w, r) }
func (*target) inlineETL(w http.ResponseWriter, r *http.Request, _ *dpq, _ *core.LOM) { etlOff(w, r) }
