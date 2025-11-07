// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"

	jsoniter "github.com/json-iterator/go"
)

var Conf = &authn.Config{}

func (h *hserv) configHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.httpConfigGet(w, r)
	case http.MethodPut:
		h.httpConfigPut(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPut, http.MethodGet)
	}
}

func (h *hserv) httpConfigGet(w http.ResponseWriter, r *http.Request) {
	if err := h.validateAdminPerms(w, r); err != nil {
		return
	}
	Conf.Lock()
	writeJSON(w, Conf, "get config")
	Conf.Unlock()
}

func (h *hserv) httpConfigPut(w http.ResponseWriter, r *http.Request) {
	if err := h.validateAdminPerms(w, r); err != nil {
		return
	}
	updateCfg := &authn.ConfigToUpdate{}
	if err := jsoniter.NewDecoder(r.Body).Decode(updateCfg); err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}

	Conf.Lock()
	err := Conf.ApplyUpdate(updateCfg)
	Conf.Unlock()
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}

	if err := jsp.SaveMeta(configPath, Conf, nil); err != nil {
		cmn.WriteErr(w, r, err)
	}
}
