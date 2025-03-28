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

func configHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		httpConfigGet(w, r)
	case http.MethodPut:
		httpConfigPut(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPut, http.MethodGet)
	}
}

func httpConfigGet(w http.ResponseWriter, r *http.Request) {
	if err := validateAdminPerms(w, r); err != nil {
		return
	}
	Conf.Lock()
	writeJSON(w, Conf, "get config")
	Conf.Unlock()
}

func httpConfigPut(w http.ResponseWriter, r *http.Request) {
	if err := validateAdminPerms(w, r); err != nil {
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
