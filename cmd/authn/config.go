// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
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
	if err := checkAuthorization(w, r); err != nil {
		return
	}
	Conf.RLock()
	defer Conf.RUnlock()
	writeJSON(w, Conf, "config")
}

func httpConfigPut(w http.ResponseWriter, r *http.Request) {
	if err := checkAuthorization(w, r); err != nil {
		return
	}
	updateCfg := &authn.ConfigToUpdate{}
	if err := jsoniter.NewDecoder(r.Body).Decode(updateCfg); err != nil {
		cmn.WriteErrMsg(w, r, "Invalid request")
		return
	}
	if err := Conf.ApplyUpdate(updateCfg); err != nil {
		cmn.WriteErr(w, r, err)
	}
}
