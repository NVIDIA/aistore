// Package authn is authentication server for AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
)

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items []string) ([]string, error) {
	items, err := cmn.MatchItems(r.URL.Path, itemsAfter, true, items)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return nil, err
	}
	return items, err
}
