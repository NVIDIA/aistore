// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

func (t *target) objSelect(w http.ResponseWriter, _ *http.Request, lom *core.LOM, msg *cmn.SelectObjectMsg) (int, error) {
	if err := msg.Validate(); err != nil {
		return http.StatusBadRequest, err
	}

	resp := lom.GetROC(false /*latestVer*/, false /*sync*/)
	if resp.Err != nil {
		return resp.Ecode, resp.Err
	}
	defer resp.R.Close()

	w.Header().Set(cos.HdrContentType, "text/csv")
	if err := cmn.SelectObject(resp.R, w, msg); err != nil {
		return http.StatusBadRequest, err
	}
	return 0, nil
}
