// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Head-batch (hdb): POST /v1/objects/<bucket-name> with
// ActMsg{Action: apc.ActHeadBatch, Value: HdbReq}. See apc.HdbResp for the response.
//
// The receiver compares `in.CheckEq(lom.ObjAttrs())` while holding the object lock.

// The sender must fall back to per-object HeadObjT2T when peers don't support it.
var ErrHdbUnsupported = errors.New("head-batch: unsupported by peer")

func IsErrHdbUnsupported(err error) bool { return errors.Is(err, ErrHdbUnsupported) }

type (
	HdbIn struct {
		ObjAttrs        // the source object
		Name     string `json:"name"`
	}
	HdbReq struct {
		In []HdbIn `json:"in"` // 1 <= len <= apc.HdbSizeMax
	}
)

// interface guard
var _ cos.OAH = (*HdbIn)(nil)

func (req *HdbReq) Validate() error {
	const tag = "head-batch"
	if len(req.In) == 0 {
		return errors.New(tag + ": empty request")
	}
	if len(req.In) > apc.HdbSizeMax {
		return fmt.Errorf(tag+": too many items (%d), expecting at most %d", len(req.In), apc.HdbSizeMax)
	}
	for i := range req.In {
		if err := cos.ValidateOname(req.In[i].Name); err != nil {
			return fmt.Errorf("%s: item %d: %w", tag, i, err)
		}
	}
	return nil
}
