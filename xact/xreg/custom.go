// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	TCBArgs struct {
		BckFrom   *meta.Bck
		BckTo     *meta.Bck
		Msg       *apc.TCBMsg
		Phase     string
		DisableDM bool
	}
	TCOArgs struct {
		BckFrom   *meta.Bck
		BckTo     *meta.Bck
		Msg       *apc.TCOMsg
		DisableDM bool
	}
	DsortArgs struct {
		BckFrom *meta.Bck
		BckTo   *meta.Bck
	}
	ECEncodeArgs struct {
		Phase   string
		Recover bool
	}
	BckRenameArgs struct {
		TCBArgs
	}
	MNCArgs struct {
		Tag    string
		Copies int
	}
	RechunkArgs struct {
		ObjSizeLimit int64
		ChunkSize    int64
		Prefix       string
	}
	LsoArgs struct {
		Msg *apc.LsoMsg
		Hdr http.Header
	}
	ResArgs struct {
		Config            *cmn.Config
		Smap              *meta.Smap
		SkipGlobMisplaced bool
	}
	RebArgs struct {
		Bck    *meta.Bck // (limited-scope)
		Prefix string    // (ditto)
		Flags  uint32    // = xact.ArgsMsg.Flags
	}
)
