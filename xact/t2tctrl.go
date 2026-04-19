// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

// POST /v1/xactions/{one of the constants below}
const (
	T2TCtrl = "t2tctrl" // see also: proxyToContTCO
)

// POST /v1/xactions/"t2tctrl"/{xkind}/{xid}/{wid}/{opcode}

const NumT2TCtrlItems = 5

// TODO: add "abortWI" and "abortXact", more - later
const (
	OpcodeStartedWI = "startedWI"
)

const NoneWID = "-" // wid is optional, may not be present

func JoinCtrlPath(xact core.Xact, wid, opcode string) string {
	return apc.URLPathXactions.Join(T2TCtrl, xact.Kind(), xact.ID(), cos.Left(wid, NoneWID), opcode)
}
