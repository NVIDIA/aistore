// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	Stats struct {
		Objs     int64 `json:"loc-objs,string" msg:"o"` // locally processed
		Bytes    int64 `json:"loc-bytes,string" msg:"b"`
		OutObjs  int64 `json:"out-objs,string" msg:"oo"` // transmit
		OutBytes int64 `json:"out-bytes,string" msg:"ob"`
		InObjs   int64 `json:"in-objs,string" msg:"io"` // receive
		InBytes  int64 `json:"in-bytes,string" msg:"ib"`
	}

	Snap struct {
		// Deprecated: reserved for emergency xaction-specific additions; unused since introduction.
		// Excluded from msgpack wire; JSON form preserved for backward compat.
		Ext any `json:"ext" msg:"-"`

		// common static props
		StartTime time.Time `json:"start-time" msg:"s"`
		EndTime   time.Time `json:"end-time" msg:"e"`
		Bck       cmn.Bck   `json:"bck" msg:"b"`
		SrcBck    cmn.Bck   `json:"src-bck" msg:"sb"`
		DstBck    cmn.Bck   `json:"dst-bck" msg:"db"`
		ID        string    `json:"id" msg:"i"`
		Kind      string    `json:"kind" msg:"k"`

		// initiating control msg + optional xaction-specific runtime stats
		CtlMsg string `json:"ctlmsg,omitempty" msg:"m,omitempty"`

		// extended error info
		AbortErr string `json:"abort-err" msg:"ae"`
		Err      string `json:"err" msg:"r"`

		// packed field: number of workers, et al.
		Packed int64 `json:"glob.id,string" msg:"p"`

		// common runtime: stats counters (above) and state
		Stats    Stats `json:"stats" msg:"x"`
		AbortedX bool  `json:"aborted" msg:"a"`
		IdleX    bool  `json:"is_idle" msg:"l"`
	}
)
