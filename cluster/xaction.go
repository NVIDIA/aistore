// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

type (
	XactID interface {
		String() string
		Int() int64
		Compare(string) int // -1 = less, 0 = equal, +1 = greater
	}

	Xact interface {
		Run() error
		ID() XactID
		Kind() string
		Bck() cmn.Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		String() string
		Finished() bool
		Aborted() bool
		AbortedAfter(time.Duration) bool
		ChanAbort() <-chan struct{}
		IsMountpathXact() bool
		Result() (interface{}, error)
		Stats() XactStats

		// modifiers
		Abort()
		AddNotif(n Notif)
	}

	XactStats interface {
		ID() string
		Kind() string
		Bck() cmn.Bck
		StartTime() time.Time
		EndTime() time.Time
		ObjCount() int64
		BytesCount() int64
		Aborted() bool
		Running() bool
		Finished() bool
	}
)
