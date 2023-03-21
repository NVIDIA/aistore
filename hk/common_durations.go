// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import "time"

// common cleanup-related durations

const (
	DelOldIval      = 8 * time.Minute // hk interval: cleanup old
	PruneActiveIval = 2 * time.Minute // hk interval: prune active

	OldAgeLso      = time.Minute      // when list-objects is considered old
	OldAgeLsoNotif = 10 * time.Second // notifications-wise
	OldAgeX        = time.Hour        // all other X
)
