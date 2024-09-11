// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import "time"

// common cleanup-related durations

const (
	DelOldIval      = 24 * time.Minute // hk timer: cleanup old xactions; old transactions
	PruneActiveIval = 2 * time.Minute  // hk timer: prune active xactions; cleanup notifs

	//
	// when things are considered _old_
	//
	OldAgeLsoX     = time.Minute      // x-lso
	OldAgeX        = time.Hour        // all other xactions
	OldAgeNotif    = 3 * time.Minute  // old notifications
	OldAgeNotifLso = 10 * time.Second // note: seconds
)
