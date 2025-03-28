// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import "github.com/NVIDIA/aistore/ext/dsort/shard"

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
//

type (
	CreationPhaseMetadata struct {
		Shards    []*shard.Shard          `msg:"shards"`
		SendOrder map[string]*shard.Shard `msg:"send_order"`
	}

	RemoteResponse struct {
		Record    *shard.Record    `msg:"r"`
		RecordObj *shard.RecordObj `msg:"o"`
	}
)
