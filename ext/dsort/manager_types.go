// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import "github.com/NVIDIA/aistore/ext/dsort/extract"

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
//

type (
	CreationPhaseMetadata struct {
		Shards    []*extract.Shard          `msg:"shards"`
		SendOrder map[string]*extract.Shard `msg:"send_order"`
	}

	RemoteResponse struct {
		Record    *extract.Record    `msg:"r"`
		RecordObj *extract.RecordObj `msg:"o"`
	}
)
