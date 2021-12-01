// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import "github.com/NVIDIA/aistore/dsort/extract"

//go:generate msgp -tests=false -marshal=false

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
