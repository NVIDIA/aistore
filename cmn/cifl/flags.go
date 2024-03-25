// Package cifl: cluster information and flags
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cifl

import "github.com/NVIDIA/aistore/cmn/cos"

type Flags cos.BitFlags

const (
	VoteInProgress = Flags(1 << iota)
	ClusterStarted
	NodeStarted
	Rebalancing
	RebalanceInterrupted
	Resilvering
	ResilverInterrupted
	Restarted
	OOS
	OOM
)

func (f Flags) IsSet(flag Flags) bool   { return cos.BitFlags(f).IsSet(cos.BitFlags(flag)) }
func (f Flags) Set(flags Flags) Flags   { return Flags(cos.BitFlags(f).Set(cos.BitFlags(flags))) }
func (f Flags) Clear(flags Flags) Flags { return Flags(cos.BitFlags(f).Clear(cos.BitFlags(flags))) }
