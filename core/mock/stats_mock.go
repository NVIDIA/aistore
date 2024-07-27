// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

type (
	StatsTracker struct{}
)

// interface guard
var _ stats.Tracker = (*StatsTracker)(nil)

func NewStatsTracker() stats.Tracker {
	return &StatsTracker{}
}

func (*StatsTracker) StartedUp() bool                                           { return true }
func (*StatsTracker) Get(string) int64                                          { return 0 }
func (*StatsTracker) IncErr(string)                                             {}
func (*StatsTracker) Inc(string)                                                {}
func (*StatsTracker) Add(string, int64)                                         {}
func (*StatsTracker) SetFlag(string, cos.NodeStateFlags)                        {}
func (*StatsTracker) SetClrFlag(string, cos.NodeStateFlags, cos.NodeStateFlags) {}
func (*StatsTracker) AddMany(...cos.NamedVal64)                                 {}
func (*StatsTracker) InitPrometheus(*meta.Snode)                                {}
func (*StatsTracker) RegExtMetric(*meta.Snode, string, string)                  {}
func (*StatsTracker) GetMetricNames() cos.StrKVs                                { return nil }
func (*StatsTracker) GetStats() *stats.Node                                     { return nil }
func (*StatsTracker) GetStatsV322() *stats.NodeV322                             { return nil }
func (*StatsTracker) ResetStats(bool)                                           {}
func (*StatsTracker) IsPrometheus() bool                                        { return false }
