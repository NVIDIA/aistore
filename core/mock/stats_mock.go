// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
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
func (*StatsTracker) Inc(string)                                                {}
func (*StatsTracker) IncWith(string, map[string]string)                         {}
func (*StatsTracker) IncBck(string, *cmn.Bck)                                   {}
func (*StatsTracker) Add(string, int64)                                         {}
func (*StatsTracker) SetFlag(string, cos.NodeStateFlags)                        {}
func (*StatsTracker) ClrFlag(string, cos.NodeStateFlags)                        {}
func (*StatsTracker) SetClrFlag(string, cos.NodeStateFlags, cos.NodeStateFlags) {}
func (*StatsTracker) AddWith(...cos.NamedVal64)                                 {}
func (*StatsTracker) RegExtMetric(*meta.Snode, string, string, *stats.Extra)    {}
func (*StatsTracker) GetMetricNames() cos.StrKVs                                { return nil }
func (*StatsTracker) GetStats() *stats.Node                                     { return nil }
func (*StatsTracker) ResetStats(bool)                                           {}
func (*StatsTracker) PromHandler() http.Handler                                 { return nil }
