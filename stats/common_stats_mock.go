// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	TrackerMock struct{}
)

// interface guard
var _ Tracker = (*TrackerMock)(nil)

func NewTrackerMock() Tracker {
	return &TrackerMock{}
}

func (*TrackerMock) StartedUp() bool            { return true }
func (*TrackerMock) Add(string, int64)          {}
func (*TrackerMock) Get(string) int64           { return 0 }
func (*TrackerMock) AddErrorHTTP(string, int64) {}
func (*TrackerMock) AddMany(...cos.NamedVal64)  {}
func (*TrackerMock) RegMetrics(*cluster.Snode)  {}
func (*TrackerMock) CoreStats() *CoreStats      { return nil }
func (*TrackerMock) GetWhatStats() interface{}  { return nil }
func (*TrackerMock) IsPrometheus() bool         { return false }
