// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

type (
	TrackerMock struct{}
)

// interface guard
var _ Tracker = &TrackerMock{}

func NewTrackerMock() Tracker {
	return &TrackerMock{}
}

func (*TrackerMock) StartedUp() bool                       { return true }
func (*TrackerMock) Add(name string, val int64)            {}
func (*TrackerMock) Get(name string) int64                 { return 0 }
func (*TrackerMock) AddErrorHTTP(method string, val int64) {}
func (*TrackerMock) AddMany(namedVal64 ...NamedVal64)      {}
func (*TrackerMock) RegisterAll()                          {}
func (*TrackerMock) CoreStats() *CoreStats                 { return nil }
func (*TrackerMock) GetWhatStats() interface{}             { return nil }
