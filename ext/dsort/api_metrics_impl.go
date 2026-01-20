//go:build sharding

// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math"
	"time"
)

///////////////
// phaseBase //
///////////////

// begin marks phase as in progress.
func (pi *phaseBase) begin() {
	pi.mu.Lock()
	pi.Running = true
	pi.Start = time.Now()
	pi.mu.Unlock()
}

// finish marks phase as finished.
func (pi *phaseBase) finish() {
	pi.mu.Lock()
	pi.Running = false
	pi.Finished = true
	pi.End = time.Now()
	pi.Elapsed = pi.End.Sub(pi.Start)
	pi.mu.Unlock()
}

/////////////
// Metrics //
/////////////

func newMetrics(description string) *Metrics {
	return &Metrics{
		Description: description,
		Extraction:  &LocalExtraction{},
		Sorting: &MetaSorting{
			SentStats: newTimeStats(),
			RecvStats: newTimeStats(),
		},
		Creation: &ShardCreation{},
	}
}

// setAbortedTo updates aborted state of Dsort.
func (m *Metrics) setAbortedTo(b bool) {
	m.Aborted.Store(b)
}

// Lock locks all phases to make sure that all of them can be updated.
func (m *Metrics) lock() {
	m.Extraction.mu.Lock()
	m.Sorting.mu.Lock()
	m.Creation.mu.Lock()
}

// Unlock unlocks all phases.
func (m *Metrics) unlock() {
	m.Creation.mu.Unlock()
	m.Sorting.mu.Unlock()
	m.Extraction.mu.Unlock()
}

func (m *Metrics) ElapsedTime() time.Duration {
	return m.Creation.End.Sub(m.Extraction.Start)
}

// update updates elapsed time for all the metrics.
// NOTE: must be done under lock every time Metrics are about to be marshaled and sent through the network.
func (m *Metrics) update() {
	if m.Extraction.End.IsZero() && !m.Extraction.Start.IsZero() {
		m.Extraction.Elapsed = time.Since(m.Extraction.Start)
	}
	if m.Sorting.End.IsZero() && !m.Sorting.Start.IsZero() {
		m.Sorting.Elapsed = time.Since(m.Sorting.Start)
	}
	if m.Creation.End.IsZero() && !m.Creation.Start.IsZero() {
		m.Creation.Elapsed = time.Since(m.Creation.Start)
	}
}

func (m *Metrics) ToJobInfo(id string, pars *parsedReqSpec) JobInfo {
	return JobInfo{
		ID:                id,
		SrcBck:            pars.InputBck,
		DstBck:            pars.OutputBck,
		StartedTime:       m.Extraction.Start,
		FinishTime:        m.Creation.End,
		ExtractedDuration: m.Extraction.Elapsed,
		SortingDuration:   m.Sorting.Elapsed,
		CreationDuration:  m.Creation.Elapsed,
		Objs:              m.Extraction.ExtractedCnt,
		Bytes:             m.Extraction.ExtractedSize,
		Metrics:           m,
		Aborted:           m.Aborted.Load(),
		Archived:          m.Archived.Load(),
	}
}

//
// utility
//

func newTimeStats() *TimeStats {
	return &TimeStats{
		MinMs: math.MaxInt64,
	}
}

func (ts *TimeStats) updateTime(newTime time.Duration) {
	t := newTime.Nanoseconds() / int64(time.Millisecond)
	ts.Total += t
	ts.Count++
	ts.MinMs = min(ts.MinMs, t)
	ts.MaxMs = max(ts.MaxMs, t)
	ts.AvgMs = ts.Total / ts.Count
}
