// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
)

const (
	ExtractionPhase = "extraction"
	SortingPhase    = "sorting"
	CreationPhase   = "creation"
)

// internals
type (
	// TimeStats contains statistics about time spent on specific task. It calculates
	// min, max and avg times.
	TimeStats struct {
		// Total contains total number of milliseconds spend on
		// specific task.
		Total int64 `json:"total_ms,string"`
		// Count contains number of time specific task was triggered.
		Count int64 `json:"count,string"`
		MinMs int64 `json:"min_ms,string"`
		MaxMs int64 `json:"max_ms,string"`
		AvgMs int64 `json:"avg_ms,string"`
	}

	// included by 3 actual phases below
	phaseBase struct {
		Start time.Time `json:"started_time"`
		End   time.Time `json:"end_time"`
		// Elapsed time (in seconds) from start to given point of time or end when
		// phase has finished.
		Elapsed time.Duration `json:"elapsed"`
		// Running specifies if phase is in progress.
		Running bool `json:"running"`
		// Finished specifies if phase has finished. If running and finished is
		// false this means that the phase did not have started yet.
		Finished bool `json:"finished"`
		//
		// private
		//
		mu sync.Mutex `json:"-"`
	}
)

// phases
type (
	// LocalExtraction contains metrics for first phase of Dsort.
	LocalExtraction struct {
		phaseBase
		// TotalCnt is the number of shards Dsort has to process in total.
		TotalCnt int64 `json:"total_count,string"`
		// ExtractedCnt is the cumulative number of extracted shards. In the
		// end, this should be roughly equal to TotalCnt/#Targets.
		ExtractedCnt int64 `json:"extracted_count,string"`
		// ExtractedSize is uncompressed size of extracted shards.
		ExtractedSize int64 `json:"extracted_size,string"`
		// ExtractedRecordCnt - number of records extracted from all shards.
		ExtractedRecordCnt int64 `json:"extracted_record_count,string"`
		// ExtractedToDiskCnt describes number of shards extracted to the disk. To
		// compute the number shards extracted to memory just subtract it from
		// ExtractedCnt.
		ExtractedToDiskCnt int64 `json:"extracted_to_disk_count,string"`
		// ExtractedToDiskSize - uncompressed size of shards extracted to disk.
		ExtractedToDiskSize int64 `json:"extracted_to_disk_size,string"`
	}

	// MetaSorting contains metrics for second phase of Dsort.
	MetaSorting struct {
		phaseBase
		// SentStats - time statistics about records sent to another target
		SentStats *TimeStats `json:"sent_stats,omitempty"`
		// RecvStats - time statistics about records receivied from another target
		RecvStats *TimeStats `json:"recv_stats,omitempty"`
	}

	// ShardCreation contains metrics for third and last phase of Dsort.
	ShardCreation struct {
		phaseBase
		// ToCreate - number of shards that to be created in this phase.
		ToCreate int64 `json:"to_create,string"`
		// CreatedCnt the number of shards that have been so far created.
		// Should match ToCreate when phase finishes.
		CreatedCnt int64 `json:"created_count,string"`
		// MovedShardCnt specifies the number of shards that have migrated from this
		// to another target. Applies only when dealing with compressed
		// data. Sometimes, rather than creating at the destination, it is faster
		// to create a shard on a specific target and send it over (to the destination).
		MovedShardCnt int64 `json:"moved_shard_count,string"`
		// RequestStats - time statistics: requests to other targets.
		RequestStats *TimeStats `json:"req_stats,omitempty"`
		// ResponseStats - time statistics: responses to other targets.
		ResponseStats *TimeStats `json:"resp_stats,omitempty"`
	}
)

// main stats-and-status types
type (
	// Metrics is general struct which contains all stats about Dsort run.
	Metrics struct {
		Extraction *LocalExtraction `json:"local_extraction,omitempty"`
		Sorting    *MetaSorting     `json:"meta_sorting,omitempty"`
		Creation   *ShardCreation   `json:"shard_creation,omitempty"`

		// job description
		Description string `json:"description,omitempty"`

		// warnings during the run
		Warnings []string `json:"warnings,omitempty"`
		// errors, if any
		Errors []string `json:"errors,omitempty"`

		// has been aborted
		Aborted atomic.Bool `json:"aborted,omitempty"`
		// has been archived to persistent storage
		Archived atomic.Bool `json:"archived,omitempty"`
	}

	// JobInfo is a struct that contains stats that represent the Dsort run in a list
	JobInfo struct {
		ID                string        `json:"id"` // job ID == xact ID (aka managerUUID)
		SrcBck            cmn.Bck       `json:"src-bck"`
		DstBck            cmn.Bck       `json:"dst-bck"`
		StartedTime       time.Time     `json:"started_time,omitempty"`
		FinishTime        time.Time     `json:"finish_time,omitempty"`
		ExtractedDuration time.Duration `json:"started_meta_sorting,omitempty"`
		SortingDuration   time.Duration `json:"started_shard_creation,omitempty"`
		CreationDuration  time.Duration `json:"finished_shard_creation,omitempty"`
		Objs              int64         `json:"loc-objs,string"`  // locally processed
		Bytes             int64         `json:"loc-bytes,string"` //
		Metrics           *Metrics
		Aborted           bool `json:"aborted"`
		Archived          bool `json:"archived"`
	}
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

/////////////
// JobInfo //
/////////////

func (j *JobInfo) Aggregate(other *JobInfo) {
	j.StartedTime = startTime(j.StartedTime, other.StartedTime)
	j.FinishTime = stopTime(j.FinishTime, other.FinishTime)

	j.ExtractedDuration = max(j.ExtractedDuration, other.ExtractedDuration)
	j.SortingDuration = max(j.SortingDuration, other.SortingDuration)
	j.CreationDuration = max(j.CreationDuration, other.CreationDuration)

	j.Aborted = j.Aborted || other.Aborted
	j.Archived = j.Archived && other.Archived

	j.Objs += other.Objs
	j.Bytes += other.Bytes
}

func (j *JobInfo) IsRunning() bool {
	return !j.Aborted && !j.Archived
}

func (j *JobInfo) IsFinished() bool {
	return !j.IsRunning()
}

// startTime returns the start time of a,b. If either is zero, the other takes precedence.
func startTime(a, b time.Time) time.Time {
	if (a.Before(b) && !a.IsZero()) || b.IsZero() {
		return a
	}
	return b
}

// stopTime returns the stop time of a,b. If either is zero it's unknown and returns 0.
func stopTime(a, b time.Time) time.Time {
	if (a.After(b) && !b.IsZero()) || a.IsZero() {
		return a
	}
	return b
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
