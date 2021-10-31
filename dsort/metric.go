// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

const (
	ExtractionPhase = "extraction"
	SortingPhase    = "sorting"
	CreationPhase   = "creation"
)

// TimeStats contains statistics about time spent on specific task. It calculates
// min, max and avg times.
type TimeStats struct {
	// Total contains total number of milliseconds spend on
	// specific task.
	Total int64 `json:"total_ms,string"`
	// Count contains number of time specific task was triggered.
	Count int64 `json:"count,string"`
	MinMs int64 `json:"min_ms,string"`
	MaxMs int64 `json:"max_ms,string"`
	AvgMs int64 `json:"avg_ms,string"`
}

// ThroughputStats contains statistics about throughput of specific task.
type ThroughputStats struct {
	total int64
	count int64

	MinTp int64 `json:"min_throughput,string"`
	MaxTp int64 `json:"max_throughput,string"`
	AvgTp int64 `json:"avg_throughput,string"`
}

// DetailedStats contains time and throughput statistics .
type DetailedStats struct {
	*TimeStats
	*ThroughputStats
}

func newTimeStats() *TimeStats {
	return &TimeStats{
		MinMs: math.MaxInt64,
	}
}

func (ts *TimeStats) updateTime(newTime time.Duration) {
	t := newTime.Nanoseconds() / int64(time.Millisecond)
	ts.Total += t
	ts.Count++
	ts.MinMs = cos.MinI64(ts.MinMs, t)
	ts.MaxMs = cos.MaxI64(ts.MaxMs, t)
	ts.AvgMs = ts.Total / ts.Count
}

func newThroughputStats() *ThroughputStats {
	return &ThroughputStats{
		MinTp: math.MaxInt64,
	}
}

func (tps *ThroughputStats) updateThroughput(size int64, dur time.Duration) {
	throughput := int64(float64(size) / dur.Seconds())

	tps.total += throughput
	tps.count++
	tps.MinTp = cos.MinI64(tps.MinTp, throughput)
	tps.MaxTp = cos.MaxI64(tps.MaxTp, throughput)
	tps.AvgTp = tps.total / tps.count
}

func newDetailedStats() *DetailedStats {
	return &DetailedStats{
		newTimeStats(),
		newThroughputStats(),
	}
}

// PhaseInfo contains general stats and state for given phase. It is base struct
// which is extended by actual phases structs.
type PhaseInfo struct {
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

// begin marks phase as in progress.
func (pi *PhaseInfo) begin() {
	pi.mu.Lock()
	pi.Running = true
	pi.Start = time.Now()
	pi.mu.Unlock()
}

// finish marks phase as finished.
func (pi *PhaseInfo) finish() {
	pi.mu.Lock()
	pi.Running = false
	pi.Finished = true
	pi.End = time.Now()
	pi.Elapsed = pi.End.Sub(pi.Start) / time.Second
	pi.mu.Unlock()
}

// LocalExtraction contains metrics for first phase of DSort.
type LocalExtraction struct {
	PhaseInfo
	// TotalCnt is the number of shards DSort has to process in total.
	TotalCnt int64 `json:"total_count,string"`
	// ExtractedCnt describes number of extracted shards to given moment. At the
	// end, number should be roughly equal to TotalCnt/#Targets.
	ExtractedCnt int64 `json:"extracted_count,string"`
	// ExtractedSize describes uncompressed size of extracted shards to given moment.
	ExtractedSize int64 `json:"extracted_size,string"`
	// ExtractedRecordCnt describes number of records extracted from all shards.
	ExtractedRecordCnt int64 `json:"extracted_record_count,string"`
	// ExtractedToDiskCnt describes number of shards extracted to the disk. To
	// compute the number shards extracted to memory just subtract it from
	// ExtractedCnt.
	ExtractedToDiskCnt int64 `json:"extracted_to_disk_count,string"`
	// ExtractedToDiskSize describes uncompressed size of extracted shards to disk
	// to given moment.
	ExtractedToDiskSize int64 `json:"extracted_to_disk_size,string"`
	// ShardExtractionStats describes time statistics about single shard extraction.
	ShardExtractionStats *DetailedStats `json:"single_shard_stats,omitempty"`
}

// MetaSorting contains metrics for second phase of DSort.
type MetaSorting struct {
	PhaseInfo
	// SentStats describes time statistics about records sending to another target
	SentStats *TimeStats `json:"sent_stats,omitempty"`
	// RecvStats describes time statistics about records receiving from another target
	RecvStats *TimeStats `json:"recv_stats,omitempty"`
}

// ShardCreation contains metrics for third and last phase of DSort.
type ShardCreation struct {
	PhaseInfo
	// ToCreate specifies number of shards that have to be created in this phase.
	ToCreate int64 `json:"to_create,string"`
	// CreatedCnt specifies the number of shards that have been so far created.
	// Should match ToCreate when phase is finished.
	CreatedCnt int64 `json:"created_count,string"`
	// MovedShardCnt specifies the number of shards that have migrated from this
	// to another target in the cluster. Applies only when dealing with compressed
	// data. Sometimes it is faster to create a shard on a specific target and send it
	// over (rather than creating on a destination target).
	MovedShardCnt int64 `json:"moved_shard_count,string"`
	// RequestStats describes time statistics about request to other target.
	RequestStats *TimeStats `json:"req_stats,omitempty"`
	// ResponseStats describes time statistics about response to other target.
	ResponseStats *TimeStats `json:"resp_stats,omitempty"`
	// LocalSendStats describes time statistics about sending record content to other target.
	LocalSendStats *DetailedStats `json:"local_send_stats,omitempty"`
	// LocalRecvStats describes time statistics about receiving record content from other target.
	LocalRecvStats *DetailedStats `json:"local_recv_stats,omitempty"`
	// ShardCreationStats describes time statistics about single shard creation.
	ShardCreationStats *DetailedStats `json:"single_shard_stats,omitempty"`
}

// Metrics is general struct which contains all stats about DSort run.
type Metrics struct {
	Extraction *LocalExtraction `json:"local_extraction,omitempty"`
	Sorting    *MetaSorting     `json:"meta_sorting,omitempty"`
	Creation   *ShardCreation   `json:"shard_creation,omitempty"`

	// Aborted specifies if the DSort has been aborted or not.
	Aborted atomic.Bool `json:"aborted,omitempty"`
	// Archived specifies if the DSort has been archived to persistent storage.
	Archived atomic.Bool `json:"archived,omitempty"`

	// Description of the job.
	Description string `json:"description,omitempty"`

	// Warnings which were produced during the job.
	Warnings []string `json:"warnings,omitempty"`
	// Errors which happened during the job.
	Errors []string `json:"errors,omitempty"`

	// extended determines if we should calculate and send extended metrics like
	// request/response times.
	extended bool
}

// newMetrics creates new Metrics instance.
func newMetrics(description string, extended bool) *Metrics {
	extraction := &LocalExtraction{}
	sorting := &MetaSorting{}
	creation := &ShardCreation{}

	if extended {
		extraction.ShardExtractionStats = newDetailedStats()

		creation.RequestStats = newTimeStats()
		creation.ResponseStats = newTimeStats()
		creation.LocalSendStats = newDetailedStats()
		creation.LocalRecvStats = newDetailedStats()
		creation.ShardCreationStats = newDetailedStats()
	}

	sorting.SentStats = newTimeStats()
	sorting.RecvStats = newTimeStats()

	return &Metrics{
		extended: extended,

		Description: description,

		Extraction: extraction,
		Sorting:    sorting,
		Creation:   creation,
	}
}

// setAbortedTo updates aborted state of DSort.
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
//
// NOTE: This should be done every time Metrics are about to be marshaled and
// sent through the network.
func (m *Metrics) update() {
	m.lock()
	if m.Extraction.End.IsZero() && !m.Extraction.Start.IsZero() {
		m.Extraction.Elapsed = time.Since(m.Extraction.Start) / time.Second
	}
	if m.Sorting.End.IsZero() && !m.Sorting.Start.IsZero() {
		m.Sorting.Elapsed = time.Since(m.Sorting.Start) / time.Second
	}
	if m.Creation.End.IsZero() && !m.Creation.Start.IsZero() {
		m.Creation.Elapsed = time.Since(m.Creation.Start) / time.Second
	}
	m.unlock()
}

func (m *Metrics) Marshal() []byte {
	m.lock()
	b, err := jsoniter.Marshal(m)
	m.unlock()
	cos.AssertNoErr(err)
	return b
}

// JobInfo is a struct that contains stats that represent the DSort run in a list
type JobInfo struct {
	ID string `json:"id"`

	StartedTime time.Time `json:"started_time,omitempty"`
	FinishTime  time.Time `json:"finish_time,omitempty"`

	ExtractedDuration time.Duration `json:"started_meta_sorting,omitempty"`
	SortingDuration   time.Duration `json:"started_shard_creation,omitempty"`
	CreationDuration  time.Duration `json:"finished_shard_creation,omitempty"`

	Aborted  bool `json:"aborted"`
	Archived bool `json:"archived"`

	Description string `json:"description"`
}

func (m *Metrics) ToJobInfo(id string) JobInfo {
	return JobInfo{
		ID: id,

		StartedTime: m.Extraction.Start,
		FinishTime:  m.Creation.End,

		ExtractedDuration: m.Extraction.Elapsed,
		SortingDuration:   m.Sorting.Elapsed,
		CreationDuration:  m.Creation.Elapsed,

		Aborted:     m.Aborted.Load(),
		Archived:    m.Archived.Load(),
		Description: m.Description,
	}
}

func (j *JobInfo) Aggregate(other *JobInfo) {
	j.StartedTime = startTime(j.StartedTime, other.StartedTime)
	j.FinishTime = stopTime(j.FinishTime, other.FinishTime)

	j.ExtractedDuration = cos.MaxDuration(j.ExtractedDuration, other.ExtractedDuration)
	j.SortingDuration = cos.MaxDuration(j.SortingDuration, other.SortingDuration)
	j.CreationDuration = cos.MaxDuration(j.CreationDuration, other.CreationDuration)

	j.Aborted = j.Aborted || other.Aborted
	j.Archived = j.Archived && other.Archived
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
