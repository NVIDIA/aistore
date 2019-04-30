package dsort

import (
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	ExtractionPhase = "extraction"
	SortingPhase    = "sorting"
	CreationPhase   = "creation"
)

// TimeStats contains statistics about time spent on specific task. It calculates
// min, max and avg times.
type TimeStats struct {
	// total contains total number of milliseconds spend on
	// specific task.
	Total int64 `json:"total_ms"`
	// Count contains number of time specific task was triggered.
	Count int64 `json:"count"`
	Min   int64 `json:"min_ms"`
	Max   int64 `json:"max_ms"`
	Avg   int64 `json:"avg_ms"`
}

func newTimeStats() *TimeStats {
	return &TimeStats{
		Min: math.MaxInt64,
	}
}

func (ts *TimeStats) update(newTime time.Duration) {
	t := newTime.Nanoseconds() / int64(time.Millisecond)
	ts.Total += t
	ts.Count++
	ts.Min = cmn.MinI64(ts.Min, t)
	ts.Max = cmn.MaxI64(ts.Max, t)
	ts.Avg = ts.Total / ts.Count
}

// PhaseInfo contains general stats and state for given phase. It is base struct
// which is extended by actual phases structs.
type PhaseInfo struct {
	sync.Mutex `json:"-"`
	Start      time.Time `json:"started_time"`
	End        time.Time `json:"end_time"`
	// Elapsed time (in seconds) from start to given point of time or end when
	// phase has finished.
	Elapsed time.Duration `json:"elapsed"`
	// Running specifies if phase is in progress.
	Running bool `json:"running"`
	// Finished specifies if phase has finished. If running and finished is
	// false this means that the phase did not have started yet.
	Finished bool `json:"finished"`
}

// begin marks phase as in progress.
func (pi *PhaseInfo) begin() {
	pi.Lock()
	pi.Running = true
	pi.Start = time.Now()
	pi.Unlock()
}

// finish marks phase as finished.
func (pi *PhaseInfo) finish() {
	pi.Lock()
	pi.Running = false
	pi.Finished = true
	pi.End = time.Now()
	pi.Elapsed = pi.End.Sub(pi.Start) / time.Second
	pi.Unlock()
}

// LocalExtraction contains metrics for first phase of DSort.
type LocalExtraction struct {
	PhaseInfo
	// SeenCnt describes number of shards which are in preparation to be either
	// skipped if they do not belong to given target or processed. It should
	// match ToSeenCnt when phase is finished.
	SeenCnt int `json:"seen_count"`
	// ToSeenCnt is the number of shards DSort has to process in total.
	ToSeenCnt int `json:"to_seen_count"`
	// ExtractedCnt describes number of extracted shards to given moment. At the
	// end, number should be roughly equal to ToSeenCnt/#Targets.
	ExtractedCnt int `json:"extracted_count"`
	// ExtractedSize describes uncompressed size of extracted shards to given moment.
	ExtractedSize int64 `json:"extracted_size"`
	// ExtractedRecordCnt describes number of records extracted from all shards.
	ExtractedRecordCnt int `json:"extracted_record_count"`
	// ExtractedToDiskCnt describes number of shards extracted to the disk. To
	// compute the number shards extracted to memory just subtract it from
	// ExtractedCnt.
	ExtractedToDiskCnt int `json:"extracted_to_disk_count"`
	// ExtractedToDiskSize describes uncompressed size of extracted shards to disk
	// to given moment.
	ExtractedToDiskSize int64 `json:"extracted_to_disk_size"`
	// ShardExtractionStats describes time statistics about single shard extraction.
	ShardExtractionStats *TimeStats `json:"single_shard_stats,omitempty"`
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
	ToCreate int `json:"to_create"`
	// CreatedCnt specifies number of shards that have been created to given
	// moment. Should match ToCreate when phase is finished.
	CreatedCnt int `json:"created_count"`
	// MovedShardCnt describes number of shards that have been moved from this
	// target to some other. This only applies when dealing with compressed
	// data. Sometimes is faster to create shard on specific target and send it
	// via network than create shard on destination target.
	MovedShardCnt int `json:"moved_shard_count"`
	// RequestStats describes time statistics about request to other target.
	RequestStats *TimeStats `json:"req_stats,omitempty"`
	// ResponseStats describes time statistics about response to other target.
	ResponseStats *TimeStats `json:"resp_stats,omitempty"`
	// ShardCreationStats describes time statistics about single shard creation.
	ShardCreationStats *TimeStats `json:"single_shard_stats,omitempty"`
}

// Metrics is general struct which contains all stats about DSort run.
type Metrics struct {
	Extraction *LocalExtraction `json:"local_extraction,omitempty"`
	Sorting    *MetaSorting     `json:"meta_sorting,omitempty"`
	Creation   *ShardCreation   `json:"shard_creation,omitempty"`

	// Aborted specifies if the DSort has been aborted or not.
	Aborted bool `json:"aborted"`
	// Archived specifies if the DSort has been archived to persistent storage.
	Archived bool `json:"archived,omitempty"`

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
func newMetrics(extended bool) *Metrics {
	extraction := &LocalExtraction{}
	sorting := &MetaSorting{}
	creation := &ShardCreation{}

	if extended {
		extraction.ShardExtractionStats = newTimeStats()

		creation.RequestStats = newTimeStats()
		creation.ResponseStats = newTimeStats()
		creation.ShardCreationStats = newTimeStats()
	}

	sorting.SentStats = newTimeStats()
	sorting.RecvStats = newTimeStats()

	return &Metrics{
		extended: extended,

		Extraction: extraction,
		Sorting:    sorting,
		Creation:   creation,
	}
}

// setAbortedTo updates aborted state of DSort.
func (m *Metrics) setAbortedTo(b bool) {
	m.Aborted = b
}

// Lock locks all phases to make sure that all of them can be updated.
func (m *Metrics) lock() {
	m.Extraction.Lock()
	m.Sorting.Lock()
	m.Creation.Lock()
}

// Unlock unlocks all phases.
func (m *Metrics) unlock() {
	m.Creation.Unlock()
	m.Sorting.Unlock()
	m.Extraction.Unlock()
}

// update updates elapsed time for all the metrics.
//
// NOTE: This should be done every time Metrics are about to be marshalled and
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

		Aborted:     m.Aborted,
		Archived:    m.Archived,
		Description: m.Description,
	}
}

func (lhs *JobInfo) Aggregate(rhs JobInfo) {
	lhs.StartedTime = startTime(lhs.StartedTime, rhs.StartedTime)
	lhs.FinishTime = stopTime(lhs.FinishTime, rhs.FinishTime)

	lhs.ExtractedDuration = cmn.MaxDuration(lhs.ExtractedDuration, rhs.ExtractedDuration)
	lhs.SortingDuration = cmn.MaxDuration(lhs.SortingDuration, rhs.SortingDuration)
	lhs.CreationDuration = cmn.MaxDuration(lhs.CreationDuration, rhs.CreationDuration)

	lhs.Aborted = lhs.Aborted || rhs.Aborted
	lhs.Archived = lhs.Archived && rhs.Archived
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
