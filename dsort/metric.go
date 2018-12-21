package dsort

import (
	"sync"
	"time"
)

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
	// compute the number shards extracted to memory just substract it from
	// ExtractedCnt.
	ExtractedToDiskCnt int `json:"extracted_to_disk_count"`
	// ExtractedToDiskSize describes uncompressed size of extracted shards to disk
	// to given moment.
	ExtractedToDiskSize int64 `json:"extracted_to_disk_size"`
}

// MetaSorting contains metrics for second phase of DSort.
type MetaSorting struct {
	PhaseInfo
	// SentCnt describes number of times records has been sent from this target
	// to some other.
	SentCnt int `json:"sent_count"`
	// RecvCnt describes number of times given target has received records from
	// some other target. Final target (which contains all record) should have
	// this number bigger than other target.
	RecvCnt int `json:"recv_count"`
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
}

// Metrics is general struct which contains all stats about DSort run.
type Metrics struct {
	Extraction *LocalExtraction `json:"local_extraction,omitempty"`
	Sorting    *MetaSorting     `json:"meta_sorting,omitempty"`
	Creation   *ShardCreation   `json:"shard_creation,omitempty"`
	// Aborted specifies if the DSort has been aborted or not.
	Aborted bool `json:"aborted"`
}

// newMetrics creates new Metrics instance.
func newMetrics() *Metrics {
	return &Metrics{
		Extraction: &LocalExtraction{},
		Sorting:    &MetaSorting{},
		Creation:   &ShardCreation{},
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
