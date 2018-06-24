/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"crypto/md5"
	"fmt"
	"hash"
	"net/http"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

// Manager maintains all the state required for a single run of a distributed archive file shuffle.
type Manager struct {
	SortedRecords      []Record
	ExtractCreater     extractCreater
	TargetURL          string
	Shards             []Shard
	StartShardCreation chan struct{}

	sortAlgo             sortAlgorithm
	fileExtension        string
	sumCompressionRatios float64 // Total compressed size / total uncompressed size
	totalInputShardsSeen int
	received             int   // Number of FileMeta slices received, defining what step in the sort a target is in.
	maxMemUsagePercent   int64 // On a per-target basis, default: 80
	inProgress           bool
	extractionPaths      sync.Map // Keys correspond to all paths to record contents on disk.
	mu                   sync.Mutex
}

type extractCreater interface {
	ExtractShard(f *os.File, toDisk bool, extractionPath string) ([]Record, int64, error)
	CreateShard(s Shard, shardFQN string) (int64, error)
	UsingCompression() bool
	RecordContents() *sync.Map
}

// Shard represents the metadata required to construct a single shard (aka an archive file).
type Shard struct {
	Name    string   `json:"name"`
	Bucket  string   `json:"bucket"`
	IsLocal bool     `json:"is_local"`
	Size    int64    `json:"size"`
	Records []Record `json:"records"`
}

// Record represents the metadata corresponding to a single file from an archive file.
type Record struct {
	Size      int64  `json:"size"`
	Key       string `json:"key"` // Used to determine the sorting order.
	Name      string `json:"name"`
	TargetURL string `json:"target_url"` // URL of the target which maintains the contents for this record.
	// Location on disk where the contents are stored. Doubles as the key for extractCreater's RecordContents.
	PathToContents string `json:"path_to_contents"`
	Header         `json:"header"`
}

// Header represents a single record's file metadata. The fields here are taken from tar.Header.
// It is very costly to marshal and unmarshal time.Time to and from JSON, so all time.Time fields are omitted.
// Furthermore, the time.Time fields are updated upon creating the new tarballs, so there is no need to
// maintain the original values.
type Header struct {
	Typeflag byte // Type of header entry (should be TypeReg for most files)

	Name     string // Name of file entry
	Linkname string // Target name of link (valid for TypeLink or TypeSymlink)

	Size  int64  // Logical file size in bytes
	Mode  int64  // Permission and mode bits
	Uid   int    // User ID of owner
	Gid   int    // Group ID of owner
	Uname string // User name of owner
	Gname string // Group name of owner
}

// Init cleans up previous state in the Manager and initializes all necessary fields.
func (m *Manager) Init(rs RequestSpec, c *http.Client, targetURL string) error {
	// Retroactively clean up leftover file content data from previous runs to allocate resources for this run.
	now := time.Now()
	m.SortedRecords = nil
	m.ExtractCreater = nil
	m.extractionPaths.Range(func(k, v interface{}) bool {
		if err := os.RemoveAll(k.(string)); err != nil {
			glog.Errorf("could not remove extraction path from previous run, err: %v", err)
		}
		return true
	})
	debug.FreeOSMemory()
	glog.Infof(
		"took %v to clean up file contents from memory and disk from previous dsort request", time.Since(now))

	if err := m.setExtractCreater(rs, c, targetURL); err != nil {
		return err
	}
	m.Shards = make([]Shard, 0)
	m.StartShardCreation = make(chan struct{}, 1)
	m.SortedRecords = make([]Record, 0)
	m.sortAlgo = rs.Algorithm
	m.fileExtension = rs.Extension
	m.sumCompressionRatios = 0
	m.received = 0
	m.totalInputShardsSeen = 0
	m.TargetURL = targetURL
	if rs.MaxMemUsagePercent == 0 {
		m.maxMemUsagePercent = 80
	} else {
		m.maxMemUsagePercent = rs.MaxMemUsagePercent
	}
	return nil
}

// setExtractCreater sets what type of file extraction and creation is used based on the RequestSpec.
func (m *Manager) setExtractCreater(rs RequestSpec, c *http.Client, targetURL string) error {
	var (
		key         func(string, hash.Hash) string
		extractSema chan struct{}
		createSema  chan struct{}
	)
	if rs.Algorithm.Kind == sortKindMD5 {
		key = keyMD5
	} else {
		key = keyIdentity
	}
	if rs.ExtractConcLimit > 0 {
		extractSema = make(chan struct{}, rs.ExtractConcLimit)
	}
	if rs.CreateConcLimit > 0 {
		createSema = make(chan struct{}, rs.CreateConcLimit)
	}
	switch rs.Extension {
	case ".tar", ".tgz", ".tar.gz":
		m.ExtractCreater = &tarExtractCreater{
			recordContents: &sync.Map{},
			targetURL:      targetURL,
			gzipped:        rs.Extension != ".tar",
			h:              md5.New(),
			key:            key,
			client:         c,
			extractSema:    extractSema,
			createSema:     createSema,
		}
	default:
		return fmt.Errorf("invalid extension: %s, must be .tar, .tgz, or .tar.gz", rs.Extension)
	}
	return nil
}

// MergeSortedRecords sets m.SortedRecords to the sorted result of itself and sortedRecords.
// This assumes both m.SortedRecords and sortedRecords are already sorted.
func (m *Manager) MergeSortedRecords(sortedRecords []Record) {
	n1 := len(m.SortedRecords)
	n2 := len(sortedRecords)
	if n1 == 0 {
		m.SortedRecords = sortedRecords
		return
	}
	var i, j int
	temp := make([]Record, 0, n1+n2)
	r1 := m.SortedRecords
	r2 := sortedRecords

	for i < n1 && j < n2 {
		if m.sortAlgo.Decreasing {
			if r1[i].Key > r2[j].Key {
				temp = append(temp, r1[i])
				i += 1
			} else {
				temp = append(temp, r2[j])
				j += 1
			}
		} else {
			if r1[i].Key < r2[j].Key {
				temp = append(temp, r1[i])
				i += 1
			} else {
				temp = append(temp, r2[j])
				j += 1
			}
		}
	}
	for i < n1 {
		temp = append(temp, r1[i])
		i += 1
	}
	for j < n2 {
		temp = append(temp, r2[j])
		j += 1
	}
	m.SortedRecords = temp
}

func (m *Manager) IncrementReceived() {
	m.received += 1
}

func (m *Manager) Received() int {
	return m.received
}

func (m *Manager) AddToSumCompressionRatios(ratio float64) {
	m.sumCompressionRatios += ratio
}

func (m *Manager) SumCompressionRatios() float64 {
	return m.sumCompressionRatios
}

func (m *Manager) TotalInputShardsSeen() int {
	return m.totalInputShardsSeen
}

func (m *Manager) AddToTotalInputShardsSeen(seen int) {
	m.totalInputShardsSeen += seen
}

func (m *Manager) InProgress() bool {
	return m.inProgress
}

func (m *Manager) SetInProgressTo(b bool) {
	m.inProgress = b
}

func (m *Manager) Lock() {
	m.mu.Lock()
}

func (m *Manager) Unlock() {
	m.mu.Unlock()
}

func (m *Manager) FileExtension() string {
	return m.fileExtension
}

func (m *Manager) MaxMemUsagePercent() int64 {
	return m.maxMemUsagePercent
}

func (m *Manager) AddExtractionPath(path string) {
	m.extractionPaths.Store(path, struct{}{})
}
