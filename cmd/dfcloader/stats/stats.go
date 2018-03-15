// Package stats is used for keeping track of IO stats including number of ops,
// latency, throughput, etc. Assume single threaded access, it doesn't provide any locking on updates.
package stats

import (
	"math"
	"time"
)

// Stats records accumulated puts/gets information.
type Stats struct {
	start      time.Time     // time current stats started
	puts       int64         // total puts
	putBytes   int64         // total bytes by all puts
	errPuts    int64         // number of failed puts
	putLatency time.Duration // Accumulated put latency
	gets       int64         // total gets
	getBytes   int64         // total bytes by all gets
	errGets    int64         // number of failed gets
	getLatency time.Duration // Accumulated get latency

	// self maintained fields
	minPutLatency time.Duration
	maxPutLatency time.Duration
	minGetLatency time.Duration
	maxGetLatency time.Duration
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a >= b {
		return a
	}

	return b
}

// NewStats returns a new stats object with given time as the starting point
func NewStats(t time.Time) Stats {
	return Stats{
		start:         t,
		minPutLatency: time.Duration(math.MaxInt64),
		minGetLatency: time.Duration(math.MaxInt64),
	}
}

// NewStatsNow returns a new stats object using current time as the starting point
func NewStatsNow() Stats {
	return NewStats(time.Now())
}

// AddPut adds a put to the stats
func (s *Stats) AddPut(size int64, delta time.Duration) {
	s.puts++
	s.putBytes += size
	s.putLatency += delta
	s.minPutLatency = minDuration(s.minPutLatency, delta)
	s.maxPutLatency = maxDuration(s.maxPutLatency, delta)
}

// AddErrPut increases the number of failed put count by 1
func (s *Stats) AddErrPut() {
	s.errPuts++
}

// TotalPuts returns the total number of puts.
func (s *Stats) TotalPuts() int64 {
	return s.puts
}

// TotalPutBytes returns the total number of bytes by all puts.
func (s *Stats) TotalPutBytes() int64 {
	return s.putBytes
}

// MinPutLatency returns the minimal put latency in nano second.
func (s *Stats) MinPutLatency() int64 {
	if s.puts == 0 {
		return 0
	}
	return int64(s.minPutLatency)
}

// MaxPutLatency returns the maximal put latency in nano second.
func (s *Stats) MaxPutLatency() int64 {
	if s.puts == 0 {
		return 0
	}
	return int64(s.maxPutLatency)
}

// AvgPutLatency returns the avg put latency in nano second.
func (s *Stats) AvgPutLatency() int64 {
	if s.puts == 0 {
		return 0
	}
	return int64(s.putLatency) / s.puts
}

// PutThroughput returns throughput of puts (puts/per second).
func (s *Stats) PutThroughput(t time.Time) int64 {
	if s.start == t {
		return 0
	}
	return int64(float64(s.putBytes) / t.Sub(s.start).Seconds())
}

// TotalErrPuts returns the total number of failed puts.
func (s *Stats) TotalErrPuts() int64 {
	return s.errPuts
}

// AddGet adds a get to the stats
func (s *Stats) AddGet(size int64, delta time.Duration) {
	s.gets++
	s.getBytes += size
	s.getLatency += delta
	s.minGetLatency = minDuration(s.minGetLatency, delta)
	s.maxGetLatency = maxDuration(s.maxGetLatency, delta)
}

// AddErrGet increases the number of failed get count by 1
func (s *Stats) AddErrGet() {
	s.errGets++
}

// TotalGets returns the total number of gets.
func (s *Stats) TotalGets() int64 {
	return s.gets
}

// TotalGetBytes returns the total number of bytes by all gets.
func (s *Stats) TotalGetBytes() int64 {
	return s.getBytes
}

// MinGetLatency returns the min get latency in nano second.
func (s *Stats) MinGetLatency() int64 {
	if s.gets == 0 {
		return 0
	}
	return int64(s.minGetLatency)
}

// MaxGetLatency returns the max get latency in nano second.
func (s *Stats) MaxGetLatency() int64 {
	if s.gets == 0 {
		return 0
	}
	return int64(s.maxGetLatency)
}

// AvgGetLatency returns the avg get latency in nano second.
func (s *Stats) AvgGetLatency() int64 {
	if s.gets == 0 {
		return 0
	}
	return int64(s.getLatency) / s.gets
}

// GetThroughput returns throughput of gets (gets/per second).
func (s *Stats) GetThroughput(t time.Time) int64 {
	if s.start == t {
		return 0
	}
	return int64(float64(s.getBytes) / t.Sub(s.start).Seconds())
}

// TotalErrGets returns the total number of failed gets.
func (s *Stats) TotalErrGets() int64 {
	return s.errGets
}

// Aggregate adds another stats to self
func (s *Stats) Aggregate(other Stats) {
	s.puts += other.puts
	s.putBytes += other.putBytes
	s.errPuts += other.errPuts
	s.putLatency += other.putLatency

	s.gets += other.gets
	s.getBytes += other.getBytes
	s.errGets += other.errGets
	s.getLatency += other.getLatency

	s.minPutLatency = minDuration(s.minPutLatency, other.minPutLatency)
	s.maxPutLatency = maxDuration(s.maxPutLatency, other.maxPutLatency)
	s.minGetLatency = minDuration(s.minGetLatency, other.minGetLatency)
	s.maxGetLatency = maxDuration(s.maxGetLatency, other.maxGetLatency)
}
