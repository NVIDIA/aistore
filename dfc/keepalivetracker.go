/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

// This file has different implementations of KeepaliveTracker.
import (
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

const (
	heartbeatType = "heartbeat"
	averageType   = "average"
)

var (
	_ KeepaliveTracker = &HeartBeatTracker{}
	_ KeepaliveTracker = &AverageTracker{}
)

// HeartBeatTracker tracks the timestamp of the last time a message is received from a server.
// Timeout: a message is not received within the interval.
type HeartBeatTracker struct {
	ch       chan struct{}
	last     map[string]time.Time
	interval time.Duration // expected to hear from the server within the interval
	statsdC  *statsd.Client
}

// ValidKeepaliveType returns true if the keepalive type is supported.
func ValidKeepaliveType(t string) bool {
	return t == heartbeatType || t == averageType
}

// NewKeepaliveTracker returns a keepalive tracker based on the parameters given.
func newKeepaliveTracker(c *keepaliveTrackerConf, statsdC *statsd.Client) KeepaliveTracker {
	switch c.Name {
	case heartbeatType:
		return newHeartBeatTracker(c.Interval, statsdC)
	case averageType:
		return newAverageTracker(c.Factor, statsdC)
	}
	return nil
}

// newHeartBeatTracker returns a HeartBeatTracker.
func newHeartBeatTracker(interval time.Duration, statsdC *statsd.Client) *HeartBeatTracker {
	hb := &HeartBeatTracker{
		last:     make(map[string]time.Time),
		ch:       make(chan struct{}, 1),
		statsdC:  statsdC,
		interval: interval,
	}

	hb.unlock()
	return hb
}

func (hb *HeartBeatTracker) lock() {
	<-hb.ch
}

func (hb *HeartBeatTracker) unlock() {
	hb.ch <- struct{}{}
}

// HeardFrom is called to indicate a keepalive message (or equivalent) has been received from a server.
func (hb *HeartBeatTracker) HeardFrom(id string, reset bool) {
	hb.lock()
	last, ok := hb.last[id]
	t := time.Now()
	hb.last[id] = t
	hb.unlock()

	if ok {
		delta := t.Sub(last)
		hb.statsdC.Send("keepalive.heartbeat."+id,
			metric{statsd.Gauge, "delta", int64(delta / time.Millisecond)},
			metric{statsd.Counter, "count", 1})
	} else {
		hb.statsdC.Send("keepalive.heartbeat."+id, metric{statsd.Counter, "count", 1})
	}
}

// TimedOut returns true if it has determined that it has not heard from the server.
func (hb *HeartBeatTracker) TimedOut(id string) bool {
	hb.lock()
	t, ok := hb.last[id]
	hb.unlock()
	return !ok || time.Since(t) > hb.interval
}

// AverageTracker keeps track of the average latency of all messages.
// Timeout: last received is more than the 'factor' of current average.
type AverageTracker struct {
	ch      chan struct{}
	rec     map[string]averageTrackerRecord
	factor  int
	statsdC *statsd.Client
}

type averageTrackerRecord struct {
	count   int64
	last    time.Time
	totalMS int64 // in ms
}

func (rec *averageTrackerRecord) avg() int64 {
	return rec.totalMS / rec.count
}

// newAverageTracker returns an AverageTracker.
func newAverageTracker(factor int, statsdC *statsd.Client) *AverageTracker {
	a := &AverageTracker{
		rec:     make(map[string]averageTrackerRecord),
		ch:      make(chan struct{}, 1),
		statsdC: statsdC,
		factor:  factor,
	}

	a.unlock()
	return a
}

func (a *AverageTracker) lock() {
	<-a.ch
}

func (a *AverageTracker) unlock() {
	a.ch <- struct{}{}
}

// HeardFrom is called to indicate a keepalive message (or equivalent) has been received from a server.
func (a *AverageTracker) HeardFrom(id string, reset bool) {
	a.lock()
	var rec averageTrackerRecord
	rec, ok := a.rec[id]
	if reset || !ok {
		a.rec[id] = averageTrackerRecord{count: 0, totalMS: 0, last: time.Now()}
		a.unlock()
		a.statsdC.Send("keepalive.average."+id, metric{statsd.Counter, "reset", 1})
		return
	}

	t := time.Now()
	delta := t.Sub(rec.last)
	rec.last = t
	rec.count++
	rec.totalMS += int64(delta / time.Millisecond)
	a.rec[id] = rec
	a.unlock()

	a.statsdC.Send("keepalive.average."+id,
		metric{statsd.Counter, "delta", int64(delta / time.Millisecond)},
		metric{statsd.Counter, "count", 1})
}

// TimedOut returns true if it has determined that is has not heard from the server.
func (a *AverageTracker) TimedOut(id string) bool {
	a.lock()
	rec, ok := a.rec[id]
	a.unlock()

	if !ok {
		return true
	}
	if rec.count == 0 {
		return false
	}

	return int64(time.Since(rec.last)/time.Millisecond) > int64(a.factor)*rec.avg()
}
