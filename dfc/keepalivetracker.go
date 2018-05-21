/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

// This file has different implementations of KeepaliveTracker.
// ideally this file stays under dfcpub, but currently dfc has a flat structure, following that
// for now.
import (
	"time"
)

// HeartBeatTracker tracks timestamp of the last a message is received from a server.
// Timeout: a message is not received within the interval.
type HeartBeatTracker struct {
	ch       chan struct{}
	last     map[string]time.Time
	interval time.Duration // expected to hear from the server within the interval
}

// IsKeepaliveTypeSupported returns true if the keepalive type is supported
func IsKeepaliveTypeSupported(t string) bool {
	return t == "heartbeat" || t == "average"
}

// NewKeepaliveTracker returns a keepalive tracker based on the parameters given
func NewKeepaliveTracker(c *keepaliveTrackerConf) KeepaliveTracker {
	switch c.Name {
	case "heartbeat":
		return newHeartBeatTracker(c.Max)
	case "average":
		return newAverageTracker(c.Factor)
	default:
		return nil
	}
}

// newHeartBeatTracker returns a HeartBeatTracker
func newHeartBeatTracker(interval time.Duration) *HeartBeatTracker {
	hb := &HeartBeatTracker{last: make(map[string]time.Time), ch: make(chan struct{}, 1)}
	hb.interval = interval
	hb.unlock()
	return hb
}

func (hb *HeartBeatTracker) lock() {
	<-hb.ch
}

func (hb *HeartBeatTracker) unlock() {
	hb.ch <- struct{}{}
}

// HeardFrom is called to indicate a keepalive message (or equivalent) is received from a server
func (hb *HeartBeatTracker) HeardFrom(id string, reset bool) {
	hb.lock()
	defer hb.unlock()

	hb.last[id] = time.Now()
}

// TimedOut returns true if it is determined that have not heard from the server
func (hb *HeartBeatTracker) TimedOut(id string) bool {
	hb.lock()
	defer hb.unlock()

	t, ok := hb.last[id]
	return !ok || time.Since(t) > hb.interval
}

// AverageTracker keeps track of the average latency of all messages.
// Timeout: last received is more than the 'factor' of current average.
type AverageTracker struct {
	ch     chan struct{}
	rec    map[string]averageTrackerRecord
	factor int
}

type averageTrackerRecord struct {
	cnt     int64
	last    time.Time
	totalMS int64 // in ms
}

func (rec *averageTrackerRecord) avg() int64 {
	return rec.totalMS / rec.cnt
}

// newAverageTracker returns a AverageTracker
func newAverageTracker(factor int) *AverageTracker {
	a := &AverageTracker{rec: make(map[string]averageTrackerRecord), ch: make(chan struct{}, 1)}
	a.factor = factor
	a.unlock()
	return a
}

func (a *AverageTracker) lock() {
	<-a.ch
}

func (a *AverageTracker) unlock() {
	a.ch <- struct{}{}
}

// HeardFrom is called to indicate a keepalive message (or equivalent) is received from a server
func (a *AverageTracker) HeardFrom(id string, reset bool) {
	a.lock()
	defer a.unlock()

	var rec averageTrackerRecord
	rec, ok := a.rec[id]
	if reset || !ok {
		a.rec[id] = averageTrackerRecord{cnt: 0, totalMS: 0, last: time.Now()}
		return
	}

	t := time.Now()
	delta := t.Sub(rec.last)
	rec.last = t
	rec.cnt++
	rec.totalMS += int64(delta / time.Millisecond)
	a.rec[id] = rec
}

// TimedOut returns true if it is determined that have not heard from the server
func (a *AverageTracker) TimedOut(id string) bool {
	a.lock()
	defer a.unlock()

	rec, ok := a.rec[id]
	if !ok {
		return true
	}

	if rec.cnt == 0 {
		return false
	}

	return int64(time.Now().Sub(rec.last)/time.Millisecond) > int64(a.factor)*rec.avg()
}

var (
	_ KeepaliveTracker = &HeartBeatTracker{}
	_ KeepaliveTracker = &AverageTracker{}
)
