// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// internal EC stats in raw format: only counters
type stats struct {
	queueLen   int64
	queueCnt   int64
	waitTime   int64
	waitCnt    int64
	encodeReq  int64
	encodeTime int64
	encodeSize int64
	encodeErr  int64
	decodeReq  int64
	decodeErr  int64
	decodeTime int64
	deleteReq  int64
	deleteTime int64
	deleteErr  int64
	objTime    int64
	objCnt     int64
}

// ECStats are stats for clients-side apps - calculated from raw counters
// All numbers except number of errors and requests are average ones
type ECStats struct {
	// mpathrunner(not ecrunner) queue len
	QueueLen float64
	// time between ecrunner receives an object and mpathrunner starts processing it
	WaitTime time.Duration
	// EC encoding time (for both EC'ed and replicated objects)
	EncodeTime time.Duration
	// size of a file put into encode queue
	EncodeSize int64
	// total number of errors while encoding objects
	EncodeErr int64
	// total number of errors while restoring objects
	DecodeErr int64
	// time to restore an object(for both EC'ed and replicated objects)
	DecodeTime time.Duration
	// time to cleanup object's slices(for both EC'ed and replicated objects)
	DeleteTime time.Duration
	// total number of errors while cleaning up object slices
	DeleteErr int64
	// total object processing time: from putting to ecrunner queue to
	// completing the request by mpathrunner
	ObjTime time.Duration
	// total number of cleanup requests
	DelReq int64
	// total number of restore requests
	GetReq int64
	// total number of encode requests
	PutReq int64
}

func (s *stats) updateQueue(l int) {
	atomic.AddInt64(&s.queueLen, int64(l))
	atomic.AddInt64(&s.queueCnt, 1)
}

func (s *stats) updateEncode(sz int64) {
	atomic.AddInt64(&s.encodeSize, sz)
	atomic.AddInt64(&s.encodeReq, 1)
}

func (s *stats) updateEncodeTime(d time.Duration, failed bool) {
	atomic.AddInt64(&s.encodeTime, int64(d))
	if failed {
		atomic.AddInt64(&s.encodeErr, 1)
	}
}

func (s *stats) updateDecode() {
	atomic.AddInt64(&s.decodeReq, 1)
}

func (s *stats) updateDecodeTime(d time.Duration, failed bool) {
	atomic.AddInt64(&s.decodeTime, int64(d))
	if failed {
		atomic.AddInt64(&s.decodeErr, 1)
	}
}

func (s *stats) updateDelete() {
	atomic.AddInt64(&s.deleteReq, 1)
}

func (s *stats) updateDeleteTime(d time.Duration, failed bool) {
	atomic.AddInt64(&s.deleteTime, int64(d))
	if failed {
		atomic.AddInt64(&s.deleteErr, 1)
	}
}

func (s *stats) updateWaitTime(d time.Duration) {
	atomic.AddInt64(&s.waitTime, int64(d))
	atomic.AddInt64(&s.waitCnt, 1)
}

func (s *stats) updateObjTime(d time.Duration) {
	atomic.AddInt64(&s.objTime, int64(d))
	atomic.AddInt64(&s.objCnt, 1)
}

func (s *stats) stats() *ECStats {
	st := &ECStats{}

	val := atomic.SwapInt64(&s.queueLen, 0)
	cnt := atomic.SwapInt64(&s.queueCnt, 0)
	if cnt > 0 {
		st.QueueLen = float64(val) / float64(cnt)
	}

	val = atomic.SwapInt64(&s.waitTime, 0)
	cnt = atomic.SwapInt64(&s.waitCnt, 0)
	if cnt > 0 {
		st.WaitTime = time.Duration(val / cnt)
	}

	val = atomic.SwapInt64(&s.encodeTime, 0)
	cnt = atomic.SwapInt64(&s.encodeReq, 0)
	sz := atomic.SwapInt64(&s.encodeSize, 0)
	if cnt > 0 {
		st.EncodeTime = time.Duration(val / cnt)
		st.EncodeSize = sz / cnt
		st.PutReq = cnt
	}

	val = atomic.SwapInt64(&s.decodeTime, 0)
	cnt = atomic.SwapInt64(&s.decodeReq, 0)
	if cnt > 0 {
		st.DecodeTime = time.Duration(val / cnt)
		st.GetReq = cnt
	}

	val = atomic.SwapInt64(&s.deleteTime, 0)
	cnt = atomic.SwapInt64(&s.deleteReq, 0)
	if cnt > 0 {
		st.DeleteTime = time.Duration(val / cnt)
		st.DelReq = cnt
	}

	val = atomic.SwapInt64(&s.objTime, 0)
	cnt = atomic.SwapInt64(&s.objCnt, 0)
	if cnt > 0 {
		st.ObjTime = time.Duration(val / cnt)
	}

	st.EncodeErr = atomic.SwapInt64(&s.encodeErr, 0)
	st.DecodeErr = atomic.SwapInt64(&s.decodeErr, 0)
	st.DeleteErr = atomic.SwapInt64(&s.deleteErr, 0)

	return st
}

func (s *ECStats) String() string {
	if s.ObjTime == 0 {
		return ""
	}

	lines := make([]string, 0, 8)
	lines = append(lines,
		fmt.Sprintf("Queue avg len: %.4f, avg wait time: %v", s.QueueLen, s.WaitTime),
		fmt.Sprintf("Avg object processing time: %v", s.ObjTime),
	)

	if s.EncodeTime != 0 {
		lines = append(lines, fmt.Sprintf("Encode avg time: %v, errors: %d, avg size: %d", s.EncodeTime, s.EncodeErr, s.EncodeSize))
	}

	if s.DecodeTime != 0 {
		lines = append(lines, fmt.Sprintf("Decode avg time: %v, errors: %d", s.DecodeTime, s.DecodeErr))
	}

	if s.DeleteTime != 0 {
		lines = append(lines, fmt.Sprintf("Delete avg time: %v, errors: %d", s.DeleteTime, s.DeleteErr))
	}

	lines = append(lines, fmt.Sprintf("Requests count: encode %d, restore %d, delete %d", s.PutReq, s.GetReq, s.DelReq))

	return strings.Join(lines, "\n")
}
