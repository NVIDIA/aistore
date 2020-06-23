// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
)

// internal EC stats in raw format: only counters
type stats struct {
	bck        cmn.Bck
	queueLen   atomic.Int64
	queueCnt   atomic.Int64
	waitTime   atomic.Int64
	waitCnt    atomic.Int64
	encodeReq  atomic.Int64
	encodeTime atomic.Int64
	encodeSize atomic.Int64
	encodeErr  atomic.Int64
	decodeReq  atomic.Int64
	decodeErr  atomic.Int64
	decodeTime atomic.Int64
	deleteReq  atomic.Int64
	deleteTime atomic.Int64
	deleteErr  atomic.Int64
	objTime    atomic.Int64
	objCnt     atomic.Int64
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
	// name of the bucket
	Bck cmn.Bck
}

func (s *stats) updateQueue(l int) {
	s.queueLen.Add(int64(l))
	s.queueCnt.Inc()
}

func (s *stats) updateEncode(size int64) {
	s.encodeSize.Add(size)
	s.encodeReq.Inc()
}

func (s *stats) updateEncodeTime(d time.Duration, failed bool) {
	s.encodeTime.Add(int64(d))
	if failed {
		s.encodeErr.Inc()
	}
}

func (s *stats) updateDecode() {
	s.decodeReq.Inc()
}

func (s *stats) updateDecodeTime(d time.Duration, failed bool) {
	s.decodeTime.Add(int64(d))
	if failed {
		s.decodeErr.Inc()
	}
}

func (s *stats) updateDelete() {
	s.deleteReq.Inc()
}

func (s *stats) updateDeleteTime(d time.Duration, failed bool) {
	s.deleteTime.Add(int64(d))
	if failed {
		s.deleteErr.Inc()
	}
}

func (s *stats) updateWaitTime(d time.Duration) {
	s.waitTime.Add(int64(d))
	s.waitCnt.Inc()
}

func (s *stats) updateObjTime(d time.Duration) {
	s.objTime.Add(int64(d))
	s.objCnt.Inc()
}

func (s *stats) stats() *ECStats {
	st := &ECStats{Bck: s.bck}

	val := s.queueLen.Swap(0)
	cnt := s.queueCnt.Swap(0)
	if cnt > 0 {
		st.QueueLen = float64(val) / float64(cnt)
	}

	val = s.waitTime.Swap(0)
	cnt = s.waitCnt.Swap(0)
	if cnt > 0 {
		st.WaitTime = time.Duration(val / cnt)
	}

	val = s.encodeTime.Swap(0)
	cnt = s.encodeReq.Swap(0)
	sz := s.encodeSize.Swap(0)
	if cnt > 0 {
		st.EncodeTime = time.Duration(val / cnt)
		st.EncodeSize = sz / cnt
		st.PutReq = cnt
	}

	val = s.decodeTime.Swap(0)
	cnt = s.decodeReq.Swap(0)
	if cnt > 0 {
		st.DecodeTime = time.Duration(val / cnt)
		st.GetReq = cnt
	}

	val = s.deleteTime.Swap(0)
	cnt = s.deleteReq.Swap(0)
	if cnt > 0 {
		st.DeleteTime = time.Duration(val / cnt)
		st.DelReq = cnt
	}

	val = s.objTime.Swap(0)
	cnt = s.objCnt.Swap(0)
	if cnt > 0 {
		st.ObjTime = time.Duration(val / cnt)
	}

	st.EncodeErr = s.encodeErr.Swap(0)
	st.DecodeErr = s.decodeErr.Swap(0)
	st.DeleteErr = s.deleteErr.Swap(0)

	return st
}

func (s *ECStats) String() string {
	if s.ObjTime == 0 {
		return ""
	}

	lines := make([]string, 0, 8)
	lines = append(lines,
		fmt.Sprintf("EC stats for bucket %s", s.Bck),
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
