// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type Slab struct {
	m         *MMSA
	pMinDepth *atomic.Int64
	tag       string
	get       [][]byte
	put       [][]byte
	bufSize   int64
	pos       int
	idx       int
	muget     sync.Mutex
	muput     sync.Mutex
}

func (s *Slab) Size() int64 { return s.bufSize }
func (s *Slab) Tag() string { return s.tag }
func (s *Slab) MMSA() *MMSA { return s.m }

func (s *Slab) Alloc() (buf []byte) {
	s.muget.Lock()
	buf = s._alloc()
	s.muget.Unlock()
	return
}

func (s *Slab) Free(buf []byte) {
	s.muput.Lock()
	debug.Assert(int64(cap(buf)) == s.Size())
	deadbeef(buf[:cap(buf)])
	s.put = append(s.put, buf[:cap(buf)]) // always freeing the original size
	s.muput.Unlock()
}

func (s *Slab) _alloc() (buf []byte) {
	if len(s.get) > s.pos { // fast path
		buf = s.get[s.pos]
		s.pos++
		s.hitsInc()
		return
	}
	return s._allocSlow()
}

func (s *Slab) _allocSlow() (buf []byte) {
	curMinDepth := int(s.pMinDepth.Load())
	debug.Assert(curMinDepth > 0)
	debug.Assert(len(s.get) == s.pos)
	s.muput.Lock()
	lput := len(s.put)
	if cnt := (curMinDepth - lput) >> 1; cnt > 0 {
		if cmn.Rom.FastV(5, cos.SmoduleMemsys) {
			nlog.Infof("%s: grow by %d to %d, caps=(%d, %d)", s.tag, cnt, lput+cnt, cap(s.get), cap(s.put))
		}
		s.grow(cnt)
	}
	s.get, s.put = s.put, s.get

	debug.Assert(len(s.put) == s.pos)

	s.put = s.put[:0]
	s.muput.Unlock()

	s.pos = 0
	buf = s.get[s.pos]
	s.pos++
	s.hitsInc()
	return
}

func (s *Slab) grow(cnt int) {
	for ; cnt > 0; cnt-- {
		buf := make([]byte, s.Size())
		s.put = append(s.put, buf)
	}
}

func (s *Slab) reduce(todepth int) int64 {
	var pfreed, gfreed int64
	s.muput.Lock()
	lput := len(s.put)
	cnt := lput - todepth
	if cnt > 0 {
		for ; cnt > 0; cnt-- {
			lput--
			s.put[lput] = nil
			pfreed += s.Size()
		}
		s.put = s.put[:lput]
	}
	s.muput.Unlock()
	if pfreed > 0 && cmn.Rom.FastV(5, cos.SmoduleMemsys) {
		nlog.Infof("%s: reduce lput %d to %d (freed %dB)", s.tag, lput, lput-cnt, pfreed)
	}

	s.muget.Lock()
	lget := len(s.get) - s.pos
	cnt = lget - todepth
	if cnt > 0 {
		for ; cnt > 0; cnt-- {
			s.get[s.pos] = nil
			s.pos++
			gfreed += s.Size()
		}
	}
	s.muget.Unlock()
	if gfreed > 0 && cmn.Rom.FastV(5, cos.SmoduleMemsys) {
		nlog.Infof("%s: reduce lget %d to %d (freed %dB)", s.tag, lget, lget-cnt, gfreed)
	}
	return pfreed + gfreed
}

func (s *Slab) cleanup() (freed int64) {
	s.muget.Lock()
	s.muput.Lock()
	for i := s.pos; i < len(s.get); i++ {
		s.get[i] = nil
		freed += s.Size()
	}
	for i := range s.put {
		s.put[i] = nil
		freed += s.Size()
	}
	if cap(s.get) > maxDepth {
		s.get = make([][]byte, 0, optDepth)
	} else {
		s.get = s.get[:0]
	}
	if cap(s.put) > maxDepth {
		s.put = make([][]byte, 0, optDepth)
	} else {
		s.put = s.put[:0]
	}
	s.pos = 0

	debug.Assert(len(s.get) == 0 && len(s.put) == 0)
	s.muput.Unlock()
	s.muget.Unlock()
	return
}

func (s *Slab) ringIdx() int { return s.idx }
func (s *Slab) hitsInc()     { s.m.hits[s.ringIdx()].Inc() }
