// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
)

type sentinel struct {
	r      core.Xact
	base   *xact.Base
	config *cmn.Config
	smap   *meta.Smap
	pend   struct {
		mu   sync.Mutex
		last map[string]int64 // pending peer => last reply
		p    []string         // peers to poll (quiesce goroutine)
		i    int64            // last poll interval
	}
	nat    int
	txDone atomic.Bool // this target: all outbound data sent and received by peers
}

func (s *sentinel) init(r core.Xact, base *xact.Base, dm *bundle.DM, config *cmn.Config, smap *meta.Smap, nat int) {
	s.r = r
	s.base = base
	s.config = config
	s.smap = smap
	s.nat = nat
	if nat <= 1 {
		return // single-target (no peers to coordinate with)
	}
	if dm == nil {
		return
	}
	debug.Assert(config != nil)
	s.pend.last = make(map[string]int64, nat-1)
	for tid := range smap.Tmap {
		if tid == core.T.SID() || smap.InMaintOrDecomm(tid) {
			continue
		}
		s.pend.last[tid] = 0
	}
}

func (s *sentinel) cleanup() {
	s.pend.mu.Lock()
	s.pend.last = nil
	s.pend.mu.Unlock()
	s.pend.p = nil
}

func (s *sentinel) bcastDone(uuid string, dm *bundle.DM) {
	o := transport.AllocSend()
	o.Hdr.Opcode = transport.OpcDone
	if uuid != "" {
		o.Hdr.Opaque = cos.UnsafeB(uuid)
	}
	err := dm.Bcast(o, nil /*roc*/)

	switch {
	case err != nil:
		nlog.WarningDepth(1, s.r.Name(), err)
	default:
		if cmn.Rom.V(4, cos.ModXs) {
			nlog.Infoln(s.r.Name(), "done")
		}
	}
}

func (s *sentinel) initLast(now int64) {
	s.pend.mu.Lock()
	for tid := range s.pend.last {
		s.pend.last[tid] = now
	}
	s.pend.mu.Unlock()
}

// Qival returns the quiesce check interval based on config
func (s *sentinel) qival() time.Duration {
	return cos.ClampDuration(s.config.Timeout.MaxHostBusy.D(), 10*time.Second, time.Minute)
}

func (s *sentinel) qcb(tot time.Duration) core.QuiRes {
	s.pend.mu.Lock()
	n := len(s.pend.last)
	s.pend.mu.Unlock()
	if n == 0 {
		return core.QuiDone
	}
	// have "pending" targets
	progressTimeout := max(s.config.Timeout.SendFile.D(), s.qival())
	return s._qcb(tot, s.qival(), progressTimeout, s.r.ErrCnt())
}

func (s *sentinel) _qcb(tot, ival, progressTimeout time.Duration, ecnt int) core.QuiRes {
	i := int64(tot / ival)
	if i <= s.pend.i {
		return core.QuiActive
	}
	s.pend.i = i

	// 1. log
	s.pending()
	if ecnt > 0 {
		nlog.Warningln(s.r.Name(), "quiescing [", tot, "errs:", ecnt, "pending:", s.pend.p, "]")
	} else {
		nlog.Infoln(s.r.Name(), "quiescing [", tot, s.pend.p, "]")
	}
	if len(s.pend.p) == 0 {
		return core.QuiDone
	}

	// 2. check Smap; abort if membership changed
	smap := core.T.Sowner().Get()
	if err := s.checkSmap(smap); err != nil {
		// TODO: a target that started with a newer Smap does not detect the change (compare txn.smapVer)
		s.base.SetClusterAbort()
		return s._qabort(err)
	}

	// 3. poll status; abort if a target did not answer for longer than progressTimeout
	if err := s.poll(progressTimeout); err != nil {
		return s._qabort(err)
	}
	return core.QuiActive
}

// poll the targets that did not send 'done' (T2T status)
func (s *sentinel) poll(progressTimeout time.Duration) error {
	var (
		errs = cos.NewErrs()
		ecnt atomic.Int64
		wg   = cos.NewClusterWaitGroup(sys.NumCPU(), len(s.pend.p))
	)
	for _, tid := range s.pend.p {
		wg.Add(1)
		go func() {
			if err := s.status(tid, progressTimeout, &ecnt); err != nil {
				errs.Add(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	_, err := errs.JoinErr()
	return err
}

func (s *sentinel) status(tid string, progressTimeout time.Duration, ecnt *atomic.Int64) error {
	tsi := s.smap.GetTarget(tid)
	rs, flags, cause, err := s.base.QueryStatus(tsi)
	if err != nil {
		cmn.SparseWarn(cos.ModXs, ecnt.Inc(), s.r.Name(), xact.OpcodeStatus, tsi.StringEx(), err)
		s.pend.mu.Lock()
		last, pending := s.pend.last[tid]
		s.pend.mu.Unlock()
		if !pending {
			return nil
		}
		debug.Assert(last != 0)
		if since := time.Duration(mono.NanoTime() - last); since > progressTimeout {
			return fmt.Errorf("%s: timed out waiting for %s [ %v, %v ]", s.r.Name(), meta.Tname(tid), since, s.pend.p)
		}
		return nil
	}

	s.onStatus(tid, rs, flags, cause)
	return nil
}

// act on a peer's status reply
func (s *sentinel) onStatus(tid string, rs core.RunState, flags xact.StatusFlags, cause string) {
	doneSending := flags.IsSet(xact.StatusDoneSending)
	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(s.r.Name(), "recv 'status' from:", meta.Tname(tid), rs, "done sending:", doneSending)
	}
	switch {
	case rs == core.RunStateAborted:
		nlog.Warningln(s.r.Name(), "peer", meta.Tname(tid), "aborted (status)")
		s.base.RecvAbortByOwner(cause) // (Quiesce returns QuiAborted)
	case rs == core.RunStateFinished || doneSending:
		s.done(tid, "status") // its 'done' is late or lost
	default: // running or idle
		s.pend.mu.Lock()
		if _, pending := s.pend.last[tid]; pending {
			s.pend.last[tid] = mono.NanoTime()
		}
		s.pend.mu.Unlock()
	}
}

// called by the quiesce owner before Finish (see "Ownership" in xact/README.md)
func (s *sentinel) _qabort(err error) core.QuiRes {
	nlog.ErrorDepth(1, err)
	s.base.AbortByOwner(err)
	return core.QuiAborted
}

func (s *sentinel) checkSmap(smap *meta.Smap) error {
	return s.smap.CheckSameTargets(smap, s.r.Name())
}

func (s *sentinel) pending() {
	s.pend.p = s.pend.p[:0]
	s.pend.mu.Lock()
	for tid := range s.pend.last {
		s.pend.p = append(s.pend.p, tid)
	}
	s.pend.mu.Unlock()
}

//
// receive
//

// in-band 'done' (archive)
func (s *sentinel) rxDone(hdr *transport.ObjHdr) {
	debug.Assert(s.isPeer(hdr.SID), s.r.Name(), " unexpected 'done' from ", hdr.SID)
	if rs := s.r.RunState(); rs == core.RunStateFinished || rs == core.RunStateAborted {
		return
	}
	s.done(hdr.SID, "recv 'done'")
}

// T2T 'done' from a peer
// (compare w/ rxDone: no run-state check - 'done' is idempotent, late push is benign)
func (s *sentinel) recvDone(sid string) error {
	if !s.isPeer(sid) {
		return fmt.Errorf("%s: unexpected %s from %q", s.r.Name(), xact.OpcodeDone, sid)
	}
	s.done(sid, "recv 'done'")
	return nil
}

// same selection as init: active target in the job's Smap, other than self
func (s *sentinel) isPeer(tid string) bool {
	return tid != core.T.SID() && s.smap.GetTarget(tid) != nil && !s.smap.InMaintOrDecomm(tid)
}

// OpcodeStatus reply flags (see xact.Peer)
func (s *sentinel) statusFlags() (f xact.StatusFlags) {
	if s.txDone.Load() {
		f |= xact.StatusDoneSending
	}
	return f
}

// mark peer done (idempotent)
func (s *sentinel) done(tid, tag string) {
	s.pend.mu.Lock()
	delete(s.pend.last, tid)
	n := len(s.pend.last)
	s.pend.mu.Unlock()
	if cmn.Rom.V(4, cos.ModXs) {
		nlog.InfoDepth(2, s.r.Name(), tag, "from:", meta.Tname(tid), n)
	}
}
