// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
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

const apairDeleted int64 = -1

type (
	apair struct {
		last atomic.Int64 // last time the peer answered
	}
	sentinel struct {
		r      core.Xact
		base   *xact.Base
		config *cmn.Config
		smap   *meta.Smap
		pend   struct {
			m map[string]*apair // map [tid => apair]
			p []string          // reusable slice [tid]
			i atomic.Int64      // periodic log & progress
			n atomic.Int64      // current num running (<= `nat`)
		}
		nat int
	}
)

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
	s.pend.n.Store(int64(nat - 1))
	s.pend.m = make(map[string]*apair, nat-1)
	for tid := range smap.Tmap {
		if tid == core.T.SID() || smap.InMaintOrDecomm(tid) {
			continue
		}
		s.pend.m[tid] = &apair{}
	}
}

func (s *sentinel) cleanup() {
	clear(s.pend.m)
	s.pend.p = s.pend.p[:0]
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
	for tid := range s.pend.m {
		apair := s.pend.m[tid]
		apair.last.CAS(0, now)
	}
}

// Qival returns the quiesce check interval based on config
func (s *sentinel) qival() time.Duration {
	return cos.ClampDuration(s.config.Timeout.MaxHostBusy.D(), 10*time.Second, time.Minute)
}

func (s *sentinel) qcb(tot time.Duration) core.QuiRes {
	if s.pend.n.Load() == 0 {
		return core.QuiDone
	}
	// have "pending" targets
	progressTimeout := max(s.config.Timeout.SendFile.D(), time.Minute)
	return s._qcb(tot, s.qival(), progressTimeout, s.r.ErrCnt())
}

func (s *sentinel) _qcb(tot, ival, progressTimeout time.Duration, ecnt int) core.QuiRes {
	i := int64(tot / ival)
	if i <= s.pend.i.Load() {
		return core.QuiActive
	}
	s.pend.i.Store(i)

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
	var (
		tsi   = s.smap.GetTarget(tid)
		apair = s.pend.m[tid]
	)
	rs, err := s.base.QueryRunState(tsi)
	if err != nil {
		cmn.SparseWarn(cos.ModXs, ecnt.Inc(), s.r.Name(), xact.OpcodeStatus, tsi.StringEx(), err)
		last := apair.last.Load()
		if last == apairDeleted {
			return nil
		}
		debug.Assert(last != 0)
		if since := time.Duration(mono.NanoTime() - last); since > progressTimeout {
			return fmt.Errorf("%s: timed out waiting for %s [ %v, %v ]", s.r.Name(), meta.Tname(tid), since, s.pend.p)
		}
		return nil
	}

	if cmn.Rom.V(5, cos.ModXs) {
		nlog.Infoln(s.r.Name(), "recv 'status' from:", meta.Tname(tid), rs, "pending:", s.pend.n.Load())
	}
	switch rs {
	case core.RunStateFinished:
		s.done(tid, "finished") // its 'done' is late or lost
	case core.RunStateAborted:
		xact.RecvAbortXact(s.r, tid, "aborted (status)") // (Quiesce returns QuiAborted)
	default: // running or idle
		apair.alive()
	}
	return nil
}

func (s *sentinel) _qabort(err error) core.QuiRes {
	nlog.ErrorDepth(1, err)
	s.r.Abort(err)
	return core.QuiAborted
}

func (s *sentinel) checkSmap(smap *meta.Smap) error {
	return s.smap.CheckSameTargets(smap, s.r.Name())
}

func (s *sentinel) pending() {
	s.pend.p = s.pend.p[:0]
	for tid := range s.pend.m {
		apair := s.pend.m[tid]
		if apair.last.Load() != apairDeleted {
			s.pend.p = append(s.pend.p, tid)
		}
	}
}

//
// receive
//

func (s *sentinel) rxDone(hdr *transport.ObjHdr) {
	if rs := s.r.RunState(); rs == core.RunStateFinished || rs == core.RunStateAborted {
		return
	}
	s.done(hdr.SID, "recv 'done'")
}

// mark peer done (idempotent)
func (s *sentinel) done(tid, tag string) {
	apair := s.pend.m[tid]
	if apair == nil { // unlikely
		debug.Assert(false, "missing apair ", tid)
		return
	}
	if prev := apair.last.Swap(apairDeleted); prev != apairDeleted {
		s.pend.n.Dec()
	}
	if cmn.Rom.V(4, cos.ModXs) {
		nlog.InfoDepth(2, s.r.Name(), tag, "from:", meta.Tname(tid), s.pend.n.Load())
	}
}

func (a *apair) alive() {
	if last := a.last.Load(); last != apairDeleted {
		a.last.CAS(last, mono.NanoTime())
	}
}
