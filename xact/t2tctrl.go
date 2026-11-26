// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/sys"
)

// POST /v1/xactions/{one of the constants below}
const (
	T2TCtrl = "t2tctrl" // see also: proxyToContTCO
)

// POST /v1/xactions/"t2tctrl"/{xkind}/{xid}/{wid}/{opcode}

const NumT2TCtrlItems = 5

const (
	OpcodeStartedWI = "startedWI"
	OpcodeAbortXact = "abortXact"
	OpcodeAbortWI   = "abortWI"
	OpcodeStatus    = "status"
	OpcodeDone      = "done"
)

const NoneWID = "-" // wid is optional, may not be present

// abort from a peer or the IC with the original cause as its text (do not re-broadcast)
type errRecvAbortXact struct {
	cause string
}

var errRecvAbortWI = errors.New("remote target aborted work item") // sender-side cancellation

func (e *errRecvAbortXact) Error() string { return cos.Left(e.cause, "received abort") }

func RecvAbortXact(xctn core.Xact, errCause string) { xctn.Abort(&errRecvAbortXact{errCause}) }

func IsErrRecvAbortXact(err error) bool {
	_, ok := errors.AsType[*errRecvAbortXact](err)
	return ok
}

func ShouldLogAbort(err error, mod int) bool { return !IsErrRecvAbortXact(err) || cmn.Rom.V(4, mod) }

type (
	// optional: kind-specific T2T control
	// (OpcodeStatus and OpcodeAbortXact are handled for all xactions)
	Peer interface {
		RecvCtrl(wid, sid, opcode string, body []byte) error
		StatusFlags() StatusFlags // optional bits following the run-state byte (OpcodeStatus)
	}

	StatusFlags uint8
)

const (
	StatusDoneSending StatusFlags = 1 << iota // all outbound data sent and received by peers
)

func (f StatusFlags) IsSet(flag StatusFlags) bool { return f&flag == flag }

// reply with the abort cause when aborted, otherwise optional status flags
func StatusReply(xctn core.Xact) []byte {
	rs := xctn.RunState()
	b := rs.Bytes()
	if rs == core.RunStateAborted {
		if err := xctn.AbortErr(); err != nil {
			b = append(b, err.Error()...)
		}
	} else if p, ok := xctn.(Peer); ok {
		if f := p.StatusFlags(); f != 0 {
			b = append(b, byte(f))
		}
	}
	return b
}

func ParseStatusReply(b []byte) (rs core.RunState, flags StatusFlags, cause string, err error) {
	if rs, err = core.ParseRunState(b); err != nil {
		return 0, 0, "", err
	}
	if rs == core.RunStateAborted {
		return rs, 0, string(b[1:]), nil
	}
	if len(b) > 1 {
		flags = StatusFlags(b[1])
	}
	return rs, flags, "", nil
}

func (xctn *Base) QueryStatus(tsi *meta.Snode) (rs core.RunState, flags StatusFlags, cause string, err error) {
	b, err := xctn.SendCtrl(tsi, "", OpcodeStatus, nil)
	if err != nil {
		return 0, 0, "", err
	}
	if rs, flags, cause, err = ParseStatusReply(b); err != nil {
		err = fmt.Errorf("%s: %s: %w", xctn.Name(), tsi.StringEx(), err)
	}
	return rs, flags, cause, err
}

// return true if other targets get the same abort without T2T (peer or IC T2T abort, user xact-stop, or cluster-wide event)
// TODO: a shared cause found locally (e.g., missing source bucket) is not marked
func (xctn *Base) SkipAbortBcast(err error) bool {
	return IsErrRecvAbortXact(err) || xctn.IsClusterAbort() || errors.Is(err, cmn.ErrXactUserAbort)
}

func (xctn *Base) BcastAbort(smap *meta.Smap) {
	err := xctn.AbortErr()
	if err == nil || xctn.SkipAbortBcast(err) {
		return
	}
	xctn.BcastCtrl(smap, "", OpcodeAbortXact, err)
}

func (xctn *Base) NewErrRecvAbortWI(tid, wid, errCause string) error {
	return fmt.Errorf("%w: %s wid=%q [%s: %q]", errRecvAbortWI, xctn.Cname(), wid, meta.Tname(tid), errCause)
}

func IsErrRecvAbortWI(err error) bool { return errors.Is(err, errRecvAbortWI) }

func T2TCtrlPath(kind, xid, wid, opcode string) string {
	return apc.URLPathXactions.Join(T2TCtrl, kind, xid, cos.Left(wid, NoneWID), opcode)
}

// return response body, if any
func (xctn *Base) SendCtrl(tsi *meta.Snode, wid, opcode string, body []byte) ([]byte, error) {
	return core.T.IntraCtrlPost(tsi, T2TCtrlPath(xctn.Kind(), xctn.ID(), wid, opcode), body)
}

// asynchronous broadcast with bounded launch parallelism
// (note: NOT waiting for completion)
func (xctn *Base) BcastCtrl(smap *meta.Smap, wid, opcode string, err error) {
	if err != nil && IsErrRecvAbortXact(err) {
		return
	}

	var (
		tmap  = smap.Tmap
		wg    = cos.NewClusterWaitGroup(sys.NumCPU(), len(tmap))
		cause []byte
	)
	wid = cos.Left(wid, NoneWID)
	if err != nil {
		cause = []byte(err.Error())
	}

	for _, tsi := range tmap {
		if tsi.ID() == core.T.SID() {
			continue // skip self
		}
		if tsi.InMaintOrDecomm() {
			continue
		}
		wg.Add(1)
		go func(si *meta.Snode) {
			defer wg.Done()
			if _, err := xctn.SendCtrl(si, wid, opcode, cause); err != nil {
				nlog.Warningln(xctn.Name(), opcode, si.StringEx(), err)
			}
		}(tsi)
	}
	if cmn.Rom.V(4, cos.ModXs) {
		nlog.InfoDepth(1, xctn.Name(), core.T.String(), "bcast", opcode, err)
	}
}
