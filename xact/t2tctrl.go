// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
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
)

const NoneWID = "-" // wid is optional, may not be present

var (
	errRecvAbortXact = errors.New("remote target aborted xaction")   // to avoid duplicated broadcast
	errRecvAbortWI   = errors.New("remote target aborted work item") // sender-side cancellation
)

func (xctn *Base) NewErrRecvAbortXact(tid, errCause string) error {
	return fmt.Errorf("%w: %s [%s: %q]", errRecvAbortXact, xctn.Cname(), meta.Tname(tid), errCause)
}

func IsErrRecvAbortXact(err error) bool { return errors.Is(err, errRecvAbortXact) }

func (xctn *Base) NewErrRecvAbortWI(tid, wid, errCause string) error {
	return fmt.Errorf("%w: %s wid=%q [%s: %q]", errRecvAbortWI, xctn.Cname(), wid, meta.Tname(tid), errCause)
}

func IsErrRecvAbortWI(err error) bool { return errors.Is(err, errRecvAbortWI) }

func (xctn *Base) SendCtrl(tsi *meta.Snode, wid, opcode string, body []byte) error {
	path := apc.URLPathXactions.Join(T2TCtrl, xctn.Kind(), xctn.ID(), cos.Left(wid, NoneWID), opcode)
	return core.T.IntraCtrlPost(tsi, path, body)
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
			if err := xctn.SendCtrl(si, wid, opcode, cause); err != nil {
				nlog.Warningln(xctn.Name(), opcode, si.StringEx(), err)
			}
		}(tsi)
	}
	nlog.WarningDepth(1, xctn.Name(), core.T.String(), "bcast abort [", err, "]")
}
