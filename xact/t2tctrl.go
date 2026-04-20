// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
)

const NoneWID = "-" // wid is optional, may not be present

// TODO -- FIXME: copy/paste from xact/xs/xutils.go
var (
	errRecvAbort = errors.New("remote target abort") // to avoid duplicated broadcast
)

func (xctn *Base) NewErrRecvAbort(tid, errCause string) error {
	return fmt.Errorf("%s: %w [%s: %s]", xctn.Name(), errRecvAbort, meta.Tname(tid), errCause)
}

func isErrRecvAbort(err error) bool {
	return errors.Is(err, errRecvAbort)
}

func (xctn *Base) SendCtrl(tsi *meta.Snode, wid, opcode string, body []byte) error {
	path := apc.URLPathXactions.Join(T2TCtrl, xctn.Kind(), xctn.ID(), cos.Left(wid, NoneWID), opcode)
	reqArgs := cmn.HreqArgs{
		Method: http.MethodPost,
		Base:   tsi.URL(cmn.NetIntraControl),
		Path:   path,
		Body:   body,
	}
	req, _, cancel, errR := reqArgs.ReqWith(cmn.Rom.MaxKeepalive())
	if errR != nil {
		return errR
	}
	defer cancel()
	req.Header.Set(apc.HdrSenderID, core.T.SID())
	req.Header.Set(apc.HdrSenderName, core.T.Snode().Name())
	// note: not setting apc.HdrSenderSmapVer
	req.Header.Set(cos.HdrUserAgent, apc.HdrUA)

	resp, err := core.T.ControlClient().Do(req)
	if err == nil {
		if code := resp.StatusCode; code >= http.StatusBadRequest {
			err = &cmn.ErrHTTP{Message: http.StatusText(code), Status: code}
		}
	}
	if resp != nil && resp.Body != nil {
		cos.DrainReader(resp.Body)
		resp.Body.Close()
	}
	return err
}

// asynchronous broadcast with bounded launch parallelism
// (NOT waiting for completion)
func (xctn *Base) BcastAbort(err error) {
	debug.Assert(err != nil)
	if err == nil || isErrRecvAbort(err) {
		return
	}
	var (
		smap  = core.T.Sowner().Get()
		tmap  = smap.Tmap
		wg    = cos.NewLimitedWaitGroup(sys.MaxParallelism(), len(tmap))
		cause = []byte(err.Error())
	)
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
			if err := xctn.SendCtrl(si, NoneWID, OpcodeAbortXact, cause); err != nil {
				nlog.Warningln(xctn.Name(), OpcodeAbortXact, si.StringEx(), err)
			}
		}(tsi)
	}
	nlog.WarningDepth(1, xctn.Name(), core.T.String(), "bcast abort [", err, "]")
}
