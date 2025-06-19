// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
)

// [TODO]
// - Close() vs usage (when len(receivers) > 0); provide xctn.onFinished() => UnregRecv
// - limitation: hdr.Opaque is exclusively reserved xaction ID

type sharedDM struct {
	dm        DM
	receivers map[string]transport.Receiver
	ocmu      sync.Mutex
	rxmu      sync.Mutex
}

// global
var SDM sharedDM

// called upon target startup
func InitSDM(config *cmn.Config, compression string) {
	extra := Extra{Config: config, Compression: compression}
	SDM.dm.init(SDM.trname(), SDM.recv, cmn.OwtNone, extra)
}

func (sdm *sharedDM) isOpen() bool { return sdm.dm.stage.opened.Load() }

func (sdm *sharedDM) IsActive() (active bool) {
	sdm.rxmu.Lock()
	active = len(sdm.receivers) > 0
	sdm.rxmu.Unlock()
	return
}

// constant (until and unless we run multiple shared-DMs)
func (*sharedDM) trname() string { return "shared-dm" }

func (sdm *sharedDM) _already() {
	nlog.WarningDepth(2, core.T.String(), sdm.trname(), "is already open")
}

// called on-demand
func (sdm *sharedDM) Open() error {
	if sdm.isOpen() {
		sdm._already()
		return nil
	}

	sdm.ocmu.Lock()
	if sdm.isOpen() {
		sdm.ocmu.Unlock()
		sdm._already()
		return nil
	}

	sdm.rxmu.Lock()
	sdm.receivers = make(map[string]transport.Receiver, 4)
	sdm.rxmu.Unlock()

	if err := sdm.dm.RegRecv(); err != nil {
		sdm.ocmu.Unlock()
		nlog.ErrorDepth(1, core.T.String(), err)
		debug.AssertNoErr(err)
		return err
	}
	sdm.dm.Open()
	sdm.ocmu.Unlock()

	nlog.InfoDepth(1, core.T.String(), "open", sdm.trname())
	return nil
}

// nothing running + 10m inactivity
func (sdm *sharedDM) Close() error {
	if !sdm.isOpen() {
		return nil
	}
	sdm.ocmu.Lock()
	if !sdm.isOpen() {
		sdm.ocmu.Unlock()
		return nil
	}

	var (
		xid string
		l   int
	)
	sdm.rxmu.Lock()
	for xid = range sdm.receivers {
		break
	}
	l = len(sdm.receivers)

	if l > 0 {
		sdm.rxmu.Unlock()
		sdm.ocmu.Unlock()
		debug.Assert(cos.IsValidUUID(xid), xid)
		return fmt.Errorf("cannot close %s: [%s, %d]", sdm.trname(), xid, l)
	}

	sdm.receivers = nil
	sdm.rxmu.Unlock()

	sdm.dm.Close(nil)
	sdm.dm.UnregRecv()
	sdm.ocmu.Unlock()

	nlog.InfoDepth(1, core.T.String(), "close", sdm.trname())
	return nil
}

func (sdm *sharedDM) RegRecv(rx transport.Receiver) {
	sdm.ocmu.Lock()
	sdm.rxmu.Lock()
	if !sdm.isOpen() {
		sdm.rxmu.Unlock()
		sdm.ocmu.Unlock()
		debug.Assert(false, sdm.trname(), " ", "closed")
		return
	}
	debug.Assert(sdm.receivers[rx.ID()] == nil, "duplicate registration for xid ", rx.ID())
	sdm.receivers[rx.ID()] = rx
	sdm.rxmu.Unlock()
	sdm.ocmu.Unlock()
}

func (sdm *sharedDM) UnregRecv(xid string) {
	sdm.ocmu.Lock()
	sdm.rxmu.Lock()
	if !sdm.isOpen() {
		sdm.rxmu.Unlock()
		sdm.ocmu.Unlock()
		debug.Assert(false, sdm.trname(), " ", "closed")
		return
	}
	delete(sdm.receivers, xid)
	sdm.rxmu.Unlock()
	sdm.ocmu.Unlock()
}

func (sdm *sharedDM) Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *meta.Snode) error {
	return sdm.dm.Send(obj, roc, tsi)
}

func (sdm *sharedDM) recv(hdr *transport.ObjHdr, r io.Reader, err error) error {
	if err != nil {
		return err
	}

	// TODO -- FIXME: remove xid demux =======================
	var (
		opaque = hdr.Opaque
		lo     = len(opaque)
	)
	if lo < cos.SizeofI16+2 {
		return fmt.Errorf("%s: opaque data too short: %d bytes", sdm.trname(), lo)
	}
	lx := int(binary.BigEndian.Uint16(opaque))
	if cos.SizeofI16+lx > lo {
		return fmt.Errorf("%s: invalid xaction ID length: %d", sdm.trname(), lx)
	}

	xid := string(opaque[cos.SizeofI16 : cos.SizeofI16+lx])
	if err := xact.CheckValidUUID(xid); err != nil {
		return fmt.Errorf("%s: %v", sdm.trname(), err)
	}
	// TODO -- FIXME: e.o. remove xid demux ==================

	sdm.rxmu.Lock()
	if !sdm.isOpen() {
		sdm.rxmu.Unlock()
		return fmt.Errorf("%s is closed, dropping recv [xid: %s, oname: %s]", sdm.trname(), xid, hdr.ObjName)
	}
	rx, ok := sdm.receivers[xid]
	sdm.rxmu.Unlock()

	if !ok {
		return fmt.Errorf("%s: xid %s not found, dropping recv [oname: %s]", sdm.trname(), xid, hdr.ObjName)
	}
	if rx.ID() != xid {
		err = fmt.Errorf("%s: xid mismatch [%q vs %q]", sdm.trname(), xid, rx.ID())
		debug.AssertNoErr(err)
		return err
	}
	return rx.RecvObj(hdr, r, nil)
}
