// Package bundle provides multi-streaming transport with the functionality
// to dynamically (un)register receive endpoints, establish long-lived flows, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package bundle

import (
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
)

type SharedDM struct {
	dm     DM
	rxcbs  map[string]transport.RecvObj
	rxmu   sync.Mutex
	openmu sync.Mutex
}

// called upon target startup
func (s *SharedDM) Init(config *cmn.Config, compression string) {
	extra := Extra{Config: config, Compression: compression}
	s.dm.init(s.trname(), s.recv, cmn.OwtNone, extra)
}

// NOTE: constant (until and unless we run multiple shared-DMs)
func (*SharedDM) trname() string { return "shared-dm" }

// called on-demand
func (s *SharedDM) Open() {
	s.openmu.Lock()
	defer s.openmu.Unlock()
	if s.dm.stage.opened.Load() {
		return
	}
	s.rxcbs = make(map[string]transport.RecvObj, 4)
	s.dm.Open()
	nlog.InfoDepth(1, core.T.String(), "open", s.trname())
}

// nothing running + 10m inactivity
func (s *SharedDM) Close() error {
	s.openmu.Lock()
	defer s.openmu.Unlock()
	if !s.dm.stage.opened.Load() {
		return nil
	}
	if len(s.rxcbs) > 0 {
		return fmt.Errorf("cannot close %s: %v", s.trname(), s.rxcbs) // TODO -- FIXME: cleanup
	}
	s.rxcbs = nil
	s.dm.Close(nil)
	nlog.InfoDepth(1, core.T.String(), "close", s.trname())
	return nil
}

func (s *SharedDM) RegRecv(xid string, cb transport.RecvObj) {
	s.rxmu.Lock()
	debug.Assert(s.rxcbs[xid] == nil)
	s.rxcbs[xid] = cb
	s.rxmu.Unlock()
}

func (s *SharedDM) UnregRecv(xid string) {
	s.rxmu.Lock()
	delete(s.rxcbs, xid)
	s.rxmu.Unlock()
}

// NOTE (and limitation): use hdr.Opaque exclusively for xaction IDs
func (s *SharedDM) recv(hdr *transport.ObjHdr, r io.Reader, err error) error {
	if err != nil {
		return err
	}
	xid := string(hdr.Opaque)
	if err := xact.CheckValidUUID(xid); err != nil {
		return fmt.Errorf("%s: %v", s.trname(), err)
	}
	s.rxmu.Lock()
	cb, ok := s.rxcbs[xid]
	s.rxmu.Unlock()
	if !ok {
		return fmt.Errorf("%s: no registered handler for xact %q", s.trname(), xid)
	}
	return cb(hdr, r, nil)
}
