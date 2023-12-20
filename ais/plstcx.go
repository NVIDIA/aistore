// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xs"
)

type (
	lstca struct {
		a  map[string]*lstcx
		mu sync.Mutex
	}
	lstcx struct {
		p *proxy
		// arg
		bckFrom *meta.Bck
		bckTo   *meta.Bck
		amsg    *apc.ActMsg // orig
		config  *cmn.Config
		// work
		tsi     *meta.Snode
		lsmsg   apc.LsoMsg
		altmsg  apc.ActMsg
		tcomsg  cmn.TCObjsMsg
		stopped atomic.Bool
	}
)

func (a *lstca) add(c *lstcx) {
	debug.Assert(c.tcomsg.TxnUUID != "")
	a.mu.Lock()
	if a.a == nil {
		a.a = make(map[string]*lstcx, 4)
	}
	a.a[c.tcomsg.TxnUUID] = c
	a.mu.Unlock()
}

func (a *lstca) del(c *lstcx) {
	debug.Assert(c.tcomsg.TxnUUID != "")
	a.mu.Lock()
	delete(a.a, c.tcomsg.TxnUUID)
	a.mu.Unlock()
}

func (a *lstca) abort(xargs *xact.ArgsMsg) {
	switch {
	case xargs.ID != "":
		if !strings.HasPrefix(xargs.ID, xs.PrefixTcoID) {
			return
		}
		a.mu.Lock()
		if c, ok := a.a[xargs.ID]; ok {
			c.stopped.Store(true)
		}
		a.mu.Unlock()
		nlog.Infoln(xargs.ID, "aborted")
	case xargs.Kind == apc.ActCopyObjects || xargs.Kind == apc.ActETLObjects:
		var ids []string
		a.mu.Lock()
		for uuid, c := range a.a {
			c.stopped.Store(true)
			ids = append(ids, uuid)
		}
		clear(a.a)
		a.mu.Unlock()
		if len(ids) > 0 {
			nlog.Infoln(ids, "aborted")
		}
	}
}

func (c *lstcx) do() (string, error) {
	// 1. lsmsg
	c.lsmsg = apc.LsoMsg{
		UUID:     cos.GenUUID(),
		Prefix:   c.tcomsg.TCBMsg.Prefix,
		Props:    apc.GetPropsName,
		PageSize: 0, // i.e., backend.MaxPageSize()
	}
	c.lsmsg.SetFlag(apc.LsNameOnly)
	smap := c.p.owner.smap.get()
	tsi, err := smap.HrwTargetTask(c.lsmsg.UUID)
	if err != nil {
		return "", err
	}
	c.tsi = tsi
	c.lsmsg.SID = tsi.ID()

	// 2. ls 1st page
	var lst *cmn.LsoResult
	lst, err = c.p.lsObjsR(c.bckFrom, &c.lsmsg, smap, tsi /*designated target*/, c.config, true)
	if err != nil {
		return "", err
	}
	if len(lst.Entries) == 0 {
		// TODO: return http status to indicate exactly that (#6393)
		nlog.Infoln(c.amsg.Action, c.bckFrom.Cname(""), " to ", c.bckTo.Cname("")+": lso counts zero - nothing to do")
		return c.lsmsg.UUID, nil
	}

	// 3. tcomsg
	c.tcomsg.ToBck = c.bckTo.Clone()
	names := make([]string, 0, len(lst.Entries))
	for _, e := range lst.Entries {
		names = append(names, e.Name)
	}
	c.tcomsg.ListRange.ObjNames = names

	// 4. multi-obj action: transform/copy
	c.altmsg.Value = &c.tcomsg
	c.altmsg.Action = apc.ActCopyObjects
	if c.amsg.Action == apc.ActETLBck {
		c.altmsg.Action = apc.ActETLObjects
	}
	cnt := min(len(names), 10)
	nlog.Infof("(%s => %s): %s => %s %v...", c.amsg.Action, c.altmsg.Action, c.bckFrom, c.bckTo, names[:cnt])

	// Run
	c.tcomsg.TxnUUID, err = c.p.tcobjs(c.bckFrom, c.bckTo, c.config, &c.altmsg, &c.tcomsg)
	if lst.ContinuationToken != "" {
		c.lsmsg.ContinuationToken = lst.ContinuationToken
		go func() {
			c.p.lstca.add(c)
			c.pages(smap)
			c.p.lstca.del(c)
		}()
	}

	return c.tcomsg.TxnUUID, err
}

// pages 2..last
func (c *lstcx) pages(smap *smapX) {
	for !c.stopped.Load() {
		// next page
		lst, err := c.p.lsObjsR(c.bckFrom, &c.lsmsg, smap, c.tsi, c.config, true)
		if err != nil {
			nlog.Errorln(err)
			return
		}
		if len(lst.Entries) == 0 {
			return
		}

		// next tcomsg
		names := make([]string, 0, len(lst.Entries))
		for _, e := range lst.Entries {
			names = append(names, e.Name)
		}
		c.tcomsg.ListRange.ObjNames = names

		if c.stopped.Load() {
			return
		}
		// next tco action
		c.altmsg.Value = &c.tcomsg
		xid, err := c.p.tcobjs(c.bckFrom, c.bckTo, c.config, &c.altmsg, &c.tcomsg)
		if err != nil {
			nlog.Errorln(err)
			return
		}
		debug.Assertf(c.tcomsg.TxnUUID == xid, "%q vs %q", c.tcomsg.TxnUUID, xid)

		// last page?
		if lst.ContinuationToken == "" {
			return
		}
		c.lsmsg.ContinuationToken = lst.ContinuationToken
	}
}
