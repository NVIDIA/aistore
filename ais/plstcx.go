// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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
		smap    *smapX
		hdr     http.Header
		// work
		tsi     *meta.Snode
		xid     string // x-tco
		cnt     int
		lsmsg   apc.LsoMsg
		altmsg  apc.ActMsg
		tcomsg  cmn.TCObjsMsg
		stopped atomic.Bool
	}
)

func (a *lstca) add(c *lstcx) {
	a.mu.Lock()
	if a.a == nil {
		a.a = make(map[string]*lstcx, 4)
	}
	a.a[c.xid] = c
	a.mu.Unlock()
}

func (a *lstca) del(c *lstcx) {
	a.mu.Lock()
	delete(a.a, c.xid)
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
	c.smap = c.p.owner.smap.get()
	tsi, err := c.smap.HrwTargetTask(c.lsmsg.UUID)
	if err != nil {
		return "", err
	}
	c.tsi = tsi
	c.lsmsg.SID = tsi.ID()

	// 2. ls 1st page
	var lst *cmn.LsoRes
	lst, err = c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, tsi /*designated target*/, c.config, true)
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
	lr, cnt := &c.tcomsg.ListRange, len(lst.Entries)
	lr.ObjNames = make([]string, 0, cnt)
	for _, e := range lst.Entries {
		lr.ObjNames = append(lr.ObjNames, e.Name)
	}

	// 4. multi-obj action: transform/copy 1st page
	c.altmsg.Value = &c.tcomsg
	c.altmsg.Action = apc.ActCopyObjects
	if c.amsg.Action == apc.ActETLBck {
		c.altmsg.Action = apc.ActETLObjects
	}

	if c.xid, err = c.p.tcobjs(c.bckFrom, c.bckTo, c.config, &c.altmsg, &c.tcomsg); err != nil {
		return "", err
	}

	nlog.Infoln("'ls --all' to execute [" + c.amsg.Action + " -> " + c.altmsg.Action + "]")
	s := fmt.Sprintf("%s[%s] %s => %s", c.altmsg.Action, c.xid, c.bckFrom, c.bckTo)

	// 5. more pages, if any
	if lst.ContinuationToken != "" {
		// Run
		nlog.Infoln("run", s, "...")
		c.lsmsg.ContinuationToken = lst.ContinuationToken
		go c.pages(s, cnt)
	} else {
		nlog.Infoln(s, "count", cnt)
	}
	return c.xid, nil
}

func (c *lstcx) pages(s string, cnt int) {
	c.cnt = cnt
	c.p.lstca.add(c)

	// pages 2, 3, ...
	var err error
	for !c.stopped.Load() && c.lsmsg.ContinuationToken != "" {
		if cnt, err = c._page(); err != nil {
			break
		}
		c.cnt += cnt
	}
	c.p.lstca.del(c)
	nlog.Infoln(s, "count", c.cnt, "stopped", c.stopped.Load(), "c-token", c.lsmsg.ContinuationToken, "err", err)
}

// next page
func (c *lstcx) _page() (int, error) {
	lst, err := c.p.lsObjsR(c.bckFrom, &c.lsmsg, c.hdr, c.smap, c.tsi, c.config, true)
	if err != nil {
		return 0, err
	}
	c.lsmsg.ContinuationToken = lst.ContinuationToken
	if len(lst.Entries) == 0 {
		debug.Assert(lst.ContinuationToken == "")
		return 0, nil
	}

	lr := &c.tcomsg.ListRange
	clear(lr.ObjNames)
	lr.ObjNames = lr.ObjNames[:0]
	for _, e := range lst.Entries {
		lr.ObjNames = append(lr.ObjNames, e.Name)
	}
	c.altmsg.Value = &c.tcomsg
	err = c.bcast()
	return len(lr.ObjNames), err
}

// calls t.httpxpost (TODO: slice of names is the only "delta" - optimize)
func (c *lstcx) bcast() (err error) {
	body := cos.MustMarshal(apc.ActMsg{Name: c.xid, Value: &c.tcomsg})
	args := allocBcArgs()
	{
		args.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathXactions.S, Body: body}
		args.to = core.Targets
		args.timeout = cmn.Rom.MaxKeepalive()
	}
	if c.stopped.Load() {
		return
	}
	results := c.p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if err = res.err; err != nil {
			break
		}
	}
	freeBcastRes(results)
	return err
}
