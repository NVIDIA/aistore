// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
)

type lstcx struct {
	p *proxy
	// arg
	bckFrom *cluster.Bck
	bckTo   *cluster.Bck
	amsg    *apc.ActMsg // orig
	tcbmsg  *apc.TCBMsg
	// work
	lsmsg  apc.LsoMsg
	altmsg apc.ActMsg
	tcomsg cmn.TCObjsMsg
}

func (c *lstcx) do() (string, error) {
	var (
		p    = c.p
		smap = c.p.owner.smap.get()
	)
	// 1. lsmsg
	c.lsmsg = apc.LsoMsg{
		UUID:     cos.GenUUID(),
		Prefix:   c.tcbmsg.Prefix,
		Props:    apc.GetPropsName,
		PageSize: apc.DefaultPageSizeCloud, // TODO: make use of backend.MaxPageSize()
	}
	c.lsmsg.SetFlag(apc.LsNameOnly)
	tsi, err := cluster.HrwTargetTask(c.lsmsg.UUID, &smap.Smap)
	if err != nil {
		return "", err
	}
	c.lsmsg.SID = tsi.ID()

	// 2. ls notif
	srcs := cluster.NodeMap{tsi.ID(): tsi}
	nl := xact.NewXactNL(c.lsmsg.UUID, apc.ActList, &smap.Smap, srcs, c.bckFrom.Bucket())
	nl.SetHrwOwner(&smap.Smap)
	p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: c.amsg}) // TODO -- FIXME: forwardCP

	// 3. ls 1st page
	var lst *cmn.LsoResult
	lst, err = p.lsObjsR(c.bckFrom, &c.lsmsg, smap, true /*wantOnlyRemote*/)
	if err != nil {
		return "", err
	}
	if len(lst.Entries) == 0 {
		glog.Infof("%s: %s => %s: lso counts zero - nothing to do", c.amsg.Action, c.bckFrom, c.bckTo)
		return c.lsmsg.UUID, nil
	}

	// 4. tcomsg
	c.tcomsg.ToBck = c.bckTo.Clone()
	c.tcomsg.TCBMsg = *c.tcbmsg
	names := make([]string, 0, len(lst.Entries))
	for _, e := range lst.Entries {
		names = append(names, e.Name)
	}
	c.tcomsg.ListRange.ObjNames = names

	// 5. multi-obj action: transform/copy
	c.altmsg.Value = &c.tcomsg
	c.altmsg.Action = apc.ActCopyObjects
	if c.amsg.Action == apc.ActETLBck {
		c.altmsg.Action = apc.ActETLObjects
	}
	cnt := cos.Min(len(names), 10)
	glog.Infof("(%s => %s): %s => %s %v...", c.amsg.Action, c.altmsg.Action, c.bckFrom, c.bckTo, names[:cnt])

	c.tcomsg.TxnUUID, err = p.tcobjs(c.bckFrom, c.bckTo, &c.altmsg)
	if lst.ContinuationToken != "" {
		c.lsmsg.ContinuationToken = lst.ContinuationToken
		go c.pages(smap)
	}
	return c.tcomsg.TxnUUID, err
}

// pages 2..last
func (c *lstcx) pages(smap *smapX) {
	p := c.p
	for {
		// next page
		lst, err := p.lsObjsR(c.bckFrom, &c.lsmsg, smap, true /*wantOnlyRemote*/)
		if err != nil {
			glog.Error(err)
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

		// next tco action
		c.altmsg.Value = &c.tcomsg
		xid, err := p.tcobjs(c.bckFrom, c.bckTo, &c.altmsg)
		if err != nil {
			glog.Error(err)
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
