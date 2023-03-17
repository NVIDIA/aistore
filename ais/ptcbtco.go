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
	"github.com/NVIDIA/aistore/xact"
)

func (p *proxy) tcbtco(bckFrom, bckTo *cluster.Bck, msg *apc.ActMsg, tcbmsg *apc.TCBMsg,
	_ /*TODO fltPresence*/ int) (string, error) {
	var (
		smap  = p.owner.smap.get()
		lsmsg = &apc.LsoMsg{
			Prefix:   tcbmsg.Prefix,
			Props:    apc.GetPropsName,
			PageSize: apc.DefaultPageSizeCloud, // TODO: backend.MaxPageSize()
		}
	)
	// 1. prep to ls
	lsmsg.SetFlag(apc.LsNameOnly)
	tsi, err := cluster.HrwTargetTask(lsmsg.UUID, &smap.Smap)
	if err != nil {
		return "", err
	}
	lsmsg.SID = tsi.ID()

	// 2. ls notif
	lsmsg.UUID = cos.GenUUID()
	nl := xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, cluster.NodeMap{tsi.ID(): tsi}, bckFrom.Bucket())
	nl.SetHrwOwner(&smap.Smap)
	p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: msg}) // TODO -- FIXME: forwardCP

	// 3. ls 1st page
	var lst *cmn.LsoResult
	lst, err = p.lsObjsR(bckFrom, lsmsg, smap, true /*wantOnlyRemote*/)
	if err != nil {
		return "", err
	}

	// 4. tco msg
	var (
		altmsg *apc.ActMsg
		tcomsg = &cmn.TCObjsMsg{ToBck: bckTo.Clone(), TCBMsg: *tcbmsg}
		names  = make([]string, 0, len(lst.Entries))
	)
	for _, e := range lst.Entries {
		names = append(names, e.Name)
	}
	tcomsg.SelectObjsMsg.ObjNames = names

	// 5. multi-obj action
	altmsg = &apc.ActMsg{Action: apc.ActCopyObjects, Value: tcomsg}
	if msg.Action == apc.ActETLBck {
		altmsg.Action = apc.ActETLObjects
	}
	glog.Infof("%s => %s: bucket %s => %s", msg.Action, altmsg.Action, bckFrom, bckTo)

	// TODO -- FIXME: cycle thru pages

	return p.tcobjs(bckFrom, bckTo, altmsg)
}
