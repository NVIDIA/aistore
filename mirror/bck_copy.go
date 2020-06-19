// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// XactBckCopy copies a bucket locally within the same cluster

type (
	XactBckCopy struct {
		xactBckBase
		slab    *memsys.Slab
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
	}
	bccJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckCopy
		buf    []byte
	}
)

//
// public methods
//

func NewXactBCC(id string, bckFrom, bckTo *cluster.Bck, t cluster.Target, slab *memsys.Slab) *XactBckCopy {
	return &XactBckCopy{
		xactBckBase: *newXactBckBase(id, cmn.ActCopyBucket, bckTo.Bck, t),
		slab:        slab,
		bckFrom:     bckFrom,
		bckTo:       bckTo,
	}
}

func (r *XactBckCopy) Run() (err error) {
	mpathCount := r.init()
	glog.Infoln(r.String(), r.bckFrom.Bck, "=>", r.bckTo.Bck)
	err = r.xactBckBase.run(mpathCount)
	// notifications
	if n := r.Notif(); n != nil {
		if n.Upon(cmn.UponTerm) {
			n.Callback(n, err)
		}
	}
	return
}

func (r *XactBckCopy) String() string { return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom) }

//
// private methods
//

func (r *XactBckCopy) init() (mpathCount int) {
	var (
		availablePaths, _ = fs.Mountpaths.Get()
		config            = cmn.GCO.Get()
	)
	mpathCount = len(availablePaths)

	r.xactBckBase.init(mpathCount)
	for _, mpathInfo := range availablePaths {
		bccJogger := newBCCJogger(r, mpathInfo, config)
		// only objects; TODO contentType := range fs.CSM.RegisteredContentTypes
		mpathLC := mpathInfo.MakePathCT(r.bckFrom.Bck, fs.ObjectType)
		r.mpathers[mpathLC] = bccJogger
		go bccJogger.jog()
	}
	return
}

//
// mpath bccJogger - main
//

func newBCCJogger(parent *XactBckCopy, mpathInfo *fs.MountpathInfo, config *cmn.Config) *bccJogger {
	j := &bccJogger{
		joggerBckBase: joggerBckBase{
			parent:    &parent.xactBckBase,
			bck:       parent.bckFrom.Bck,
			mpathInfo: mpathInfo,
			config:    config,
			skipLoad:  true,
		},
		parent: parent,
	}
	j.joggerBckBase.callback = j.copyObject
	return j
}

func (j *bccJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.bckFrom.Bck)
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *bccJogger) copyObject(lom *cluster.LOM) error {
	_, err := j.parent.Target().CopyObject(lom, j.parent.bckTo, j.buf, false)
	return err
}
