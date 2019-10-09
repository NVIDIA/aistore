// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
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
		slab  *memsys.Slab2
		bckTo *cluster.Bck
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

func NewXactBCC(id int64, bckFrom, bckTo *cluster.Bck, action string, t cluster.Target, slab *memsys.Slab2) *XactBckCopy {
	return &XactBckCopy{
		xactBckBase: *newXactBckBase(id, action, bckFrom, t),
		slab:        slab,
		bckTo:       bckTo,
	}
}

func (r *XactBckCopy) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return
	}
	glog.Infoln(r.String(), r.Bucket(), "=>", r.bckTo)
	return r.xactBckBase.run(numjs)
}

//
// private methods
//

func (r *XactBckCopy) init() (numjs int, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	r.xactBckBase.init(availablePaths)
	numjs = len(availablePaths)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		bccJogger := newBccJogger(r, mpathInfo, config)
		// only objects; TODO contentType := range fs.CSM.RegisteredContentTypes
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.Provider())
		r.mpathers[mpathLC] = bccJogger
		go bccJogger.jog()
	}
	return
}

func (r *XactBckCopy) Description() string {
	cmn.Assert(r.Kind() == cmn.ActCopyBucket)
	return "copy ais bucket"
}

//
// mpath bccJogger - as mpather
//

func newBccJogger(parent *XactBckCopy, mpathInfo *fs.MountpathInfo, config *cmn.Config) *bccJogger {
	jbase := joggerBckBase{parent: &parent.xactBckBase, mpathInfo: mpathInfo, config: config, skipLoad: true}
	j := &bccJogger{joggerBckBase: jbase, parent: parent}
	j.joggerBckBase.callback = j.copyObject
	return j
}

//
// mpath bccJogger - main
//
func (j *bccJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *bccJogger) copyObject(lom *cluster.LOM) error {
	return j.parent.Target().CopyObject(lom, j.parent.bckTo, j.buf)
}
