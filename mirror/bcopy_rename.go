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

// XactBckCopyRename copies or renames a bucket

type (
	XactBckCopyRename struct {
		xactBckBase
		slab     *memsys.Slab2
		bucketTo string
		rename   bool
	}
	bcrJogger struct { // one per mountpath
		joggerBckBase
		parent *XactBckCopyRename
		buf    []byte
	}
)

//
// public methods
//

func NewXactBCR(id int64, bucketFrom, bucketTo, action string, t cluster.Target, slab *memsys.Slab2,
	local bool) *XactBckCopyRename {
	return &XactBckCopyRename{
		xactBckBase: *newXactBckBase(id, action, bucketFrom, t, local),
		slab:        slab,
		bucketTo:    bucketTo,
		rename:      action == cmn.ActRenameLB,
	}
}

func (r *XactBckCopyRename) Run() (err error) {
	var numjs int
	if numjs, err = r.init(); err != nil {
		return
	}
	glog.Infoln(r.String(), r.Bucket(), "=>", r.bucketTo)
	return r.xactBckBase.run(numjs)
}

//
// private methods
//

func (r *XactBckCopyRename) init() (numjs int, err error) {
	availablePaths, _ := fs.Mountpaths.Get()
	r.xactBckBase.init(availablePaths)
	numjs = len(availablePaths)
	config := cmn.GCO.Get()
	for _, mpathInfo := range availablePaths {
		bcrJogger := newBcrJogger(r, mpathInfo, config)
		// only objects; TODO contentType := range fs.CSM.RegisteredContentTypes
		mpathLC := mpathInfo.MakePath(fs.ObjectType, r.BckIsLocal())
		r.mpathers[mpathLC] = bcrJogger
		go bcrJogger.jog()
	}
	return
}

func (r *XactBckCopyRename) Description() string {
	if r.Kind() == cmn.ActRenameLB {
		return "rename local bucket"
	}
	cmn.Assert(r.Kind() == cmn.ActCopyLB)
	return "copy local bucket"
}

//
// mpath bcrJogger - as mpather
//

func newBcrJogger(parent *XactBckCopyRename, mpathInfo *fs.MountpathInfo, config *cmn.Config) *bcrJogger {
	jbase := joggerBckBase{parent: &parent.xactBckBase, mpathInfo: mpathInfo, config: config, skipLoad: true}
	j := &bcrJogger{joggerBckBase: jbase, parent: parent}
	j.joggerBckBase.callback = j.copyObject
	return j
}

//
// mpath bcrJogger - main
//
func (j *bcrJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.Bucket())
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *bcrJogger) copyObject(lom *cluster.LOM) error {
	return j.parent.Target().CopyObject(lom, j.parent.bucketTo, j.buf, j.parent.rename)
}
