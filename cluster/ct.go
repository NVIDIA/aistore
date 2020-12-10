// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"io"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

//////////////////////////////
// ais on-disk content type //
//////////////////////////////

type CT struct {
	fqn         string
	bck         *Bck
	objName     string
	contentType string
	mpathInfo   *fs.MountpathInfo
}

func (ct *CT) ContentType() string      { return ct.contentType }
func (ct *CT) ObjName() string          { return ct.objName }
func (ct *CT) Bprops() *cmn.BucketProps { return ct.bck.Props }
func (ct *CT) Bck() *Bck                { return ct.bck }
func (ct *CT) FQN() string              { return ct.fqn }

// TODO: Remove redundancy.
func (ct *CT) Bucket() cmn.Bck              { return ct.Bck().Bck }
func (ct *CT) ObjectName() string           { return ct.ObjName() }
func (ct *CT) MpathInfo() *fs.MountpathInfo { return ct.mpathInfo }

// e.g.: generate workfile FQN from object FQN:
//  ct, err := NewCTFromFQN(fqn, nil)
//  if err != nil { ... }
//  fqn := ct.Make(fs.WorkfileType)
//
// e.g.: generate EC metafile FQN from bucket name, bucket provider and object name:
//  ct, err := NewCTFromBO(bckName, bckProvider, objName, nil)
//  if err != nil { ... }
//  fqn := ct.Make(MetaType)

func NewCTFromFQN(fqn string, b Bowner) (ct *CT, err error) {
	parsedFQN, _, err := ResolveFQN(fqn)
	if err != nil {
		return nil, err
	}
	ct = &CT{
		fqn:         fqn,
		bck:         &Bck{Bck: parsedFQN.Bck},
		objName:     parsedFQN.ObjName,
		contentType: parsedFQN.ContentType,
		mpathInfo:   parsedFQN.MpathInfo,
	}
	if b != nil {
		err = ct.bck.Init(b, nil)
	}
	return
}

func NewCTFromBO(bckName, bckProvider, objName string, b Bowner, ctType ...string) (ct *CT, err error) {
	ct = &CT{
		bck:     NewBck(bckName, bckProvider, cmn.NsGlobal),
		objName: objName,
	}
	if b != nil {
		if err = ct.bck.Init(b, nil); err != nil {
			return
		}
	}
	ct.mpathInfo, _, err = HrwMpath(ct.bck.MakeUname(objName))
	if err != nil {
		return
	}
	if len(ctType) == 0 {
		ct.contentType = fs.ObjectType
	} else {
		ct.contentType = ctType[0]
	}
	ct.objName = objName
	ct.fqn = fs.CSM.GenContentFQN(ct, ct.contentType, "")
	return
}

// Construct CT from LOM and change ContentType and FQN
func NewCTFromLOM(lom *LOM, ctType string) *CT {
	return &CT{
		fqn:       fs.CSM.GenContentFQN(lom, ctType, ""),
		bck:       lom.Bck(),
		objName:   lom.ObjName,
		mpathInfo: lom.mpathInfo,
	}
}

// Clone CT and change ContentType and FQN
func (ct *CT) Clone(ctType string) *CT {
	return &CT{
		fqn:         fs.CSM.GenContentFQN(ct, ctType, ""),
		bck:         ct.bck,
		objName:     ct.objName,
		contentType: ctType,
		mpathInfo:   ct.mpathInfo,
	}
}

func (ct *CT) Make(toType string, pref ...string /*optional prefix*/) string {
	var prefix string
	cmn.Assert(toType != "")

	if len(pref) > 0 {
		prefix = pref[0]
	}
	return fs.CSM.GenContentFQN(ct, toType, prefix)
}

// Save CT to local drives. If workFQN is set, it saves in two steps: first,
// save to workFQN; second, rename workFQN to ct.FQN. If unset, it writes
// directly to ct.FQN
func (ct *CT) Write(t Target, reader io.Reader, size int64, workFQN ...string) (err error) {
	bdir := ct.mpathInfo.MakePathBck(ct.bck.Bck)
	if err := fs.Access(bdir); err != nil {
		return err
	}
	buf, slab := t.MMSA().Alloc()
	if len(workFQN) == 0 {
		_, err = cmn.SaveReader(ct.fqn, reader, buf, cmn.ChecksumNone, size, "")
	} else {
		_, err = cmn.SaveReaderSafe(workFQN[0], ct.fqn, reader, buf, cmn.ChecksumNone, size, "")
	}
	slab.Free(buf)
	return err
}
