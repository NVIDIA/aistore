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
	fqn     string // FQN to make it from
	bck     *Bck   // bucket
	objName string // object name

	parsedFQN fs.ParsedFQN
}

func (ct *CT) ContentType() string      { return ct.parsedFQN.ContentType }
func (ct *CT) ObjName() string          { return ct.parsedFQN.ObjName }
func (ct *CT) Bprops() *cmn.BucketProps { return ct.bck.Props }
func (ct *CT) Bck() *Bck                { return ct.bck }
func (ct *CT) ParsedFQN() fs.ParsedFQN  { return ct.parsedFQN }
func (ct *CT) FQN() string              { return ct.fqn }

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
	ct = &CT{
		fqn: fqn,
	}
	ct.parsedFQN, _, err = ResolveFQN(fqn)
	if err != nil {
		return ct, err
	}
	ct.bck = &Bck{Bck: ct.parsedFQN.Bck}
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
	ct.parsedFQN.MpathInfo, ct.parsedFQN.Digest, err = HrwMpath(ct.bck.MakeUname(objName))
	if err != nil {
		return
	}
	if len(ctType) == 0 {
		ct.parsedFQN.ContentType = fs.ObjectType
	} else {
		ct.parsedFQN.ContentType = ctType[0]
	}
	ct.parsedFQN.Bck = ct.bck.Bck
	ct.parsedFQN.ObjName = objName
	ct.fqn = fs.CSM.GenContentParsedFQN(ct.parsedFQN, ct.parsedFQN.ContentType, "" /* workfile prefix */)
	return
}

// Construct CT from LOM and change ContentType and FQN
func NewCTFromLOM(lom *LOM, ctType string) *CT {
	return &CT{
		fqn:       fs.CSM.GenContentParsedFQN(lom.ParsedFQN, ctType, "" /* workfile prefix */),
		bck:       lom.Bck(),
		objName:   lom.ObjName,
		parsedFQN: lom.ParsedFQN,
	}
}

// Clone CT and change ContentType and FQN
func (ct *CT) Clone(ctType string) *CT {
	return &CT{
		fqn:       fs.CSM.GenContentParsedFQN(ct.parsedFQN, ctType, "" /* workfile prefix */),
		bck:       ct.bck,
		objName:   ct.objName,
		parsedFQN: ct.parsedFQN,
	}
}

func (ct *CT) Make(toType string, pref ...string /*optional prefix*/) string {
	var prefix string
	cmn.Assert(toType != "")

	if len(pref) > 0 {
		prefix = pref[0]
	}
	return fs.CSM.GenContentParsedFQN(ct.parsedFQN, toType, prefix)
}

// Save CT to local drives. If workFQN is set, it saves in two steps: first,
// save to workFQN; second, rename workFQN to ct.FQN. If unset, it writes
// directly to ct.FQN
func (ct *CT) Write(t Target, reader io.Reader, size int64, workFQN ...string) (err error) {
	bdir := ct.parsedFQN.MpathInfo.MakePathBck(ct.bck.Bck)
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
