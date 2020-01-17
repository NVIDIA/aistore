// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
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
func (ct *CT) Bucket() string           { return ct.bck.Name }
func (ct *CT) Provider() string         { return ct.bck.Provider }
func (ct *CT) Bprops() *cmn.BucketProps { return ct.bck.Props }
func (ct *CT) Bck() *Bck                { return ct.bck }
func (ct *CT) ParsedFQN() fs.ParsedFQN  { return ct.parsedFQN }

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
	ct.bck = &Bck{Name: ct.parsedFQN.Bucket, Provider: ct.parsedFQN.Provider}
	if b != nil {
		err = ct.bck.Init(b)
	}
	return
}

func NewCTFromBO(bckName, bckProvider, objName string, b Bowner) (ct *CT, err error) {
	ct = &CT{
		bck:     &Bck{Name: bckName, Provider: bckProvider},
		objName: objName,
	}
	if b != nil {
		if err = ct.bck.Init(b); err != nil {
			return
		}
	}
	ct.parsedFQN.MpathInfo, ct.parsedFQN.Digest, err = HrwMpath(ct.bck.MakeUname(objName))
	if err != nil {
		return
	}
	ct.parsedFQN.ContentType = fs.ObjectType
	ct.parsedFQN.Provider = ct.bck.Provider
	ct.parsedFQN.Bucket, ct.parsedFQN.ObjName = bckName, objName
	return
}

func (ct *CT) Make(toType string, pref ...string /*optional prefix*/) string {
	var prefix string
	cmn.Assert(toType != "")

	if len(pref) > 0 {
		prefix = pref[0]
	}
	return fs.CSM.GenContentParsedFQN(ct.parsedFQN, toType, prefix)
}
