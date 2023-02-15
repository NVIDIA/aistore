// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
)

//////////////////////////////
// ais on-disk content type //
//////////////////////////////

type CT struct {
	fqn         string
	objName     string
	contentType string
	hrwFQN      string
	bck         *Bck
	mi          *fs.Mountpath
	uname       string
	digest      uint64
	size        int64
	mtime       int64
}

// interface guard
var _ fs.PartsFQN = (*CT)(nil)

func (ct *CT) FQN() string              { return ct.fqn }
func (ct *CT) ObjectName() string       { return ct.objName }
func (ct *CT) ContentType() string      { return ct.contentType }
func (ct *CT) Bck() *Bck                { return ct.bck }
func (ct *CT) Bucket() *cmn.Bck         { return (*cmn.Bck)(ct.bck) }
func (ct *CT) Mountpath() *fs.Mountpath { return ct.mi }
func (ct *CT) SizeBytes() int64         { return ct.size }
func (ct *CT) MtimeUnix() int64         { return ct.mtime }

func (ct *CT) LoadFromFS() error {
	st, err := os.Stat(ct.FQN())
	if err != nil {
		return err
	}
	ct.size = st.Size()
	ct.mtime = st.ModTime().UnixNano()
	return nil
}

func (ct *CT) Uname() string {
	if ct.uname == "" {
		ct.uname = ct.bck.MakeUname(ct.objName)
	}
	return ct.uname
}

func (ct *CT) CacheIdx() int      { return fs.LcacheIdx(ct.digest) }
func (ct *CT) getLomLocker() *nlc { return &lomLocker[ct.CacheIdx()] }

func (ct *CT) Lock(exclusive bool) {
	nlc := ct.getLomLocker()
	nlc.Lock(ct.Uname(), exclusive)
}

func (ct *CT) Unlock(exclusive bool) {
	nlc := ct.getLomLocker()
	nlc.Unlock(ct.Uname(), exclusive)
}

// e.g.: generate workfile FQN from object FQN:
//  ct, err := NewCTFromFQN(fqn, nil)
//  if err != nil { ... }
//  fqn := ct.Make(fs.WorkfileType)
//
// e.g.: generate EC metafile FQN from bucket name, backend provider and object name:
//  ct, err := NewCTFromBO(bckName, bckProvider, objName, nil)
//  if err != nil { ... }
//  fqn := ct.Make(fs.ECMetaType)

func NewCTFromFQN(fqn string, b Bowner) (ct *CT, err error) {
	parsedFQN, hrwFQN, errP := ResolveFQN(fqn)
	if errP != nil {
		return nil, errP
	}
	ct = &CT{
		fqn:         fqn,
		objName:     parsedFQN.ObjName,
		contentType: parsedFQN.ContentType,
		hrwFQN:      hrwFQN,
		bck:         CloneBck(&parsedFQN.Bck),
		mi:          parsedFQN.Mountpath,
		digest:      parsedFQN.Digest,
	}
	if b != nil {
		err = ct.bck.initFast(b)
	}
	return
}

func NewCTFromBO(bck *cmn.Bck, objName string, b Bowner, ctType ...string) (ct *CT, err error) {
	ct = &CT{objName: objName, bck: CloneBck(bck)}
	if b != nil {
		if err = ct.bck.Init(b); err != nil {
			return
		}
	}
	var digest uint64
	ct.mi, digest, err = HrwMpath(ct.bck.MakeUname(objName))
	if err != nil {
		return
	}
	ct.digest = digest
	if len(ctType) == 0 {
		ct.contentType = fs.ObjectType
	} else {
		ct.contentType = ctType[0]
	}
	ct.fqn = fs.CSM.Gen(ct, ct.contentType, "")
	return
}

// Construct CT from LOM and change ContentType and FQN
func NewCTFromLOM(lom *LOM, ctType string) *CT {
	return &CT{
		fqn:         fs.CSM.Gen(lom, ctType, ""),
		objName:     lom.ObjName,
		contentType: ctType,
		bck:         lom.Bck(),
		mi:          lom.mi,
		digest:      lom.mpathDigest,
	}
}

// Clone CT and change ContentType and FQN
func (ct *CT) Clone(ctType string) *CT {
	return &CT{
		fqn:         fs.CSM.Gen(ct, ctType, ""),
		objName:     ct.objName,
		contentType: ctType,
		bck:         ct.bck,
		mi:          ct.mi,
		digest:      ct.digest,
	}
}

func (ct *CT) Make(toType string, pref ...string /*optional prefix*/) string {
	var prefix string
	cos.Assert(toType != "")

	if len(pref) > 0 {
		prefix = pref[0]
	}
	return fs.CSM.Gen(ct, toType, prefix)
}

// Save CT to local drives. If workFQN is set, it saves in two steps: first,
// save to workFQN; second, rename workFQN to ct.FQN. If unset, it writes
// directly to ct.FQN
func (ct *CT) Write(t Target, reader io.Reader, size int64, workFQN ...string) (err error) {
	bdir := ct.mi.MakePathBck(ct.Bucket())
	if err := cos.Stat(bdir); err != nil {
		return err
	}
	buf, slab := t.PageMM().Alloc()
	if len(workFQN) == 0 {
		_, err = cos.SaveReader(ct.fqn, reader, buf, cos.ChecksumNone, size, "")
	} else {
		_, err = cos.SaveReaderSafe(workFQN[0], ct.fqn, reader, buf, cos.ChecksumNone, size, "")
	}
	slab.Free(buf)
	return err
}
