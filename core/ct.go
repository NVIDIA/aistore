// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"
	"io"
	"os"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

//////////////////////////////
// ais on-disk content type //
//////////////////////////////

type CT struct {
	fqn         string
	objName     string
	contentType string
	hrwFQN      *string
	bck         *meta.Bck
	mi          *fs.Mountpath
	uname       *string
	digest      uint64
	size        int64
	mtime       int64
}

// interface guard
var _ fs.PartsFQN = (*CT)(nil)

func (ct *CT) FQN() string              { return ct.fqn }
func (ct *CT) ObjectName() string       { return ct.objName }
func (ct *CT) ContentType() string      { return ct.contentType }
func (ct *CT) Bck() *meta.Bck           { return ct.bck }
func (ct *CT) Bucket() *cmn.Bck         { return (*cmn.Bck)(ct.bck) }
func (ct *CT) Mountpath() *fs.Mountpath { return ct.mi }
func (ct *CT) Lsize() int64             { return ct.size }
func (ct *CT) MtimeUnix() int64         { return ct.mtime }
func (ct *CT) Digest() uint64           { return ct.digest }
func (ct *CT) Cname() string            { return ct.bck.Cname(ct.objName) }

func (ct *CT) LoadSliceFromFS() error {
	debug.Assert(ct.ContentType() == fs.ECSliceType, "unexpected content type: ", ct.ContentType())
	st, err := os.Stat(ct.FQN())
	if err != nil {
		return err
	}
	ct.size = st.Size()
	ct.mtime = st.ModTime().UnixNano()
	return nil
}

func (ct *CT) UnamePtr() *string {
	if ct.uname == nil {
		uname := ct.bck.MakeUname(ct.objName)
		ct.uname = cos.UnsafeSptr(uname)
	}
	return ct.uname
}

func (ct *CT) CacheIdx() int      { return fs.LcacheIdx(ct.digest) }
func (ct *CT) getLomLocker() *nlc { return &g.locker[ct.CacheIdx()] }

func (ct *CT) Lock(exclusive bool) {
	nlc := ct.getLomLocker()
	uname := ct.UnamePtr()
	nlc.Lock(*uname, exclusive)
}

func (ct *CT) Unlock(exclusive bool) {
	nlc := ct.getLomLocker()
	uname := ct.UnamePtr()
	nlc.Unlock(*uname, exclusive)
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

func NewCTFromFQN(fqn string, b meta.Bowner) (ct *CT, err error) {
	var (
		hrwFQN string
		parsed fs.ParsedFQN
	)
	if hrwFQN, err = ResolveFQN(fqn, &parsed); err != nil {
		return nil, err
	}
	ct = &CT{
		fqn:         fqn,
		objName:     parsed.ObjName,
		contentType: parsed.ContentType,
		hrwFQN:      &hrwFQN,
		bck:         meta.CloneBck(&parsed.Bck),
		mi:          parsed.Mountpath,
		digest:      parsed.Digest,
	}
	if b != nil {
		err = ct.bck.InitFast(b)
	}
	return ct, err
}

func NewCTFromBO(bck *cmn.Bck, objName string, b meta.Bowner, ctType ...string) (ct *CT, err error) {
	ct = &CT{objName: objName, bck: meta.CloneBck(bck)}
	if b != nil {
		if err = ct.bck.Init(b); err != nil {
			return
		}
	}
	var digest uint64
	ct.mi, digest, err = fs.Hrw(ct.bck.MakeUname(objName))
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
		digest:      lom.digest,
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

func (ct *CT) Make(toType string) string {
	debug.Assert(toType != "")
	return fs.CSM.Gen(ct, toType, "")
}

// Save CT to local drives. If workFQN is set, it saves in two steps: first,
// save to workFQN; second, rename workFQN to ct.fqn. If unset, it writes
// directly to ct.fqn
func (ct *CT) Write(reader io.Reader, size int64, workFQN string) (err error) {
	bdir := ct.mi.MakePathBck(ct.Bucket())
	if err = cos.Stat(bdir); err != nil {
		return &errBdir{ct.Cname(), err}
	}
	buf, slab := g.pmm.Alloc()
	if workFQN == "" {
		_, err = cos.SaveReader(ct.fqn, reader, buf, cos.ChecksumNone, size)
	} else {
		_, err = ct.saveAndRename(workFQN, reader, buf, cos.ChecksumNone, size)
	}
	slab.Free(buf)
	return err
}

func (ct *CT) saveAndRename(tmpfqn string, reader io.Reader, buf []byte, cksumType string, size int64) (cksum *cos.CksumHash, err error) {
	if cksum, err = cos.SaveReader(tmpfqn, reader, buf, cksumType, size); err != nil {
		return
	}
	if err = cos.Rename(tmpfqn, ct.fqn); err != nil {
		err = fmt.Errorf("failed to rename temp to %s: %w", ct.Cname(), err)
		if rmErr := cos.RemoveFile(tmpfqn); rmErr != nil {
			nlog.Errorln("nested error:", err, "[ failed to remove temp fqn:", rmErr, "]")
		}
	}
	return
}
