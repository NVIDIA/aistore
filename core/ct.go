// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	hrwFQN      *string
	bck         *meta.Bck
	mi          *fs.Mountpath
	uname       *string
	fqn         string
	objName     string
	contentType string
	digest      uint64
	size        int64
	mtime       int64
}

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
	debug.Assert(ct.ContentType() == fs.ECSliceCT, "unexpected content type: ", ct.ContentType())
	st, err := os.Lstat(ct.FQN())
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

func (ct *CT) getLomLocker() *nlc { return &g.locker[lcacheIdx(ct.digest)] } // see also: lom.CacheIdx()

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

//
// begin: CT constructors (flavors) -----------------------------------------------------
//

// full construction and init, including:
// - parse fqn
// - init bucket
// - HRW
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

// usage: dsort only
func NewDsortCT(bck *cmn.Bck, objName string) (ct *CT, err error) {
	ct = &CT{objName: objName, bck: meta.CloneBck(bck), contentType: fs.ObjCT}
	err = ct.init()
	return
}

// bare minimum: from fs.ParsedFQN
func NewCTFromParsed(parsed *fs.ParsedFQN, fqn string) *CT {
	return &CT{
		fqn:         fqn,
		objName:     parsed.ObjName,
		contentType: parsed.ContentType,
		bck:         meta.CloneBck(&parsed.Bck),
		mi:          parsed.Mountpath,
		digest:      parsed.Digest,
	}
}

// bare minimum: from LOM
func NewCTFromLOM(lom *LOM, ctType string, extras ...string) (ct *CT) {
	ct = &CT{
		objName:     lom.ObjName,
		contentType: ctType,
		bck:         lom.Bck(),
		mi:          lom.mi,
		digest:      lom.digest,
	}
	ct.fqn = ct.GenFQN("", extras...)
	return ct
}

// same as above but with a designated mountpath
func newChunkCT(lom *LOM, mi *fs.Mountpath) (ct *CT) {
	ct = &CT{objName: lom.ObjName, bck: lom.Bck(), contentType: fs.ChunkCT}
	ct.mi = mi // instead of ct.init()
	return
}

// bare minimum: from CT
func (ct *CT) Clone(ctType string) (clone *CT) {
	clone = &CT{
		objName:     ct.objName,
		contentType: ctType,
		bck:         ct.bck,
		mi:          ct.mi,
		digest:      ct.digest,
	}
	clone.fqn = clone.GenFQN("")
	return clone
}

// construct, init bucket and compute HRW
// (similar to lom.InitBck())
func NewCTFromBO(bck *meta.Bck, objName, ctType string, extras ...string) (ct *CT, err error) {
	ct = &CT{objName: objName, bck: bck, contentType: ctType}
	if err = ct.bck.Init(T.Bowner()); err != nil {
		return
	}
	err = ct.init(extras...)
	return
}

func (ct *CT) init(extras ...string) error {
	mi, digest, err := fs.Hrw(ct.bck.MakeUname(ct.objName))
	if err != nil {
		return err
	}
	ct.mi, ct.digest = mi, digest

	switch ct.contentType {
	case fs.WorkCT:
		debug.Assert(len(extras) == 1 && extras[0] != "", "WorkCT requires work prefix")
		ct.fqn = ct.GenFQN("", extras[0])
	case fs.ChunkCT:
		debug.Assert(len(extras) == 2, "ChunkCT requires uploadID and chunk number")
		ct.fqn = ct.GenFQN("", extras[0], extras[1])
	case fs.ChunkMetaCT:
		debug.Assert(len(extras) <= 1, "ChunkMetaCT takes optional uploadID")
		ct.fqn = ct.GenFQN("", extras...)
	default:
		// NOTE: `extras` are only meaningful for Work/Chunk content-types, others ignore them
		// (may consider asserting though after adding an `if` into tools.PrepareObjects)
		ct.fqn = ct.GenFQN("")
	}
	return nil
}

//
// end: CT constructors (flavors) -----------------------------------------------------
//

// Save CT to local drives. If workFQN is set, it saves in two steps: first,
// save to workFQN; second, rename workFQN to ct.fqn. If unset, it writes
// directly to ct.fqn
func (ct *CT) Write(reader io.Reader, size int64, workFQN string) (err error) {
	bdir := ct.mi.MakePathBck(ct.Bucket())
	if err = cos.Stat(bdir); err != nil {
		return &errBdir{cname: ct.Cname(), err: err}
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

// Content FQN makers
//
// Both LOM and CT provide convenience methods to generate a fully qualified name (FQN)
// for any derived piece of content (workfiles, EC slices, chunks, etc.).
// Internally both delegate to fs.CSM.Gen, which applies type-specific naming rules
// (shortening long object names, attaching tie-breakers, etc.).
//
// - LOM.GenFQN(cttype, extras...)
//   Use when you have an initialized (not necessarily loaded) LOM.
//  `cttype` must be one of the `fs` declared constants (WorkCT, ChunkCT, ChunkMetaCT, ...).
//
// - CT.GenFQN(cttype, extras...)
//   Same as above but for an existing CT. If `cttype` is empty, the CT's own
//   content type is reused.
//
// - `extras` provide disambiguation (e.g., work tag, uploadID, chunk number).

func (lom *LOM) GenFQN(cttype string, extras ...string) string {
	debug.Assert(lom.mi != nil)
	return fs.CSM.Gen(lom.ObjName, cttype, lom.Bucket(), lom.mi, extras...)
}

func (ct *CT) GenFQN(cttype string, extras ...string) string {
	if cttype == "" {
		cttype = ct.contentType
	}
	debug.Assert(cttype != "")
	return fs.CSM.Gen(ct.objName, cttype, ct.Bucket(), ct.Mountpath(), extras...)
}
