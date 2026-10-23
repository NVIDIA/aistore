// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

// FNTL: file name too long (0x24)
//
// The shortened `.x<sha256>` is exclusively used for local-storage encoding only.
// Its **boundary** is low-level local filesystem operations: the shortened name
// must **never** be used as the object's logical name, must never escape into
// other layers or components.
// - scope: open, stat, rename, unlink, xattr, and physical walks
// - all other uses retain the original name: uname, lock, cache, HRW,
//   intra-cluster requests and transport headers, listing, and API responses
// - original => shortened: at the filesystem syscall, on ENAMETOOLONG or
//   proactively (shortenFntl; see also fs.CSM.Gen)
// - shortened => original: at the start of a physical walk, before processing
//   the name (initFntl)
// - `orig_fntl` (custom MD): written with the shortened file, read only by initFntl
// - everywhere else, the original name is OrigObjName

// - TODO: lom.ObjName of a loaded FNTL object is the shortened name; use
//   OrigObjName outside the local-filesystem boundary

func (lom *LOM) IsFntl() bool { return lom.md.lid.haslmfl(lmflFntl) }

// (compare with fs/content Gen())
func (lom *LOM) shortenFntl() []string {
	debug.AssertFunc(func() bool { return fs.IsFntl(lom.ObjName) }, lom.FQN)

	noname := fs.ShortenFntl(lom.FQN)
	nfqn := lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, noname)

	debug.Assert(len(nfqn) < 4096, "PATH_MAX /usr/include/limits.h", len(nfqn))
	return []string{nfqn, noname}
}

func (lom *LOM) fixupFntl() {
	if !fs.IsFntl(lom.ObjName) {
		return
	}
	lom.ObjName = fs.ShortenFntl(lom.FQN)                             // noname
	lom.FQN = lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName) // nfqn
	lom.setHRW(true)
}

// a LOM from the FQN of a shortened (`.x`) file takes the uname, digest, and HRW of the original name
// (the same identity as a LOM from that name)
// on any error (or `orig_fntl` of another bucket - e.g. a copy), keep the short identity (return false)
func (lom *LOM) initFntl() bool {
	md, err := lom.lmfs(false)
	if err != nil {
		return false
	}
	ofqn, ok := md.GetCustomKey(cmn.OrigFntl)
	if !ok || lom.ObjName != fs.ShortenFntl(ofqn) {
		return false
	}
	var parsed fs.ParsedFQN
	if parsed.Init(ofqn) != nil || !fs.IsFntl(parsed.ObjName) || !parsed.Bck.Equal(lom.Bucket()) {
		return false
	}
	uname := lom.bck.MakeUname(parsed.ObjName)
	mi, digest, err := fs.Hrw(uname)
	if err != nil {
		return false
	}
	lom.md.uname = cos.UnsafeSptr(uname)
	lom.digest = digest
	// the short name is the hash of the long FQN (`ofqn`, checked above):
	// HRW iff both the file and `ofqn` are on the HRW mountpath
	lom.setHRW(lom.mi.Path == mi.Path && parsed.Mountpath.Path == mi.Path)
	return true
}

// the object name that the other targets know
// (`lom.ObjName` of a FNTL object is the shortened name, the uname keeps the original)
func (lom *LOM) OrigObjName() string {
	if !fs.IsShortFntl(lom.ObjName) {
		return lom.ObjName
	}
	_, objName := cmn.ParseUname(lom.Uname())
	return objName
}

// when original (long) name did not resolve via initFntl - exceptions
// including: bucket mismatch, mountpath lifecycle, transient failures
// (current usage: global rebalance)
func (lom *LOM) IsLoadedUnresolvedFntl() bool {
	return lom.IsFntl() && lom.OrigObjName() == lom.ObjName
}

func (lom *LOM) pushFntl(short []string) (saved []string) {
	saved = []string{lom.FQN, lom.ObjName}
	lom.FQN, lom.ObjName = short[0], short[1]
	return saved
}

func (lom *LOM) popFntl(saved []string) {
	lom.FQN, lom.ObjName = saved[0], saved[1]
}
