// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	contentTypeLen = 2

	ObjCT       = "ob"
	WorkCT      = "wk"
	ECSliceCT   = "ec"
	ECMetaCT    = "mt"
	ChunkCT     = "ch"
	ChunkMetaCT = "ut"

	// ext
	DsortFileCT = "ds"
	DsortWorkCT = "dw"
)

const (
	ssepa = "."
	bsepa = '.'
)

type (
	ContentInfo struct {
		Base   string   // original base
		Extras []string // original `extras` used to generate ubase; nil when not relevant (or not used)
		Old    bool     // is only used to indicate old WorkCT
		Ok     bool     // success or fail
	}

	contentRes interface {
		// generate unique base name from the original one
		// * extras - user-defined marker(s)
		makeUbase(base string, extras ...string) (ubase string)

		// Parse ubase back to ContentInfo
		parseUbase(ubase string) ContentInfo
	}

	contentSpecMgr struct {
		m map[string]contentRes
	}
)

type (
	objCR       struct{}
	workCR      struct{}
	ecSliceCR   struct{}
	ecMetaCR    struct{}
	objChunkCR  struct{}
	chunkMetaCR struct{}
	dsortCR     struct{}
)

const (
	cttag  = "content type"
	csmtag = "content-spec"
)

var (
	CSM *contentSpecMgr

	pid  int64 = 0xDEADBEEF   // pid of the current process
	spid       = "0xDEADBEEF" // string version of the pid

	_once sync.Once
)

// interface guard
var (
	_ contentRes = (*objCR)(nil)
	_ contentRes = (*workCR)(nil)
	_ contentRes = (*ecSliceCR)(nil)
	_ contentRes = (*ecMetaCR)(nil)
	_ contentRes = (*objChunkCR)(nil)
	_ contentRes = (*chunkMetaCR)(nil)
)

// register all content types
func initCSM() {
	CSM = &contentSpecMgr{m: make(map[string]contentRes, 8)}

	pid = int64(os.Getpid())
	spid = strconv.FormatInt(pid, 16)

	CSM.regAll()
}

func (csm *contentSpecMgr) regAll() {
	csm._reg(ObjCT, &objCR{})
	csm._reg(WorkCT, &workCR{})
	csm._reg(ECSliceCT, &ecSliceCR{})
	csm._reg(ECMetaCT, &ecMetaCR{})
	csm._reg(ChunkCT, &objChunkCR{})
	csm._reg(ChunkMetaCT, &chunkMetaCR{})

	csm._reg(DsortFileCT, &dsortCR{})
	csm._reg(DsortWorkCT, &dsortCR{})
}

// register (content-type, resolver) pair
func (csm *contentSpecMgr) _reg(cttype string, cr contentRes) {
	if strings.ContainsRune(cttype, filepath.Separator) {
		panic(fmt.Errorf("%s: %s %q cannot contain %q", csmtag, cttag, cttype, filepath.Separator))
	}
	if len(cttype) != contentTypeLen {
		panic(fmt.Errorf("%s: %s %q must have length %d", csmtag, cttag, cttype, contentTypeLen))
	}
	if _, ok := csm.m[cttype]; ok {
		debug.AssertNoErr(fmt.Errorf("%s: %s %q is already registered", csmtag, cttag, cttype))
	}
	csm.m[cttype] = cr
}

// generate FQN from given parts and optional extras
func (csm *contentSpecMgr) Gen(objName, cttype string, bck *cmn.Bck, mi *Mountpath, extras ...string) (fqn string) {
	debug.Assert(len(csm.m) > 0, "not initialized")
	cr, ok := csm.m[cttype]
	debug.Assert(ok, cttype)

	// Make sure that generated FQN never exceeds NAME_MAX.
	// On Linux, syscall.ENAMETOOLONG (0x24) is raised if *any* path component is > 255 chars.
	// - bucket names are already validated (≤64), and our "extras" are short markers
	//   like PID, uploadID and chunk number, etc.
	// - the only unbounded input is the user-provided object name, so we shorten it
	//   here if needed.
	// See also: core/lom for "fntl"/"Fntl"
	var oname = objName
	if IsFntl(objName) {
		oname = ShortenFntl(objName)
	}

	// new (and unique) base
	ubase := cr.makeUbase(oname, extras...)
	debug.Assertf(!IsFntl(ubase), "ubase too long [%q (%q, %q) %v]", cttype, oname, objName, extras) // (unlikely)

	return mi.MakePathFQN(bck, cttype, ubase)
}

// parse content info back from provided `ubase` (see respective makeUbase())
func (csm *contentSpecMgr) ParseUbase(ubase, cttype string) ContentInfo {
	cr, ok := csm.m[cttype]
	debug.Assert(ok, cttype)
	return cr.parseUbase(ubase)
}

//
// private methods
// common content types
//

func (*objCR) makeUbase(base string, _ ...string) string { return base }

func (*objCR) parseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}

func (*workCR) makeUbase(base string, extras ...string) string {
	debug.Assert(len(extras) == 1, extras)
	debug.Assert(extras[0] != "", "work prefix cannot be empty")
	var (
		dir, fname = filepath.Split(base)
		tieBreaker = cos.GenTie()
	)
	debug.Assert(len(extras) > 0)
	fname = extras[0] + ssepa + fname
	base = filepath.Join(dir, fname)
	return base + ssepa + tieBreaker + ssepa + spid
}

func (*workCR) parseUbase(base string) (ci ContentInfo) {
	// remove original content type
	cntIndex := strings.IndexByte(base, bsepa)
	if cntIndex < 0 {
		return
	}
	base = base[cntIndex+1:]

	pidIndex := strings.LastIndexByte(base, bsepa) // pid
	if pidIndex < 0 {
		return
	}
	tieIndex := strings.LastIndexByte(base[:pidIndex], bsepa) // tie breaker
	if tieIndex < 0 {
		return
	}
	filePID, err := strconv.ParseInt(base[pidIndex+1:], 16, 64)
	if err != nil {
		return
	}

	return ContentInfo{Base: base[:tieIndex], Old: filePID != pid, Ok: true}
}

// The following content resolvers handle temporary or derived files that don't require
// complex filename parsing. They rely on directory structure (%ch/, %ec/, %mt/) rather
// than filename encoding for organization:
//
// - objChunkCR: temporary chunk files during chunked uploads
//   Named by the chunk manifest (e.g., objectname.sessionID.0001)
//   Cleaned up by session ID after upload completion/failure
//
// - ecSliceCR: erasure coding data slices (%ec/ directory)
//   Generated during EC encoding, cleaned up with the original object
//
// - ecMetaCR: erasure coding metadata (%mt/ directory)
//   Contains EC reconstruction information, lifecycle tied to EC slices
//
// All use pass-through makeUbase/parseUbase since the content type
// in the path provides sufficient identification for cleanup and management.

func (*objChunkCR) makeUbase(base string, extras ...string) string {
	debug.Assert(len(extras) == 2, "expecting uploadID and chunk number, got: ", extras)
	debug.Assert(cos.ValidateManifestID(extras[0]) == nil, "uploadID must be valid")
	debug.Assert(extras[1] != "", "chunk number must be non-empty")
	return base + ssepa + extras[0] + ssepa + extras[1]
}

func (*objChunkCR) parseUbase(base string) (ci ContentInfo) {
	// (chunk number)
	i := strings.LastIndexByte(base, bsepa)
	if i < 0 {
		return
	}

	// (upload ID)
	j := strings.LastIndexByte(base[:i], bsepa)
	if j < 0 {
		return
	}

	uploadID, chunkNum := base[j+1:i], base[i+1:]
	if err := cos.ValidateManifestID(uploadID); err != nil {
		return
	}
	if _, err := strconv.Atoi(chunkNum); err != nil {
		return
	}

	return ContentInfo{Base: base[:j], Extras: []string{uploadID, chunkNum}, Ok: true}
}

func (*chunkMetaCR) makeUbase(base string, extras ...string) string {
	if len(extras) == 0 {
		return base // completed
	}
	debug.Assert(len(extras) == 1, extras)
	debug.Assert(cos.ValidateManifestID(extras[0]) == nil, "uploadID must be valid")
	return base + ssepa + extras[0] // partial (with uploadID)
}

func (*chunkMetaCR) parseUbase(base string) ContentInfo {
	i := strings.LastIndexByte(base, bsepa)
	if i < 0 {
		return ContentInfo{Base: base, Ok: true} // completed
	}
	uploadID := base[i+1:]
	if err := cos.ValidateManifestID(uploadID); err != nil {
		return ContentInfo{}
	}
	return ContentInfo{Base: base[:i], Extras: []string{uploadID}, Ok: true} // partial
}

func (*ecSliceCR) makeUbase(base string, _ ...string) string { return base }

func (*ecSliceCR) parseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}

func (*ecMetaCR) makeUbase(base string, _ ...string) string { return base }

func (*ecMetaCR) parseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}

func (*dsortCR) makeUbase(base string, _ ...string) string { return base }

func (*dsortCR) parseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}
