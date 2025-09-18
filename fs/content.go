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

	PartsFQN interface {
		ObjectName() string
		Bucket() *cmn.Bck
		Mountpath() *Mountpath
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

const cttag = "content type"

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

func (f *contentSpecMgr) regAll() {
	f._reg(ObjCT, &objCR{})
	f._reg(WorkCT, &workCR{})
	f._reg(ECSliceCT, &ecSliceCR{})
	f._reg(ECMetaCT, &ecMetaCR{})
	f._reg(ChunkCT, &objChunkCR{})
	f._reg(ChunkMetaCT, &chunkMetaCR{})

	f._reg(DsortFileCT, &dsortCR{})
	f._reg(DsortWorkCT, &dsortCR{})
}

// register (content-type, resolver) pair
func (f *contentSpecMgr) _reg(contentType string, cr contentRes) {
	if strings.ContainsRune(contentType, filepath.Separator) {
		panic(fmt.Errorf("contentSpecMgr: %s %q cannot contain %q", cttag, contentType, filepath.Separator))
	}
	if len(contentType) != contentTypeLen {
		panic(fmt.Errorf("contentSpecMgr: %s %q must have length %d", cttag, contentType, contentTypeLen))
	}
	if _, ok := f.m[contentType]; ok {
		debug.AssertNoErr(fmt.Errorf("%s %q is already registered", cttag, contentType))
	}
	f.m[contentType] = cr
}

// generate FQN from given parts and optional extras
func (f *contentSpecMgr) Gen(parts PartsFQN, contentType string, extras ...string) (fqn string) {
	debug.Assert(len(f.m) > 0, "not initialized")
	cr, ok := f.m[contentType]
	debug.Assert(ok, contentType)

	// make new (and unique) base
	objName := parts.ObjectName()

	// Ensure that generated FQN never exceeds NAME_MAX.
	// On Linux, syscall.ENAMETOOLONG (0x24) is raised if *any* path component is > 255 chars.
	// - bucket names are already validated (≤64), and our "extras" are short markers
	//   like PID, uploadID and chunk number, etc.
	// - the only unbounded input is the user-provided object name, so we shorten it
	//   here if needed.
	// See also: core/lom for "fntl"/"Fntl"

	if IsFntl(objName) {
		objName = ShortenFntl(objName)
	}

	ubase := cr.makeUbase(objName, extras...)
	debug.Assertf(!IsFntl(ubase), "ubase too long [%q (%q, %q) %v]", // (unlikely)
		contentType, objName, parts.ObjectName(), extras)

	return parts.Mountpath().MakePathFQN(parts.Bucket(), contentType, ubase)
}

// parse content info back from provided `ubase` (see respective makeUbase())
func (f *contentSpecMgr) ParseUbase(ubase, contentType string) ContentInfo {
	cr, ok := f.m[contentType]
	debug.Assert(ok, contentType)
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
