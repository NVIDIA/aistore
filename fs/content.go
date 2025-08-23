// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

/*
 * Besides objects we must deal with additional files like: workfiles, dsort
 * intermediate files (used when spilling to disk) or EC slices. These files can
 * have different rules of rebalancing, evicting and other processing. Each
 * content type needs to implement ContentRes to reflect the rules and
 * permission for different services. To see how the interface can be
 * implemented see: DefaultWorkfile implemention.
 *
 * When walking through the files we need to know if the file is an object or
 * other content. To do that we generate fqn with Gen. It adds short
 * prefix (or suffix) to the base name, which we believe is unique and will separate objects
 * from content files. We parse the file type to run ParseFQN (implemented
 * by this file type) on the rest of the base name.
 */

const (
	contentTypeLen = 2

	ObjCT       = "ob"
	WorkCT      = "wk"
	ECSliceCT   = "ec"
	ECMetaCT    = "mt"
	ChunkCT     = "ch"
	ChunkMetaCT = "ut"
)

const (
	ssepa = "."
	bsepa = '.'
)

type (
	ContentRes interface {
		// Generates unique base name for original one. This function may add
		// additional information to the base name.
		// extras - user-defined marker(s)
		MakeFQN(base string, extras ...string) (ufqn string)
		// Parses generated unique fqn to the original one.
		ParseFQN(base string) (orig string, old, ok bool)
	}

	PartsFQN interface {
		ObjectName() string
		Bucket() *cmn.Bck
		Mountpath() *Mountpath
	}

	ContentInfo struct {
		Dir  string // original directory
		Base string // original basename
		Type string // content type
		Old  bool   // true if old (subj. to space cleanup)
	}

	contentSpecMgr struct {
		m map[string]ContentRes
	}
)

type (
	ObjectContentRes    struct{}
	WorkContentRes      struct{}
	ECSliceContentRes   struct{}
	ECMetaContentRes    struct{}
	ObjChunkContentRes  struct{}
	ChunkMetaContentRes struct{}
)

const cttag = "content type"

var CSM *contentSpecMgr

// interface guard
var (
	_ ContentRes = (*ObjectContentRes)(nil)
	_ ContentRes = (*WorkContentRes)(nil)
	_ ContentRes = (*ECSliceContentRes)(nil)
	_ ContentRes = (*ECMetaContentRes)(nil)
	_ ContentRes = (*ObjChunkContentRes)(nil)
	_ ContentRes = (*ChunkMetaContentRes)(nil)
)

func (f *contentSpecMgr) Resolver(contentType string) ContentRes {
	r := f.m[contentType]
	return r
}

// Reg registers new content type with a given content resolver.
// NOTE: all content type registrations must happen at startup.
func (f *contentSpecMgr) Reg(contentType string, spec ContentRes, unitTest ...bool) {
	err := f._reg(contentType, spec)
	if err != nil && len(unitTest) == 0 {
		debug.AssertNoErr(err)
		cos.ExitLog(err)
	}
}

func (f *contentSpecMgr) _reg(contentType string, spec ContentRes) error {
	if strings.ContainsRune(contentType, filepath.Separator) {
		return fmt.Errorf("%s %q cannot contain %q", cttag, contentType, filepath.Separator)
	}
	if len(contentType) != contentTypeLen {
		return fmt.Errorf("%s %q must have length %d", cttag, contentType, contentTypeLen)
	}
	if _, ok := f.m[contentType]; ok {
		return fmt.Errorf("%s %q is already registered", cttag, contentType)
	}
	f.m[contentType] = spec
	return nil
}

// Gen returns a new FQN generated from given parts.
func (f *contentSpecMgr) Gen(parts PartsFQN, contentType string, extras ...string) (fqn string) {
	spec, ok := f.m[contentType]
	debug.Assert(ok, contentType)

	objName := spec.MakeFQN(parts.ObjectName(), extras...)

	// [NOTE]
	// override caller-provided  `objName` to prevent "file name too long" errno 0x24
	// - full pathname should be fine as (validated) bucket name <= 64
	// - see related: core/lom and "fixup fntl"
	if IsFntl(objName) {
		nlog.Warningln("fntl:", objName)
		objName = ShortenFntl(objName)
	}

	return parts.Mountpath().MakePathFQN(parts.Bucket(), contentType, objName)
}

//
// common content types (see also ext/dsort)
//

func (*ObjectContentRes) MakeFQN(base string, _ ...string) string { return base }

func (*ObjectContentRes) ParseFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}

func (*WorkContentRes) MakeFQN(base string, extras ...string) string {
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

func (*WorkContentRes) ParseFQN(base string) (orig string, old, ok bool) {
	// remove original content type
	cntIndex := strings.IndexByte(base, bsepa)
	if cntIndex < 0 {
		return "", false, false
	}
	base = base[cntIndex+1:]

	pidIndex := strings.LastIndexByte(base, bsepa) // pid
	if pidIndex < 0 {
		return "", false, false
	}
	tieIndex := strings.LastIndexByte(base[:pidIndex], bsepa) // tie breaker
	if tieIndex < 0 {
		return "", false, false
	}
	filePID, err := strconv.ParseInt(base[pidIndex+1:], 16, 64)
	if err != nil {
		return "", false, false
	}

	return base[:tieIndex], filePID != pid, true
}

// The following content resolvers handle temporary or derived files that don't require
// complex filename parsing. They rely on directory structure (%ch/, %ec/, %mt/) rather
// than filename encoding for organization:
//
// - ObjChunkContentRes: temporary chunk files during chunked uploads
//   Named by the chunk manifest (e.g., objectname.sessionID.0001)
//   Cleaned up by session ID after upload completion/failure
//
// - ECSliceContentRes: erasure coding data slices (%ec/ directory)
//   Generated during EC encoding, cleaned up with the original object
//
// - ECMetaContentRes: erasure coding metadata (%mt/ directory)
//   Contains EC reconstruction information, lifecycle tied to EC slices
//
// All use pass-through MakeFQN/ParseFQN since the content type
// in the path provides sufficient identification for cleanup and management.

func (*ObjChunkContentRes) MakeFQN(base string, extras ...string) string {
	debug.Assert(len(extras) == 2, "expecting uploadID and chunk number, got: ", extras)
	return base + ssepa + extras[0] + ssepa + extras[1]
}

func (*ObjChunkContentRes) ParseFQN(base string) (string, bool, bool) {
	// (chunk number)
	i := strings.LastIndexByte(base, bsepa)
	if i < 0 {
		return base, false, false
	}
	debug.Func(func() {
		_, err := strconv.Atoi(base[i+1:])
		debug.Assert(err == nil, err, " [", base, "]")
	})
	// (upload ID)
	j := strings.LastIndexByte(base[:i], bsepa)
	if j < 0 {
		return base, false, false
	}
	return base[:j], false, true
}

func (*ChunkMetaContentRes) MakeFQN(base string, extras ...string) string {
	if len(extras) == 0 {
		return base
	}
	debug.Assert(len(extras) == 1, extras)
	return base + ssepa + extras[0]
}

func (*ChunkMetaContentRes) ParseFQN(base string) (orig string, old, ok bool) {
	i := strings.LastIndexByte(base, bsepa)
	if i < 0 {
		return base, false, false
	}
	return base[:i], false, true
}

func (*ECSliceContentRes) MakeFQN(base string, _ ...string) string { return base }

func (*ECSliceContentRes) ParseFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}

func (*ECMetaContentRes) MakeFQN(base string, _ ...string) string { return base }

func (*ECMetaContentRes) ParseFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}
