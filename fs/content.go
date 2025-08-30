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
 * from content files. We parse the file type to run ParseUbase (implemented
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
	ContentInfo struct {
		Base   string   // original base
		Extras []string // original `extras` used to generate ubase; nil when not relevant (or not used)
		Old    bool     // is only used to indicate old WorkCT
		Ok     bool     // success or fail
	}

	ContentRes interface {
		// generate unique base name from the original one
		// * extras - user-defined marker(s)
		MakeUbase(base string, extras ...string) (ubase string)

		// Parse ubase back to ContentInfo
		ParseUbase(ubase string) ContentInfo
	}

	PartsFQN interface {
		ObjectName() string
		Bucket() *cmn.Bck
		Mountpath() *Mountpath
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

	// make new (and unique) base
	ubase := spec.MakeUbase(parts.ObjectName(), extras...)

	// [NOTE]
	// override caller-provided  `objName` to prevent "file name too long" errno 0x24
	// - full pathname should be fine as (validated) bucket name <= 64
	// - see related: core/lom and "fixup fntl"
	if IsFntl(ubase) {
		nlog.Warningln("fntl:", ubase)
		ubase = ShortenFntl(ubase)
	}

	return parts.Mountpath().MakePathFQN(parts.Bucket(), contentType, ubase)
}

//
// common content types (see also ext/dsort)
//

func (*ObjectContentRes) MakeUbase(base string, _ ...string) string { return base }

func (*ObjectContentRes) ParseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}

func (*WorkContentRes) MakeUbase(base string, extras ...string) string {
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

func (*WorkContentRes) ParseUbase(base string) (ci ContentInfo) {
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
// All use pass-through MakeUbase/ParseUbase since the content type
// in the path provides sufficient identification for cleanup and management.

func (*ObjChunkContentRes) MakeUbase(base string, extras ...string) string {
	debug.Assert(len(extras) == 2, "expecting uploadID and chunk number, got: ", extras)
	return base + ssepa + extras[0] + ssepa + extras[1]
}

func (*ObjChunkContentRes) ParseUbase(base string) (ci ContentInfo) {
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

func (*ChunkMetaContentRes) MakeUbase(base string, extras ...string) string {
	if len(extras) == 0 {
		return base // completed
	}
	debug.Assert(len(extras) == 1, extras)
	return base + ssepa + extras[0] // partial (with uploadID)
}

func (*ChunkMetaContentRes) ParseUbase(base string) ContentInfo {
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

func (*ECSliceContentRes) MakeUbase(base string, _ ...string) string { return base }

func (*ECSliceContentRes) ParseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}

func (*ECMetaContentRes) MakeUbase(base string, _ ...string) string { return base }

func (*ECMetaContentRes) ParseUbase(base string) ContentInfo {
	return ContentInfo{Base: base, Ok: true}
}
