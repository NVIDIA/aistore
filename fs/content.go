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
 * content type needs to implement ContentResolver to reflect the rules and
 * permission for different services. To see how the interface can be
 * implemented see: DefaultWorkfile implemention.
 *
 * When walking through the files we need to know if the file is an object or
 * other content. To do that we generate fqn with Gen. It adds short
 * prefix (or suffix) to the base name, which we believe is unique and will separate objects
 * from content files. We parse the file type to run ParseUniqueFQN (implemented
 * by this file type) on the rest of the base name.
 */

const (
	contentTypeLen = 2

	ObjectType   = "ob"
	WorkfileType = "wk"
	ECSliceType  = "ec"
	ECMetaType   = "mt"
	ObjChunkType = "oc"
)

const (
	ssepa = "."
	bsepa = '.'
)

type (
	ContentResolver interface {
		// Generates unique base name for original one. This function may add
		// additional information to the base name.
		// extra - a user-defined marker
		GenUniqueFQN(base, extra string) (ufqn string)
		// Parses generated unique fqn to the original one.
		ParseUniqueFQN(base string) (orig string, old, ok bool)
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
		m map[string]ContentResolver
	}
)

type (
	ObjectContentResolver   struct{}
	WorkfileContentResolver struct{}
	ECSliceContentResolver  struct{}
	ECMetaContentResolver   struct{}
	ObjChunkContentResolver struct{}
)

var CSM *contentSpecMgr

// interface guard
var (
	_ ContentResolver = (*ObjectContentResolver)(nil)
	_ ContentResolver = (*WorkfileContentResolver)(nil)
	_ ContentResolver = (*ECSliceContentResolver)(nil)
	_ ContentResolver = (*ECMetaContentResolver)(nil)
	_ ContentResolver = (*ObjChunkContentResolver)(nil)
)

func (f *contentSpecMgr) Resolver(contentType string) ContentResolver {
	r := f.m[contentType]
	return r
}

// Reg registers new content type with a given content resolver.
// NOTE: all content type registrations must happen at startup.
func (f *contentSpecMgr) Reg(contentType string, spec ContentResolver, unitTest ...bool) {
	err := f._reg(contentType, spec)
	if err != nil && len(unitTest) == 0 {
		debug.Assert(false)
		cos.ExitLog(err)
	}
}

func (f *contentSpecMgr) _reg(contentType string, spec ContentResolver) error {
	if strings.ContainsRune(contentType, filepath.Separator) {
		return fmt.Errorf("%s content type cannot contain %q", contentType, filepath.Separator)
	}
	if len(contentType) != contentTypeLen {
		return fmt.Errorf("%s content type must have length %d", contentType, contentTypeLen)
	}
	if _, ok := f.m[contentType]; ok {
		return fmt.Errorf("%s content type is already registered", contentType)
	}
	f.m[contentType] = spec
	return nil
}

// Gen returns a new FQN generated from given parts.
func (f *contentSpecMgr) Gen(parts PartsFQN, contentType, extra string) (fqn string) {
	var (
		spec    = f.m[contentType]
		objName = spec.GenUniqueFQN(parts.ObjectName(), extra)
	)

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

// FileSpec returns the specification/attributes and information about the `fqn`
// (which must be generated by the Gen)
func (f *contentSpecMgr) FileSpec(fqn string) (resolver ContentResolver, info *ContentInfo) {
	dir, base := filepath.Split(fqn)
	if dir == "" || base == "" {
		return
	}
	debug.Assert(cos.IsLastB(dir, filepath.Separator), dir)

	var parsed ParsedFQN
	if err := parsed.Init(fqn); err != nil {
		return
	}
	spec, found := f.m[parsed.ContentType]
	if !found {
		nlog.Errorf("%q: unknown content type %s", fqn, parsed.ContentType)
		return
	}
	origBase, old, ok := spec.ParseUniqueFQN(base)
	if !ok {
		return
	}
	resolver = spec
	info = &ContentInfo{Dir: dir, Base: origBase, Old: old, Type: parsed.ContentType}
	return
}

func (*ObjectContentResolver) GenUniqueFQN(base, _ string) string { return base }

func (*ObjectContentResolver) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}

func (*WorkfileContentResolver) GenUniqueFQN(base, extra string) string {
	var (
		dir, fname = filepath.Split(base)
		tieBreaker = cos.GenTie()
	)
	fname = extra + ssepa + fname
	base = filepath.Join(dir, fname)
	return base + ssepa + tieBreaker + ssepa + spid
}

func (*WorkfileContentResolver) ParseUniqueFQN(base string) (orig string, old, ok bool) {
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

func (*ObjChunkContentResolver) GenUniqueFQN(base, snum string) string {
	return base + ssepa + snum
}

func (*ObjChunkContentResolver) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	idx := strings.LastIndexByte(base, bsepa) // ".0001", "0002", etc.
	if idx < 0 {
		return "", false, false
	}
	return base[:idx], false, true
}

func (*ECSliceContentResolver) GenUniqueFQN(base, _ string) string { return base }

func (*ECSliceContentResolver) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}

func (*ECMetaContentResolver) GenUniqueFQN(base, _ string) string { return base }

func (*ECMetaContentResolver) ParseUniqueFQN(base string) (orig string, old, ok bool) {
	return base, false, true
}
